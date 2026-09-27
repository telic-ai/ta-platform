import { describe, expect, it, vi } from "vitest";
import { CompanyApiError, CompanyClient, type TimelineEvent } from "../src/company";
import { fakeFetch, json, streamOf } from "./helpers";

const BASE = "https://admin.example.test/api/";
const LIVE = "https://live.example.test/live-api";

const headersOf = (init: RequestInit) => init.headers as Record<string, string>;

function event(seq: number): TimelineEvent {
  return {
    event_id: `e${seq}`,
    company_id: "c",
    interview_id: "i",
    sequence_number: seq,
    event_type: "code.diff",
    occurred_at: "2026-09-27T00:00:00Z",
    payload: {},
  };
}

const sse = (...chunks: string[]) =>
  new Response(streamOf(chunks), { status: 200, headers: { "Content-Type": "text/event-stream" } });
const frame = (seq: number) => `id: ${seq}\nevent: event\ndata: ${JSON.stringify(event(seq))}\n\n`;
const caughtUp = (seq: number) => `event: caught_up\ndata: {"last_event_id":${seq}}\n\n`;

describe("CompanyClient requests", () => {
  it("sends the bearer token and JSON bodies to the Admin API", async () => {
    const { fetch, calls } = fakeFetch(() => json(201, { id: "i1" }));
    const client = new CompanyClient({ baseUrl: BASE, token: "tok", fetch });
    await client.createInterview({ candidate_name: "Ada", candidate_email: "ada@x.test" });
    expect(calls[0].url).toBe("https://admin.example.test/api/interviews");
    expect(calls[0].init.method).toBe("POST");
    expect(headersOf(calls[0].init).Authorization).toBe("Bearer tok");
    expect(headersOf(calls[0].init)["Content-Type"]).toBe("application/json");
    expect(JSON.parse(calls[0].init.body as string)).toEqual({ candidate_name: "Ada", candidate_email: "ada@x.test" });
  });

  it("maps every call to its route", async () => {
    const { fetch, calls } = fakeFetch((url) => {
      if (url.includes("/dashboard/interviews")) return json(200, { interviews: [] });
      if (url.includes("/timeline")) return json(200, { events: [event(2)] });
      if (url.endsWith("/scores")) return json(200, { scores: [] });
      if (url.includes("/tasks") && !url.match(/tasks\/t/)) return json(200, { tasks: [] });
      if (url.endsWith("/users")) return json(200, { users: [] });
      if (url.endsWith("/policies")) return json(200, { policies: [] });
      if (url.includes("/interviews?") || url.endsWith("/interviews")) return json(200, { interviews: [] });
      return json(200, {});
    });
    const c = new CompanyClient({ baseUrl: BASE, token: "t", fetch });
    await c.getCompany();
    await c.listUsers();
    await c.createUser({ email: "a@b.test", role: "viewer" });
    await c.updateUser("u/1", { role: "admin" });
    await c.listInterviews({ status: "completed", limit: 5 });
    await c.listInterviews();
    await c.getInterview("i1");
    await c.updateInterview("i1", { status: "completed" });
    await c.requestErasure("i1");
    await c.listScores("i1");
    await c.decideScore("i1", "s1", { status: "human_approved" });
    await c.listTasks({ interviewId: "i1" });
    await c.createTask({ title: "t" });
    await c.updateTask("t1", { assignee_id: null });
    await c.createInvite({ email: "a@b.test", interview_id: "i1" });
    await c.listPolicies();
    await c.setPolicy("replay", false);
    await c.dashboardOverview(7);
    await c.dashboardInterviews();
    expect(await c.timeline("i1", 1)).toEqual([event(2)]);
    await c.timeline("i1");
    expect(calls.map((call) => `${call.init.method} ${call.url.replace("https://admin.example.test/api", "")}`)).toEqual([
      "GET /company",
      "GET /users",
      "POST /users",
      "PATCH /users/u%2F1",
      "GET /interviews?status=completed&limit=5",
      "GET /interviews",
      "GET /interviews/i1",
      "PATCH /interviews/i1",
      "POST /interviews/i1/erase",
      "GET /interviews/i1/scores",
      "POST /interviews/i1/scores/s1/decision",
      "GET /tasks?interview_id=i1",
      "POST /tasks",
      "PATCH /tasks/t1",
      "POST /invites",
      "GET /policies",
      "PUT /policies/replay",
      "GET /dashboard/overview?days=7",
      "GET /dashboard/interviews",
      "GET /interviews/i1/timeline?after_seq=1",
      "GET /interviews/i1/timeline",
    ]);
    expect(JSON.parse(calls[13].init.body as string)).toEqual({ assignee_id: null });
    expect(JSON.parse(calls[16].init.body as string)).toEqual({ enabled: false });
  });

  it("requests a company-scoped search key", async () => {
    const issued = { key: "k", filter_by: "company_id:=c1", expires_at: "2026-09-27T13:00:00Z", host: "http://ts", collections: ["interviews"] };
    const { fetch, calls } = fakeFetch(() => json(201, issued));
    const c = new CompanyClient({ baseUrl: BASE, token: "t", fetch });
    expect(await c.issueSearchKey()).toEqual(issued);
    expect(calls[0].init.method).toBe("POST");
    expect(calls[0].url).toBe("https://admin.example.test/api/search/key");
    expect(calls[0].init.body).toBeUndefined();
  });

  it("resolves 204s to undefined", async () => {
    const { fetch } = fakeFetch(() => new Response(null, { status: 204 }));
    const c = new CompanyClient({ baseUrl: BASE, token: "t", fetch });
    await expect(c.deleteTask("t1")).resolves.toBeUndefined();
    await expect(c.deleteUser("u1")).resolves.toBeUndefined();
  });

  it("turns error bodies into CompanyApiError", async () => {
    const { fetch } = fakeFetch(() => json(403, { code: "forbidden", message: 'role "viewer" may not interviews:erase' }));
    const c = new CompanyClient({ baseUrl: BASE, token: "t", fetch });
    const error = await c.requestErasure("i1").catch((e: unknown) => e);
    expect(error).toBeInstanceOf(CompanyApiError);
    expect(error).toMatchObject({ status: 403, code: "forbidden", message: 'role "viewer" may not interviews:erase' });

    const plain = fakeFetch(() => new Response("oops", { status: 502, statusText: "Bad Gateway" }));
    await expect(new CompanyClient({ baseUrl: BASE, token: "t", fetch: plain.fetch }).getCompany()).rejects.toMatchObject({
      status: 502,
      code: "http_error",
      message: "Bad Gateway",
    });
  });
});

describe("CompanyClient.watchLive", () => {
  it("streams events from the live service with the token", async () => {
    const controller = new AbortController();
    const seen: number[] = [];
    const statuses: string[] = [];
    const { fetch, calls } = fakeFetch(() => sse(frame(1), frame(2), caughtUp(2), frame(3)));
    const client = new CompanyClient({ baseUrl: BASE, liveUrl: LIVE, token: "tok", fetch });
    const done = client.watchLive("i1", {
      signal: controller.signal,
      onEvent: (e) => {
        seen.push(e.sequence_number);
        if (e.sequence_number === 3) controller.abort();
      },
      onStatus: (s) => statuses.push(s),
      retryDelayMs: () => 0,
    });
    await done;
    expect(seen).toEqual([1, 2, 3]);
    expect(calls[0].url).toBe("https://live.example.test/live-api/interviews/i1/live");
    expect(headersOf(calls[0].init).Authorization).toBe("Bearer tok");
    expect(headersOf(calls[0].init)["Last-Event-ID"]).toBeUndefined();
    expect(statuses.slice(0, 2)).toEqual(["connecting", "live"]);
  });

  it("reconnects with Last-Event-ID and never repeats an event", async () => {
    const controller = new AbortController();
    const seen: number[] = [];
    let attempt = 0;
    const { fetch, calls } = fakeFetch(() => {
      attempt++;
      if (attempt === 1) return sse(frame(1), frame(2), caughtUp(2)); // then the stream drops
      if (attempt === 2) return new Response("down", { status: 503 });
      // The server replays 2 (a duplicate) and 3.
      return sse(frame(2), frame(3), caughtUp(3));
    });
    const caught: number[] = [];
    const client = new CompanyClient({ baseUrl: BASE, token: "t", fetch });
    await client.watchLive("i1", {
      signal: controller.signal,
      onEvent: (e) => seen.push(e.sequence_number),
      onCaughtUp: (last) => {
        caught.push(last);
        if (last === 3) controller.abort();
      },
      retryDelayMs: () => 0,
    });
    expect(seen).toEqual([1, 2, 3]);
    expect(caught).toEqual([2, 3]);
    expect(calls.map((c) => headersOf(c.init)["Last-Event-ID"])).toEqual([undefined, "2", "2"]);
  });

  it("resumes from a given lastEventId", async () => {
    const controller = new AbortController();
    const { fetch, calls } = fakeFetch(() => sse(frame(5), caughtUp(5)));
    await new CompanyClient({ baseUrl: BASE, token: "t", fetch }).watchLive("i1", {
      signal: controller.signal,
      lastEventId: 4,
      onEvent: () => controller.abort(),
    });
    expect(headersOf(calls[0].init)["Last-Event-ID"]).toBe("4");
  });

  it("rejects on errors a reconnect cannot fix", async () => {
    for (const status of [401, 403, 404]) {
      const { fetch, calls } = fakeFetch(() => json(status, { code: "nope", message: "nope" }));
      const watch = new CompanyClient({ baseUrl: BASE, token: "t", fetch }).watchLive("i1", {
        signal: new AbortController().signal,
        onEvent: () => {},
        retryDelayMs: () => 0,
      });
      await expect(watch).rejects.toMatchObject({ status });
      expect(calls).toHaveLength(1);
    }
  });

  it("stops when aborted while waiting to reconnect", async () => {
    vi.useFakeTimers();
    try {
      const controller = new AbortController();
      const { fetch, calls } = fakeFetch(() => new Response("down", { status: 503 }));
      const watch = new CompanyClient({ baseUrl: BASE, token: "t", fetch }).watchLive("i1", {
        signal: controller.signal,
        onEvent: () => {},
        retryDelayMs: () => 60_000,
      });
      await vi.advanceTimersByTimeAsync(10);
      controller.abort();
      await expect(watch).resolves.toBeUndefined();
      expect(calls).toHaveLength(1);
    } finally {
      vi.useRealTimers();
    }
  });
});
