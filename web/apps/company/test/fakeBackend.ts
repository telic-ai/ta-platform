import type { Interview, Policy, TimelineEvent } from "@ta-platform/api-client";

export const ADMIN = "http://admin.test/api";
export const LIVE = "http://live.test/live-api";
export const CANDIDATE = "http://candidate.test";
export const TOKEN = "member-token";

export interface Call {
  method: string;
  path: string;
  body?: unknown;
  auth?: string;
  lastEventId?: string;
}

/** A controllable SSE response body. */
export class LiveStream {
  private controller?: ReadableStreamDefaultController<Uint8Array>;
  private closed = false;
  readonly body = new ReadableStream<Uint8Array>({ start: (c) => void (this.controller = c) });
  send(event: string, data: unknown, id?: number) {
    if (this.closed) return;
    const frame = `${id === undefined ? "" : `id: ${id}\n`}event: ${event}\ndata: ${JSON.stringify(data)}\n\n`;
    this.controller?.enqueue(new TextEncoder().encode(frame));
  }
  close() {
    if (this.closed) return;
    this.closed = true;
    this.controller?.close();
  }
}

export function timelineEvent(seq: number, event_type: string, payload: Record<string, unknown>): TimelineEvent {
  return {
    event_id: `e${seq}`,
    company_id: "c1",
    interview_id: "iv-1",
    sequence_number: seq,
    event_type,
    occurred_at: new Date(Date.UTC(2026, 8, 27, 10, 0, seq)).toISOString(),
    payload,
  };
}

export function interview(id: string, name: string, extra: Partial<Interview> = {}): Interview {
  return {
    id,
    candidate_name: name,
    candidate_email: `${name.toLowerCase()}@x.test`,
    status: "scheduled",
    created_by: null,
    scheduled_at: null,
    terminal_at: null,
    erase_requested_at: null,
    legal_hold: false,
    purged_at: null,
    created_at: "2026-09-27T09:00:00Z",
    updated_at: "2026-09-27T09:00:00Z",
    ...extra,
  };
}

const json = (status: number, body: unknown) =>
  new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json" } });

/** An in-memory Admin API and live monitor. */
export function fakeBackend() {
  const calls: Call[] = [];
  const state = {
    interviews: [interview("iv-1", "Ada", { status: "in_progress" })],
    timeline: [] as TimelineEvent[],
    policies: ["ai_assistance", "ai_scoring", "code_execution", "live_monitoring", "replay"].map(
      (key): Policy => ({ key: key as Policy["key"], enabled: true, updated_by: null, updated_at: null }),
    ),
    streams: [] as LiveStream[],
    /** Overrides: "METHOD /path" -> response. */
    fail: new Map<string, () => Response>(),
  };

  const fetch = (async (input: RequestInfo | URL, init: RequestInit = {}) => {
    const url = new URL(String(input));
    const headers = (init.headers ?? {}) as Record<string, string>;
    const method = init.method ?? "GET";
    const live = url.origin === new URL(LIVE).origin;
    const path = url.pathname.replace(live ? "/live-api" : "/api", "") + url.search;
    const call: Call = {
      method,
      path,
      body: init.body ? JSON.parse(init.body as string) : undefined,
      auth: headers.Authorization,
      lastEventId: headers["Last-Event-ID"],
    };
    calls.push(call);
    const override = state.fail.get(`${method} ${url.pathname.replace(live ? "/live-api" : "/api", "")}`);
    if (override) return override();
    if (headers.Authorization !== `Bearer ${TOKEN}`) return json(401, { code: "unauthorized", message: "no" });

    if (live && /\/interviews\/[^/]+\/live$/.test(url.pathname)) {
      const stream = new LiveStream();
      state.streams.push(stream);
      init.signal?.addEventListener("abort", () => stream.close());
      return new Response(stream.body, { status: 200, headers: { "Content-Type": "text/event-stream" } });
    }
    const route = `${method} ${url.pathname.replace("/api", "")}`;
    let m: RegExpMatchArray | null;
    if (route === "GET /company") return json(200, { id: "c1", name: "Acme", slug: "acme", created_at: "", updated_at: "" });
    if (route === "GET /interviews") return json(200, { interviews: state.interviews });
    if (route === "POST /interviews") {
      const body = call.body as { candidate_name: string; candidate_email: string };
      const created = interview(`iv-${state.interviews.length + 1}`, body.candidate_name, { candidate_email: body.candidate_email });
      state.interviews = [created, ...state.interviews];
      return json(201, created);
    }
    if ((m = route.match(/^GET \/interviews\/([^/]+)$/))) {
      const found = state.interviews.find((i) => i.id === m![1]);
      return found ? json(200, found) : json(404, { code: "not_found", message: "not found" });
    }
    if ((m = route.match(/^PATCH \/interviews\/([^/]+)$/))) {
      state.interviews = state.interviews.map((i) => (i.id === m![1] ? { ...i, ...(call.body as object) } : i));
      return json(200, state.interviews.find((i) => i.id === m![1]));
    }
    if ((m = route.match(/^POST \/interviews\/([^/]+)\/erase$/))) {
      const at = "2026-09-27T12:00:00Z";
      state.interviews = state.interviews.map((i) => (i.id === m![1] ? { ...i, erase_requested_at: at } : i));
      return json(202, { id: m[1], erase_requested_at: at, legal_hold: false });
    }
    if ((m = route.match(/^GET \/interviews\/([^/]+)\/timeline$/))) return json(200, { events: state.timeline });
    if (route === "POST /invites") {
      const body = call.body as { email: string; interview_id: string };
      return json(201, { id: "inv-1", email: body.email, role: "candidate", interview_id: body.interview_id, expires_at: "2026-10-04T12:00:00Z", token: "one-time/token" });
    }
    if (route === "GET /policies") return json(200, { policies: state.policies });
    if ((m = route.match(/^PUT \/policies\/([^/]+)$/))) {
      const enabled = (call.body as { enabled: boolean }).enabled;
      state.policies = state.policies.map((p) => (p.key === m![1] ? { ...p, enabled } : p));
      return json(200, state.policies.find((p) => p.key === m![1]));
    }
    if (route === "GET /dashboard/overview") {
      return json(200, {
        since: "2026-09-21",
        days: Number(url.searchParams.get("days") ?? 14),
        daily: [
          { day: "2026-09-26", event_type: "code.diff", events: 5 },
          { day: "2026-09-27", event_type: "code.diff", events: 3 },
          { day: "2026-09-27", event_type: "session.started", events: 2 },
        ],
        totals: { "code.diff": 8, "session.started": 2, "prompt.submitted": 4 },
      });
    }
    if (route === "GET /dashboard/interviews") {
      return json(200, {
        interviews: [
          { interview_id: "iv-1", events: 12, prompts: 4, ai_responses: 4, runs: 3, runs_succeeded: 2, diffs: 8, ai_applied_diffs: 1,
            lines_added: 30, lines_removed: 4, first_at: "2026-09-27T10:00:00Z", last_at: "2026-09-27T10:30:00Z" },
        ],
      });
    }
    return json(404, { code: "not_found", message: `no route ${route}` });
  }) as typeof globalThis.fetch;

  return { fetch, calls, state };
}
