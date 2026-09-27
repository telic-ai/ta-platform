import { describe, expect, it } from "vitest";
import { WorkspaceClient, WorkspaceError } from "../src/workspace";
import { fakeFetch, json, streamOf } from "./helpers";

const BASE = "https://workspace.example.test/";

describe("WorkspaceClient", () => {
  it("starts a session and authenticates later calls with its token", async () => {
    const { fetch, calls } = fakeFetch((url) =>
      url.endsWith("/session/start")
        ? json(200, { sessionId: "s", accessToken: "tok", tokenType: "Bearer", scope: "candidate:workspace", expiresAt: "x" })
        : json(200, { accepted: true, linesAdded: 1, linesRemoved: 0 }),
    );
    const client = new WorkspaceClient({ baseUrl: BASE, fetch });
    await client.startSession("invite");
    await client.submitDiff({ clientSeq: 1, origin: "manual", path: "main.py", patch: "p" });
    expect(calls.map((c) => c.url)).toEqual([`${BASE}session/start`, `${BASE}session/diff`]);
    expect((calls[0].init.headers as Record<string, string>).Authorization).toBeUndefined();
    expect(JSON.parse(calls[0].init.body as string)).toEqual({ inviteToken: "invite" });
    expect((calls[1].init.headers as Record<string, string>).Authorization).toBe("Bearer tok");
    expect(client.sessionToken).toBe("tok");
  });

  it("refuses authenticated calls before a session exists", async () => {
    const client = new WorkspaceClient({ baseUrl: BASE, fetch: fakeFetch(() => json(200, {})).fetch });
    await expect(client.runCode({ language: "python", entrypoint: "a", files: {} })).rejects.toMatchObject({ status: 401 });
  });

  it("streams a prompt: accepted, deltas, then done", async () => {
    const { fetch, calls } = fakeFetch(
      () =>
        new Response(
          streamOf([
            'event: prompt\ndata: {"promptId":"p1","sequenceNumber":4}\n\n',
            'event: delta\ndata: {"text":"Hel"}\n\nevent: delta\ndata: {"text":"lo"}\n\n',
            'event: done\ndata: {"status":"completed","stopReason":"end_turn"}\n\n',
          ]),
          { status: 200, headers: { "Content-Type": "text/event-stream" } },
        ),
    );
    const client = new WorkspaceClient({ baseUrl: BASE, token: "tok", fetch });
    const deltas: string[] = [];
    let accepted;
    const done = await client.submitPrompt(
      { prompt: "hi", history: [{ role: "user", content: "a" }, { role: "assistant", content: "b" }] },
      { onAccepted: (a) => (accepted = a), onDelta: (d) => deltas.push(d) },
    );
    expect(accepted).toEqual({ promptId: "p1", sequenceNumber: 4 });
    expect(deltas.join("")).toBe("Hello");
    expect(done).toEqual({ status: "completed", stopReason: "end_turn" });
    expect((calls[0].init.headers as Record<string, string>).Accept).toBe("text/event-stream");
    expect(JSON.parse(calls[0].init.body as string).history).toHaveLength(2);
  });

  it("passes the abort signal so closing the stream cancels upstream", async () => {
    let signal: AbortSignal | undefined;
    const { fetch } = fakeFetch((_, init) => {
      signal = init.signal ?? undefined;
      return new Promise<Response>((_, reject) => signal?.addEventListener("abort", () => reject(signal?.reason)));
    });
    const client = new WorkspaceClient({ baseUrl: BASE, token: "tok", fetch });
    const controller = new AbortController();
    const pending = client.submitPrompt({ prompt: "hi" }, { signal: controller.signal });
    controller.abort();
    await expect(pending).rejects.toBeDefined();
    expect(signal?.aborted).toBe(true);
  });

  it("fails if the stream ends without done", async () => {
    const client = new WorkspaceClient({
      baseUrl: BASE,
      token: "tok",
      fetch: fakeFetch(() => new Response(streamOf(['event: delta\ndata: {"text":"x"}\n\n']))).fetch,
    });
    await expect(client.submitPrompt({ prompt: "hi" })).rejects.toMatchObject({ code: "stream_ended" });
  });

  it("maps error bodies to WorkspaceError, including 429 run_in_progress", async () => {
    const client = new WorkspaceClient({
      baseUrl: BASE,
      token: "tok",
      fetch: fakeFetch(() => json(429, { code: "run_in_progress", message: "a run is already in progress" })).fetch,
    });
    const error = await client.runCode({ language: "python", entrypoint: "a", files: { a: "x" } }).catch((e) => e);
    expect(error).toBeInstanceOf(WorkspaceError);
    expect(error).toMatchObject({ status: 429, code: "run_in_progress" });

    const plain = new WorkspaceClient({ baseUrl: BASE, token: "tok", fetch: fakeFetch(() => new Response("oops", { status: 502 })).fetch });
    await expect(plain.submitDiff({ clientSeq: 1, origin: "manual", path: "a", patch: "p" })).rejects.toMatchObject({
      status: 502,
      code: "http_error",
    });
  });

  it("returns run results", async () => {
    const result = { executionId: "e", status: "timed_out", exitCode: -1, stdout: "", stderr: "", stdoutTruncated: false, stderrTruncated: false, durationMs: 10000 };
    const client = new WorkspaceClient({ baseUrl: BASE, token: "tok", fetch: fakeFetch(() => json(200, result)).fetch });
    expect(await client.runCode({ language: "python", entrypoint: "a", files: { a: "x" } })).toEqual(result);
  });
});
