import { vi } from "vitest";

export const WORKSPACE = "https://workspace.test";

type Body = Record<string, unknown>;
interface Recorded {
  url: string;
  path: string;
  body: Body;
  auth?: string;
}

/**
 * An in-memory Candidate Workspace behind a fetch function. Prompt streams
 * are driven by the test through `stream`.
 */
export function fakeWorkspace() {
  const calls: Recorded[] = [];
  const encoder = new TextEncoder();
  let streamController: ReadableStreamDefaultController<Uint8Array> | undefined;
  let promptSignal: AbortSignal | undefined;
  const runs: Array<(response: Response) => void> = [];

  const json = (status: number, body: unknown) =>
    new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json" } });

  const fetch = vi.fn(async (input: RequestInfo | URL, init: RequestInit = {}) => {
    const url = String(input);
    const path = url.startsWith(WORKSPACE) ? url.slice(WORKSPACE.length) : url;
    const headers = (init.headers ?? {}) as Record<string, string>;
    calls.push({ url, path, body: JSON.parse(String(init.body ?? "{}")), auth: headers.Authorization });
    switch (path) {
      case "/session/start":
        return calls.at(-1)!.body.inviteToken === "good-invite"
          ? json(200, { sessionId: "s1", accessToken: "tok-1", tokenType: "Bearer", scope: "candidate:workspace", expiresAt: "2026-01-01T00:00:00Z" })
          : json(401, { code: "invalid_invite", message: "bad" });
      case "/session/prompt": {
        promptSignal = init.signal ?? undefined;
        const body = new ReadableStream<Uint8Array>({
          start(controller) {
            streamController = controller;
            promptSignal?.addEventListener("abort", () => controller.error(new DOMException("aborted", "AbortError")));
          },
        });
        return new Response(body, { status: 200, headers: { "Content-Type": "text/event-stream" } });
      }
      case "/session/run":
        return new Promise<Response>((resolve) => runs.push(resolve));
      case "/session/diff":
        return json(202, { accepted: true, sequenceNumber: calls.length, linesAdded: 1, linesRemoved: 0 });
      default:
        return json(404, { code: "not_found", message: path });
    }
  });

  return {
    fetch: fetch as unknown as typeof globalThis.fetch,
    calls,
    diffs: () => calls.filter((c) => c.path === "/session/diff").map((c) => c.body),
    stream: {
      send(event: string, data: unknown) {
        streamController!.enqueue(encoder.encode(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`));
      },
      close() {
        streamController!.close();
      },
      get aborted() {
        return promptSignal?.aborted ?? false;
      },
    },
    finishRun(status: number, body: unknown) {
      runs.shift()!(json(status, body));
    },
    pendingRuns: () => runs.length,
  };
}
