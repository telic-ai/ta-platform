import type { components } from "./generated/schema";
import { parseSSE } from "./sse";

type Schemas = components["schemas"];
export type StartSessionResponse = Schemas["StartSessionResponse"];
export type ChatMessage = Schemas["ChatMessage"];
export type PromptRequest = Schemas["PromptRequest"];
export type PromptAccepted = Schemas["PromptAccepted"];
export type PromptDone = Schemas["PromptDone"];
export type RunRequest = Schemas["RunRequest"];
export type RunResult = Schemas["RunResult"];
export type DiffRequest = Schemas["DiffRequest"];
export type DiffResult = Schemas["DiffResult"];

/** A non-2xx response from the Candidate Workspace. */
export class WorkspaceError extends Error {
  constructor(
    readonly status: number,
    readonly code: string,
    message: string,
  ) {
    super(message);
    this.name = "WorkspaceError";
  }
}

export interface WorkspaceClientOptions {
  /** Base URL of the Candidate Workspace; the only origin the app calls. */
  baseUrl: string;
  token?: string;
  fetch?: typeof fetch;
}

export interface PromptHandlers {
  signal?: AbortSignal;
  onAccepted?: (accepted: PromptAccepted) => void;
  onDelta?: (text: string) => void;
}

/** Typed client for the Candidate Workspace session API. */
export class WorkspaceClient {
  private readonly baseUrl: string;
  private readonly fetchImpl: typeof fetch;
  private token: string | undefined;

  constructor(options: WorkspaceClientOptions) {
    this.baseUrl = options.baseUrl.replace(/\/+$/, "");
    this.fetchImpl = options.fetch ?? globalThis.fetch.bind(globalThis);
    this.token = options.token;
  }

  get sessionToken(): string | undefined {
    return this.token;
  }

  /** Exchanges a one-time invite and keeps the session token. */
  async startSession(inviteToken: string): Promise<StartSessionResponse> {
    const session = await this.json<StartSessionResponse>("/session/start", { inviteToken }, false);
    this.token = session.accessToken;
    return session;
  }

  /**
   * Streams the assistant's answer. Aborting the signal closes the
   * connection, which cancels the upstream model call.
   */
  async submitPrompt(request: PromptRequest, handlers: PromptHandlers = {}): Promise<PromptDone> {
    const response = await this.post("/session/prompt", request, true, handlers.signal, "text/event-stream");
    if (!response.body) throw new WorkspaceError(response.status, "no_body", "empty prompt stream");
    for await (const event of parseSSE(response.body)) {
      switch (event.event) {
        case "prompt":
          handlers.onAccepted?.(JSON.parse(event.data) as PromptAccepted);
          break;
        case "delta":
          handlers.onDelta?.((JSON.parse(event.data) as { text: string }).text);
          break;
        case "done":
          return JSON.parse(event.data) as PromptDone;
      }
    }
    throw new WorkspaceError(502, "stream_ended", "prompt stream ended before completion");
  }

  /** Runs code and waits for the result. A run already in flight is a 429 WorkspaceError. */
  runCode(request: RunRequest): Promise<RunResult> {
    return this.json<RunResult>("/session/run", request, true);
  }

  /** Posts one diff. An out-of-order diff resolves with accepted: false. */
  submitDiff(request: DiffRequest): Promise<DiffResult> {
    return this.json<DiffResult>("/session/diff", request, true);
  }

  private async json<T>(path: string, body: unknown, auth: boolean): Promise<T> {
    const response = await this.post(path, body, auth);
    return (await response.json()) as T;
  }

  private async post(path: string, body: unknown, auth: boolean, signal?: AbortSignal, accept = "application/json"): Promise<Response> {
    const headers: Record<string, string> = { "Content-Type": "application/json", Accept: accept };
    if (auth) {
      if (!this.token) throw new WorkspaceError(401, "no_session", "start a session first");
      headers.Authorization = `Bearer ${this.token}`;
    }
    const response = await this.fetchImpl(this.baseUrl + path, {
      method: "POST",
      headers,
      body: JSON.stringify(body),
      signal,
    });
    if (!response.ok) {
      let code = "http_error";
      let message = response.statusText;
      try {
        const error = (await response.json()) as { code?: string; message?: string };
        code = error.code ?? code;
        message = error.message ?? message;
      } catch {
        // Not a JSON error body.
      }
      throw new WorkspaceError(response.status, code, message);
    }
    return response;
  }
}
