import type { components } from "./generated/schema";
import { parseSSE } from "./sse";

type Schemas = components["schemas"];
export type Role = Schemas["Role"];
export type Company = Schemas["Company"];
export type User = Schemas["User"];
export type CreateUserRequest = Schemas["CreateUserRequest"];
export type UpdateUserRequest = Schemas["UpdateUserRequest"];
export type Interview = Schemas["Interview"];
export type InterviewStatus = Schemas["InterviewStatus"];
export type CreateInterviewRequest = Schemas["CreateInterviewRequest"];
export type UpdateInterviewRequest = Schemas["UpdateInterviewRequest"];
export type EraseAccepted = Schemas["EraseAccepted"];
export type Score = Schemas["Score"];
export type ScoreDecisionRequest = Schemas["ScoreDecisionRequest"];
export type Task = Schemas["Task"];
export type CreateTaskRequest = Schemas["CreateTaskRequest"];
export type UpdateTaskRequest = Schemas["UpdateTaskRequest"];
export type CreateInviteRequest = Schemas["CreateInviteRequest"];
export type Invite = Schemas["Invite"];
export type Policy = Schemas["Policy"];
export type PolicyKey = Schemas["PolicyKey"];
export type DashboardOverview = Schemas["DashboardOverview"];
export type InterviewActivity = Schemas["InterviewActivity"];
export type TimelineEvent = Schemas["TimelineEvent"];

/** A non-2xx response from the Admin API or live monitor. */
export class CompanyApiError extends Error {
  constructor(
    readonly status: number,
    readonly code: string,
    message: string,
  ) {
    super(message);
    this.name = "CompanyApiError";
  }
}

export interface CompanyClientOptions {
  /** Base URL of the Admin API. */
  baseUrl: string;
  /** Base URL of the live-monitor service; defaults to baseUrl. */
  liveUrl?: string;
  /** The member's session token. */
  token: string;
  fetch?: typeof fetch;
}

export type LiveStatus = "connecting" | "live" | "reconnecting";

export interface LiveHandlers {
  /** Called once per event, in sequence order, never twice for one event. */
  onEvent: (event: TimelineEvent) => void;
  /** Called after each (re)connection has replayed what was missed. */
  onCaughtUp?: (lastEventId: number) => void;
  onStatus?: (status: LiveStatus, error?: unknown) => void;
  signal: AbortSignal;
  /** Resume after this sequence number; 0 streams from the start. */
  lastEventId?: number;
  /** Backoff before reconnect attempt n (1-based). */
  retryDelayMs?: (attempt: number) => number;
}

const defaultRetryDelay = (attempt: number) => Math.min(250 * 2 ** (attempt - 1), 10_000);

/** Errors a reconnect cannot fix. */
const fatalStatuses = new Set([400, 401, 403, 404]);

/** Typed client for the company-facing Admin API and live stream. */
export class CompanyClient {
  private readonly baseUrl: string;
  private readonly liveUrl: string;
  private readonly fetchImpl: typeof fetch;
  private readonly token: string;

  constructor(options: CompanyClientOptions) {
    this.baseUrl = options.baseUrl.replace(/\/+$/, "");
    this.liveUrl = (options.liveUrl ?? options.baseUrl).replace(/\/+$/, "");
    this.fetchImpl = options.fetch ?? globalThis.fetch.bind(globalThis);
    this.token = options.token;
  }

  getCompany(): Promise<Company> {
    return this.request("GET", "/company");
  }

  async listUsers(): Promise<User[]> {
    return (await this.request<{ users: User[] }>("GET", "/users")).users;
  }

  createUser(request: CreateUserRequest): Promise<User> {
    return this.request("POST", "/users", request);
  }

  updateUser(id: string, request: UpdateUserRequest): Promise<User> {
    return this.request("PATCH", `/users/${encodeURIComponent(id)}`, request);
  }

  deleteUser(id: string): Promise<void> {
    return this.request("DELETE", `/users/${encodeURIComponent(id)}`);
  }

  async listInterviews(filter: { status?: InterviewStatus; limit?: number } = {}): Promise<Interview[]> {
    const query = new URLSearchParams();
    if (filter.status) query.set("status", filter.status);
    if (filter.limit) query.set("limit", String(filter.limit));
    const suffix = query.size ? `?${query}` : "";
    return (await this.request<{ interviews: Interview[] }>("GET", `/interviews${suffix}`)).interviews;
  }

  getInterview(id: string): Promise<Interview> {
    return this.request("GET", `/interviews/${encodeURIComponent(id)}`);
  }

  createInterview(request: CreateInterviewRequest): Promise<Interview> {
    return this.request("POST", "/interviews", request);
  }

  updateInterview(id: string, request: UpdateInterviewRequest): Promise<Interview> {
    return this.request("PATCH", `/interviews/${encodeURIComponent(id)}`, request);
  }

  /** Requests erasure; the server only marks the interview (202). */
  requestErasure(id: string): Promise<EraseAccepted> {
    return this.request("POST", `/interviews/${encodeURIComponent(id)}/erase`);
  }

  async listScores(interviewId: string): Promise<Score[]> {
    return (await this.request<{ scores: Score[] }>("GET", `/interviews/${encodeURIComponent(interviewId)}/scores`)).scores;
  }

  decideScore(interviewId: string, scoreId: string, request: ScoreDecisionRequest): Promise<Score> {
    return this.request(
      "POST",
      `/interviews/${encodeURIComponent(interviewId)}/scores/${encodeURIComponent(scoreId)}/decision`,
      request,
    );
  }

  async listTasks(filter: { interviewId?: string; assigneeId?: string } = {}): Promise<Task[]> {
    const query = new URLSearchParams();
    if (filter.interviewId) query.set("interview_id", filter.interviewId);
    if (filter.assigneeId) query.set("assignee_id", filter.assigneeId);
    const suffix = query.size ? `?${query}` : "";
    return (await this.request<{ tasks: Task[] }>("GET", `/tasks${suffix}`)).tasks;
  }

  createTask(request: CreateTaskRequest): Promise<Task> {
    return this.request("POST", "/tasks", request);
  }

  updateTask(id: string, request: UpdateTaskRequest): Promise<Task> {
    return this.request("PATCH", `/tasks/${encodeURIComponent(id)}`, request);
  }

  deleteTask(id: string): Promise<void> {
    return this.request("DELETE", `/tasks/${encodeURIComponent(id)}`);
  }

  /** Issues an invite. The token is only ever returned here. */
  createInvite(request: CreateInviteRequest): Promise<Invite> {
    return this.request("POST", "/invites", request);
  }

  async listPolicies(): Promise<Policy[]> {
    return (await this.request<{ policies: Policy[] }>("GET", "/policies")).policies;
  }

  setPolicy(key: PolicyKey, enabled: boolean): Promise<Policy> {
    return this.request("PUT", `/policies/${encodeURIComponent(key)}`, { enabled });
  }

  dashboardOverview(days?: number): Promise<DashboardOverview> {
    return this.request("GET", `/dashboard/overview${days ? `?days=${days}` : ""}`);
  }

  async dashboardInterviews(): Promise<InterviewActivity[]> {
    return (await this.request<{ interviews: InterviewActivity[] }>("GET", "/dashboard/interviews")).interviews;
  }

  /** The interview's events after afterSeq, in order, for replay. */
  async timeline(interviewId: string, afterSeq = 0): Promise<TimelineEvent[]> {
    const suffix = afterSeq > 0 ? `?after_seq=${afterSeq}` : "";
    return (await this.request<{ events: TimelineEvent[] }>("GET", `/interviews/${encodeURIComponent(interviewId)}/timeline${suffix}`))
      .events;
  }

  /**
   * Streams an interview live until the signal aborts. It reconnects with
   * backoff, resuming from the last event seen via Last-Event-ID, so
   * handlers see every event exactly once and in order. Resolves when
   * aborted; rejects on errors a reconnect cannot fix (401/403/404).
   */
  async watchLive(interviewId: string, handlers: LiveHandlers): Promise<void> {
    const retryDelay = handlers.retryDelayMs ?? defaultRetryDelay;
    let lastEventId = handlers.lastEventId ?? 0;
    let attempt = 0;
    const url = `${this.liveUrl}/interviews/${encodeURIComponent(interviewId)}/live`;
    while (!handlers.signal.aborted) {
      handlers.onStatus?.(attempt === 0 ? "connecting" : "reconnecting");
      try {
        const headers: Record<string, string> = { Authorization: `Bearer ${this.token}`, Accept: "text/event-stream" };
        if (lastEventId > 0) headers["Last-Event-ID"] = String(lastEventId);
        const response = await this.fetchImpl(url, { headers, signal: handlers.signal });
        if (!response.ok) throw await errorFrom(response);
        if (!response.body) throw new CompanyApiError(502, "no_body", "empty live stream");
        for await (const message of parseSSE(response.body)) {
          if (message.event === "event") {
            const event = JSON.parse(message.data) as TimelineEvent;
            if (event.sequence_number <= lastEventId) continue;
            lastEventId = event.sequence_number;
            handlers.onEvent(event);
          } else if (message.event === "caught_up") {
            attempt = 0;
            handlers.onStatus?.("live");
            handlers.onCaughtUp?.(lastEventId);
          }
        }
      } catch (error) {
        if (handlers.signal.aborted) return;
        if (error instanceof CompanyApiError && fatalStatuses.has(error.status)) throw error;
        handlers.onStatus?.("reconnecting", error);
      }
      if (handlers.signal.aborted) return;
      attempt++;
      await sleep(retryDelay(attempt), handlers.signal);
    }
  }

  private async request<T>(method: string, path: string, body?: unknown): Promise<T> {
    const headers: Record<string, string> = { Authorization: `Bearer ${this.token}`, Accept: "application/json" };
    if (body !== undefined) headers["Content-Type"] = "application/json";
    const response = await this.fetchImpl(this.baseUrl + path, {
      method,
      headers,
      body: body === undefined ? undefined : JSON.stringify(body),
    });
    if (!response.ok) throw await errorFrom(response);
    if (response.status === 204) return undefined as T;
    return (await response.json()) as T;
  }
}

async function errorFrom(response: Response): Promise<CompanyApiError> {
  let code = "http_error";
  let message = response.statusText;
  try {
    const error = (await response.json()) as { code?: string; message?: string };
    code = error.code ?? code;
    message = error.message ?? message;
  } catch {
    // Not a JSON error body.
  }
  return new CompanyApiError(response.status, code, message);
}

function sleep(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    if (signal.aborted) return resolve();
    const timer = setTimeout(done, ms);
    signal.addEventListener("abort", done, { once: true });
    function done() {
      clearTimeout(timer);
      signal.removeEventListener("abort", done);
      resolve();
    }
  });
}
