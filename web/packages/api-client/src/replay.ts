import { applyPatch } from "diff";
import type { TimelineEvent } from "./company";

export interface ReplayPrompt {
  seq: number;
  promptId: string;
  prompt: string;
  response?: string;
  status?: string;
}

export interface ReplayRun {
  seq: number;
  executionId: string;
  language: string;
  status?: string;
  exitCode?: number;
  stdout?: string;
  stderr?: string;
  durationMs?: number;
}

export interface ReplayState {
  /** Workspace files as of the replay position. */
  files: Record<string, string>;
  /** The file the most recent diff touched. */
  activeFile?: string;
  prompts: ReplayPrompt[];
  runs: ReplayRun[];
  diffs: number;
  aiAppliedDiffs: number;
  /** Sequence numbers of diffs whose patch did not apply. */
  failedPatches: number[];
  /** The last event applied. */
  seq: number;
}

type Payload = Record<string, unknown>;
const str = (v: unknown) => (typeof v === "string" ? v : undefined);
const num = (v: unknown) => (typeof v === "number" ? v : undefined);

/**
 * Rebuilds the candidate's workspace from an interview timeline, applying
 * events up to and including uptoSeq (all of them by default). Files start
 * from initialFiles (the workspace starters) and change by code.diff
 * patches; a patch that does not apply is recorded and skipped.
 */
export function replayState(
  events: readonly TimelineEvent[],
  initialFiles: Record<string, string> = {},
  uptoSeq = Number.POSITIVE_INFINITY,
): ReplayState {
  const state: ReplayState = {
    files: { ...initialFiles },
    prompts: [],
    runs: [],
    diffs: 0,
    aiAppliedDiffs: 0,
    failedPatches: [],
    seq: 0,
  };
  const prompts = new Map<string, ReplayPrompt>();
  const runs = new Map<string, ReplayRun>();
  const ordered = [...events].sort((a, b) => a.sequence_number - b.sequence_number);
  for (const event of ordered) {
    if (event.sequence_number > uptoSeq) break;
    const p = (event.payload ?? {}) as Payload;
    switch (event.event_type) {
      case "code.diff": {
        const path = str(p.path);
        const patch = str(p.patch);
        if (!path || !patch) break;
        state.diffs++;
        if (p.origin === "ai_applied") state.aiAppliedDiffs++;
        const next = applyPatch(state.files[path] ?? "", patch);
        if (next === false) {
          state.failedPatches.push(event.sequence_number);
        } else {
          state.files[path] = next;
          state.activeFile = path;
        }
        break;
      }
      case "prompt.submitted": {
        const prompt: ReplayPrompt = { seq: event.sequence_number, promptId: str(p.prompt_id) ?? "", prompt: str(p.prompt) ?? "" };
        prompts.set(prompt.promptId, prompt);
        state.prompts.push(prompt);
        break;
      }
      case "ai.response.completed": {
        const prompt = prompts.get(str(p.prompt_id) ?? "");
        if (prompt) {
          prompt.response = str(p.response_text) ?? "";
          prompt.status = str(p.status);
        }
        break;
      }
      case "execution.requested": {
        const run: ReplayRun = { seq: event.sequence_number, executionId: str(p.execution_id) ?? "", language: str(p.language) ?? "" };
        runs.set(run.executionId, run);
        state.runs.push(run);
        break;
      }
      case "execution.completed": {
        const run = runs.get(str(p.execution_id) ?? "");
        if (run) {
          run.status = str(p.status);
          run.exitCode = num(p.exit_code);
          run.stdout = str(p.stdout);
          run.stderr = str(p.stderr);
          run.durationMs = num(p.duration_ms);
        }
        break;
      }
    }
    state.seq = event.sequence_number;
  }
  return state;
}

/** A one-line, human-readable description of an event for timelines. */
export function describeEvent(event: TimelineEvent): string {
  const p = (event.payload ?? {}) as Payload;
  switch (event.event_type) {
    case "session.started":
      return "Candidate started the session";
    case "prompt.submitted":
      return `Asked the assistant: ${truncate(str(p.prompt) ?? "", 80)}`;
    case "ai.response.completed":
      return `Assistant answered (${str(p.status) ?? "unknown"})`;
    case "code.diff":
      return `${p.origin === "ai_applied" ? "Applied AI code to" : "Edited"} ${str(p.path) ?? "a file"} (+${num(p.lines_added) ?? 0} −${num(p.lines_removed) ?? 0})`;
    case "execution.requested":
      return `Ran ${str(p.entrypoint) ?? "the code"}`;
    case "execution.completed":
      return `Run ${str(p.status) ?? "finished"}${num(p.exit_code) !== undefined ? ` (exit ${num(p.exit_code)})` : ""}`;
    default:
      return event.event_type;
  }
}

function truncate(text: string, max: number): string {
  const line = text.replace(/\s+/g, " ").trim();
  return line.length > max ? `${line.slice(0, max - 1)}…` : line;
}
