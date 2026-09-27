import { structuredPatch } from "diff";
import type { DiffRequest, DiffResult } from "./workspace";

/**
 * Returns a single-file unified diff from before to after, or null when
 * nothing changed. Only ---/+++ headers are emitted; the workspace rejects
 * other preambles.
 */
export function makePatch(path: string, before: string, after: string): string | null {
  if (before === after) return null;
  const patch = structuredPatch(`a/${path}`, `b/${path}`, before, after, undefined, undefined, { context: 3 });
  if (patch.hunks.length === 0) return null;
  const lines = [`--- a/${path}`, `+++ b/${path}`];
  for (const hunk of patch.hunks) {
    // A zero-length range names the line before it (so an empty file is
    // -0,0); jsdiff's own formatPatch makes the same adjustment.
    const oldStart = hunk.oldLines === 0 ? hunk.oldStart - 1 : hunk.oldStart;
    const newStart = hunk.newLines === 0 ? hunk.newStart - 1 : hunk.newStart;
    lines.push(`@@ -${oldStart},${hunk.oldLines} +${newStart},${hunk.newLines} @@`, ...hunk.lines);
  }
  return lines.join("\n") + "\n";
}

export interface DiffSender {
  submitDiff(request: DiffRequest): Promise<DiffResult>;
}

export interface DiffDebouncerOptions {
  client: DiffSender;
  /** Idle time after the last keystroke before a manual diff is posted. */
  delayMs?: number;
  /** Called after each post, with the request and the server's answer or error. */
  onPosted?: (request: DiffRequest, result: DiffResult | Error) => void;
}

/**
 * Turns editor changes into ordered diff posts. Manual edits are debounced
 * into one diff per burst; an applied AI suggestion first flushes pending
 * manual edits, so each diff has a single, correct origin. Posts are sent
 * one at a time with increasing clientSeq, so the workspace never has to
 * drop one of ours as out of order.
 */
export class DiffDebouncer {
  private readonly client: DiffSender;
  private readonly delayMs: number;
  private readonly onPosted?: DiffDebouncerOptions["onPosted"];
  private readonly posted = new Map<string, string>();
  private readonly current = new Map<string, string>();
  private readonly timers = new Map<string, ReturnType<typeof setTimeout>>();
  private sequence = 0;
  private queue: Promise<void> = Promise.resolve();

  constructor(options: DiffDebouncerOptions) {
    this.client = options.client;
    this.delayMs = options.delayMs ?? 750;
    this.onPosted = options.onPosted;
  }

  /** Sets the content a file had before any tracked edit. */
  track(path: string, content: string): void {
    this.posted.set(path, content);
    this.current.set(path, content);
  }

  /** Records a manual edit; the diff is posted once typing pauses. */
  edit(path: string, content: string): void {
    this.current.set(path, content);
    const existing = this.timers.get(path);
    if (existing) clearTimeout(existing);
    this.timers.set(
      path,
      setTimeout(() => {
        this.timers.delete(path);
        this.enqueue(path, "manual");
      }, this.delayMs),
    );
  }

  /** Records an applied AI suggestion and posts it immediately. */
  applyAI(path: string, content: string, promptId: string): Promise<void> {
    this.flushPath(path);
    this.current.set(path, content);
    return this.enqueue(path, "ai_applied", promptId);
  }

  /** Posts every pending manual edit now and waits for all posts. */
  flush(): Promise<void> {
    for (const path of [...this.timers.keys()]) this.flushPath(path);
    return this.queue;
  }

  /** Cancels pending timers without posting. */
  dispose(): void {
    for (const timer of this.timers.values()) clearTimeout(timer);
    this.timers.clear();
  }

  private flushPath(path: string): void {
    const timer = this.timers.get(path);
    if (!timer) return;
    clearTimeout(timer);
    this.timers.delete(path);
    this.enqueue(path, "manual");
  }

  private enqueue(path: string, origin: DiffRequest["origin"], promptId?: string): Promise<void> {
    // Compute the patch now, against what was last queued, so bursts stay
    // separate even while an earlier post is in flight.
    const before = this.posted.get(path) ?? "";
    const after = this.current.get(path) ?? "";
    const patch = makePatch(path, before, after);
    if (patch === null) return this.queue;
    this.posted.set(path, after);
    const request: DiffRequest = { clientSeq: ++this.sequence, origin, path, patch };
    if (promptId) request.promptId = promptId;
    this.queue = this.queue.then(async () => {
      let outcome: DiffResult | Error;
      try {
        outcome = await this.client.submitDiff(request);
      } catch (error) {
        outcome = error instanceof Error ? error : new Error(String(error));
      }
      this.onPosted?.(request, outcome);
    });
    return this.queue;
  }
}
