import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { DiffDebouncer, makePatch } from "../src/diffs";
import type { DiffRequest, DiffResult } from "../src/workspace";

describe("makePatch", () => {
  it("returns null when nothing changed", () => {
    expect(makePatch("main.py", "a\n", "a\n")).toBeNull();
  });

  it("emits only ---/+++ headers and hunks", () => {
    const patch = makePatch("main.py", "a\nb\nc\n", "a\nB\nc\nd\n")!;
    expect(patch).toBe("--- a/main.py\n+++ b/main.py\n@@ -1,3 +1,4 @@\n a\n-b\n+B\n c\n+d\n");
  });

  it("marks a missing final newline", () => {
    expect(makePatch("x", "a", "b")).toBe("--- a/x\n+++ b/x\n@@ -1,1 +1,1 @@\n-a\n\\ No newline at end of file\n+b\n\\ No newline at end of file\n");
  });

  it("diffs from an empty file", () => {
    expect(makePatch("new.py", "", "one\ntwo\n")).toBe("--- a/new.py\n+++ b/new.py\n@@ -0,0 +1,2 @@\n+one\n+two\n");
  });
});

describe("DiffDebouncer", () => {
  let sent: DiffRequest[];
  let resolveNext: Array<() => void>;
  const client = {
    submitDiff: (request: DiffRequest): Promise<DiffResult> => {
      sent.push(request);
      return Promise.resolve({ accepted: true, linesAdded: 0, linesRemoved: 0 });
    },
  };

  beforeEach(() => {
    vi.useFakeTimers();
    sent = [];
    resolveNext = [];
  });
  afterEach(() => vi.useRealTimers());

  it("coalesces a burst of keystrokes into one manual diff", async () => {
    const debouncer = new DiffDebouncer({ client, delayMs: 500 });
    debouncer.track("main.py", "");
    for (const content of ["p", "pr", "pri", "print(1)\n"]) {
      debouncer.edit("main.py", content);
      await vi.advanceTimersByTimeAsync(200);
    }
    expect(sent).toHaveLength(0);
    await vi.advanceTimersByTimeAsync(500);
    expect(sent).toEqual([{ clientSeq: 1, origin: "manual", path: "main.py", patch: makePatch("main.py", "", "print(1)\n") }]);
  });

  it("numbers diffs in order and diffs each burst against the last one", async () => {
    const debouncer = new DiffDebouncer({ client, delayMs: 100 });
    debouncer.track("main.py", "a\n");
    debouncer.edit("main.py", "b\n");
    await vi.advanceTimersByTimeAsync(100);
    debouncer.edit("main.py", "c\n");
    await vi.advanceTimersByTimeAsync(100);
    expect(sent.map((d) => d.clientSeq)).toEqual([1, 2]);
    expect(sent[1].patch).toBe(makePatch("main.py", "b\n", "c\n"));
  });

  it("flushes pending manual edits before an AI diff so origins stay separate", async () => {
    const debouncer = new DiffDebouncer({ client, delayMs: 1000 });
    debouncer.track("main.py", "x = 1\n");
    debouncer.edit("main.py", "x = 2\n");
    await debouncer.applyAI("main.py", "x = 2\ny = 3\n", "prompt-1");
    expect(sent.map((d) => [d.clientSeq, d.origin, d.promptId])).toEqual([
      [1, "manual", undefined],
      [2, "ai_applied", "prompt-1"],
    ]);
    expect(sent[1].patch).toBe(makePatch("main.py", "x = 2\n", "x = 2\ny = 3\n"));
    await vi.advanceTimersByTimeAsync(2000);
    expect(sent).toHaveLength(2);
  });

  it("does not post an edit that returns to the posted content", async () => {
    const debouncer = new DiffDebouncer({ client, delayMs: 100 });
    debouncer.track("main.py", "same\n");
    debouncer.edit("main.py", "changed\n");
    debouncer.edit("main.py", "same\n");
    await vi.advanceTimersByTimeAsync(100);
    expect(sent).toHaveLength(0);
  });

  it("sends posts one at a time, in sequence order", async () => {
    const order: number[] = [];
    const slow = {
      submitDiff: (request: DiffRequest) =>
        new Promise<DiffResult>((resolve) => {
          order.push(request.clientSeq);
          resolveNext.push(() => resolve({ accepted: true, linesAdded: 0, linesRemoved: 0 }));
        }),
    };
    const debouncer = new DiffDebouncer({ client: slow, delayMs: 10 });
    debouncer.track("a.py", "");
    debouncer.track("b.py", "");
    debouncer.edit("a.py", "1\n");
    debouncer.edit("b.py", "2\n");
    await vi.advanceTimersByTimeAsync(10);
    expect(order).toEqual([1]);
    resolveNext.shift()!();
    await vi.advanceTimersByTimeAsync(0);
    expect(order).toEqual([1, 2]);
    resolveNext.shift()!();
    await debouncer.flush();
  });

  it("reports failures and keeps going", async () => {
    const results: Array<DiffResult | Error> = [];
    let fail = true;
    const flaky = {
      submitDiff: (_: DiffRequest) => (fail ? ((fail = false), Promise.reject(new Error("offline"))) : Promise.resolve({ accepted: true, linesAdded: 1, linesRemoved: 0 })),
    };
    const debouncer = new DiffDebouncer({ client: flaky, delayMs: 10, onPosted: (_, result) => results.push(result) });
    debouncer.track("a.py", "");
    debouncer.edit("a.py", "1\n");
    await debouncer.flush();
    debouncer.edit("a.py", "2\n");
    await debouncer.flush();
    expect(results[0]).toBeInstanceOf(Error);
    expect(results[1]).toMatchObject({ accepted: true });
  });

  it("dispose cancels pending posts", async () => {
    const debouncer = new DiffDebouncer({ client, delayMs: 10 });
    debouncer.track("a.py", "");
    debouncer.edit("a.py", "1\n");
    debouncer.dispose();
    await vi.advanceTimersByTimeAsync(100);
    expect(sent).toHaveLength(0);
  });
});
