import { describe, expect, it } from "vitest";
import type { TimelineEvent } from "../src/company";
import { describeEvent, replayState } from "../src/replay";
import { starterFiles, WORKSPACE_STARTERS } from "../src/starters";
import fixtures from "./fixtures/patches.json";

let seq = 0;
function ev(event_type: string, payload: Record<string, unknown>): TimelineEvent {
  seq++;
  return {
    event_id: `e${seq}`,
    company_id: "c",
    interview_id: "i",
    sequence_number: seq,
    event_type,
    occurred_at: "2026-09-27T00:00:00Z",
    payload,
  };
}

describe("replayState", () => {
  it("rebuilds every shared fixture from its patch", () => {
    for (const fixture of fixtures as { name: string; path: string; before: string; after: string; patch: string }[]) {
      const state = replayState([ev("code.diff", { path: fixture.path, patch: fixture.patch, origin: "manual" })], {
        [fixture.path]: fixture.before,
      });
      expect(state.files[fixture.path], fixture.name).toBe(fixture.after);
      expect(state.failedPatches, fixture.name).toEqual([]);
    }
  });

  it("follows a session: diffs from the starter, prompts, runs, and a scrub position", () => {
    seq = 0;
    const py = WORKSPACE_STARTERS.python;
    const events = [
      ev("session.started", {}),
      ev("code.diff", {
        path: py.file,
        origin: "manual",
        patch: "--- a/main.py\n+++ b/main.py\n@@ -1,2 +1,2 @@\n def solve():\n-    pass\n+    return 42\n",
        lines_added: 1,
        lines_removed: 1,
      }),
      ev("prompt.submitted", { prompt_id: "p1", prompt: "is this right?" }),
      ev("ai.response.completed", { prompt_id: "p1", response_text: "Yes.", status: "completed" }),
      ev("execution.requested", { execution_id: "x1", language: "python", entrypoint: "main.py" }),
      ev("execution.completed", { execution_id: "x1", status: "succeeded", exit_code: 0, stdout: "42\n", stderr: "", duration_ms: 12 }),
      ev("code.diff", {
        path: py.file,
        origin: "ai_applied",
        patch: "--- a/main.py\n+++ b/main.py\n@@ -1,2 +1,2 @@\n def solve():\n-    return 42\n+    return 6 * 7\n",
      }),
    ];
    const full = replayState(events, starterFiles());
    expect(full.files["main.py"]).toBe("def solve():\n    return 6 * 7\n\n\nprint(solve())\n");
    expect(full.files["main.js"]).toBe(WORKSPACE_STARTERS.javascript.starter);
    expect(full.activeFile).toBe("main.py");
    expect(full.diffs).toBe(2);
    expect(full.aiAppliedDiffs).toBe(1);
    expect(full.prompts).toEqual([{ seq: 3, promptId: "p1", prompt: "is this right?", response: "Yes.", status: "completed" }]);
    expect(full.runs).toEqual([
      { seq: 5, executionId: "x1", language: "python", status: "succeeded", exitCode: 0, stdout: "42\n", stderr: "", durationMs: 12 },
    ]);
    expect(full.seq).toBe(7);

    const early = replayState(events, starterFiles(), 3);
    expect(early.files["main.py"]).toBe("def solve():\n    return 42\n\n\nprint(solve())\n");
    expect(early.prompts[0].response).toBeUndefined();
    expect(early.runs).toEqual([]);
    expect(early.seq).toBe(3);

    // Out-of-order input is replayed in sequence order.
    expect(replayState([...events].reverse(), starterFiles()).files).toEqual(full.files);
  });

  it("records patches that do not apply and keeps going", () => {
    seq = 0;
    const events = [
      ev("code.diff", { path: "main.py", origin: "manual", patch: "--- a/main.py\n+++ b/main.py\n@@ -1,1 +1,1 @@\n-nope\n+x\n" }),
      ev("code.diff", { path: "new.py", origin: "manual", patch: "--- a/new.py\n+++ b/new.py\n@@ -0,0 +1,1 @@\n+ok\n" }),
      ev("code.diff", { path: "bad.py" }),
    ];
    const state = replayState(events, { "main.py": "original\n" });
    expect(state.failedPatches).toEqual([1]);
    expect(state.files).toEqual({ "main.py": "original\n", "new.py": "ok\n" });
    expect(state.diffs).toBe(2);
  });

  it("ignores completions whose request it never saw", () => {
    seq = 0;
    const state = replayState([
      ev("ai.response.completed", { prompt_id: "ghost", response_text: "x" }),
      ev("execution.completed", { execution_id: "ghost", status: "failed" }),
    ]);
    expect(state.prompts).toEqual([]);
    expect(state.runs).toEqual([]);
    expect(state.seq).toBe(2);
  });
});

describe("describeEvent", () => {
  it("describes each event type", () => {
    expect(describeEvent(ev("session.started", {}))).toBe("Candidate started the session");
    expect(describeEvent(ev("prompt.submitted", { prompt: "how do I\n  sort?" }))).toBe("Asked the assistant: how do I sort?");
    expect(describeEvent(ev("prompt.submitted", { prompt: "x".repeat(200) }))).toHaveLength(21 + 80);
    expect(describeEvent(ev("ai.response.completed", { status: "refused" }))).toBe("Assistant answered (refused)");
    expect(describeEvent(ev("code.diff", { path: "a.py", origin: "manual", lines_added: 3, lines_removed: 1 }))).toBe(
      "Edited a.py (+3 −1)",
    );
    expect(describeEvent(ev("code.diff", { path: "a.py", origin: "ai_applied" }))).toBe("Applied AI code to a.py (+0 −0)");
    expect(describeEvent(ev("execution.requested", { entrypoint: "main.py" }))).toBe("Ran main.py");
    expect(describeEvent(ev("execution.completed", { status: "failed", exit_code: 1 }))).toBe("Run failed (exit 1)");
    expect(describeEvent(ev("job.posted", {}))).toBe("job.posted");
  });
});

describe("starterFiles", () => {
  it("maps each language's file to its starter", () => {
    expect(starterFiles()).toEqual({
      "main.py": WORKSPACE_STARTERS.python.starter,
      "main.js": WORKSPACE_STARTERS.javascript.starter,
    });
  });
});
