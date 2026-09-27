import { act, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it } from "vitest";
import { App } from "../src/App";
import { fakeWorkspace, WORKSPACE } from "./fakeWorkspace";

async function startedApp(workspace = fakeWorkspace()) {
  const user = userEvent.setup();
  render(<App workspaceUrl={WORKSPACE} fetch={workspace.fetch} invite="good-invite" diffDelayMs={30} />);
  await user.click(screen.getByRole("button", { name: "Start" }));
  await screen.findByRole("textbox", { name: "Code" });
  return { user, workspace };
}

describe("candidate app", () => {
  it("rejects a bad invite and starts with a good one", async () => {
    const workspace = fakeWorkspace();
    const user = userEvent.setup();
    render(<App workspaceUrl={WORKSPACE} fetch={workspace.fetch} />);
    await user.type(screen.getByLabelText("Invite code"), "bad-invite");
    await user.click(screen.getByRole("button", { name: "Start" }));
    expect((await screen.findByRole("alert")).textContent).toContain("invalid, expired, or already used");
    await user.clear(screen.getByLabelText("Invite code"));
    await user.type(screen.getByLabelText("Invite code"), "good-invite");
    await user.click(screen.getByRole("button", { name: "Start" }));
    expect(await screen.findByRole("textbox", { name: "Code" })).toBeTruthy();
    expect(sessionStorage.getItem("ta-platform.candidate.session")).toBe("tok-1");
  });

  it("streams a prompt answer as it arrives", async () => {
    const { user, workspace } = await startedApp();
    await user.type(screen.getByLabelText("Ask the assistant"), "How do I reverse a list?");
    await user.click(screen.getByRole("button", { name: "Send" }));
    await waitFor(() => expect(workspace.calls.some((c) => c.path === "/session/prompt")).toBe(true));

    act(() => workspace.stream.send("prompt", { promptId: "p-1", sequenceNumber: 3 }));
    act(() => workspace.stream.send("delta", { text: "Use " }));
    const answer = await screen.findByLabelText("answer");
    await waitFor(() => expect(answer.textContent).toBe("Use "));
    act(() => workspace.stream.send("delta", { text: "reversed()." }));
    await waitFor(() => expect(answer.textContent).toBe("Use reversed()."));
    expect(screen.getByRole("button", { name: "Stop" })).toBeTruthy();

    act(() => {
      workspace.stream.send("done", { status: "completed", stopReason: "end_turn" });
      workspace.stream.close();
    });
    expect(await screen.findByRole("button", { name: "Send" })).toBeTruthy();
    const request = workspace.calls.find((c) => c.path === "/session/prompt")!;
    expect(request.body).toEqual({ prompt: "How do I reverse a list?", history: [] });
    expect(request.auth).toBe("Bearer tok-1");
  });

  it("stops a streaming answer by aborting the request", async () => {
    const { user, workspace } = await startedApp();
    await user.type(screen.getByLabelText("Ask the assistant"), "long answer please");
    await user.click(screen.getByRole("button", { name: "Send" }));
    await waitFor(() => expect(workspace.calls.some((c) => c.path === "/session/prompt")).toBe(true));
    act(() => workspace.stream.send("delta", { text: "partial" }));
    await user.click(await screen.findByRole("button", { name: "Stop" }));
    expect(workspace.stream.aborted).toBe(true);
    expect(await screen.findByText("Stopped.")).toBeTruthy();
  });

  it("runs code and shows the result, and explains a 429", async () => {
    const { user, workspace } = await startedApp();
    await user.click(screen.getByRole("button", { name: "Run" }));
    await waitFor(() => expect(workspace.pendingRuns()).toBe(1));
    expect((screen.getByRole("button", { name: "Running…" }) as HTMLButtonElement).disabled).toBe(true);
    act(() =>
      workspace.finishRun(200, {
        executionId: "e1", status: "timed_out", exitCode: -1, stdout: "tick\n", stderr: "",
        stdoutTruncated: false, stderrTruncated: false, durationMs: 10000,
      }),
    );
    expect(await screen.findByText(/Timed out/)).toBeTruthy();
    expect(screen.getByLabelText("stdout").textContent).toBe("tick\n");
    const run = workspace.calls.find((c) => c.path === "/session/run")!;
    expect(run.body).toMatchObject({ language: "python", entrypoint: "main.py", files: { "main.py": expect.any(String) } });

    await user.click(screen.getByRole("button", { name: "Run" }));
    await waitFor(() => expect(workspace.pendingRuns()).toBe(1));
    act(() => workspace.finishRun(429, { code: "run_in_progress", message: "busy" }));
    expect((await screen.findByRole("alert")).textContent).toContain("A run is already in progress");
  });

  it("posts debounced manual diffs, and AI diffs with their prompt", async () => {
    const { user, workspace } = await startedApp();
    const editor = screen.getByRole("textbox", { name: "Code" });
    await user.clear(editor);
    await user.type(editor, "print(1)");
    await waitFor(() => expect(workspace.diffs()).toHaveLength(1));
    expect(workspace.diffs()[0]).toMatchObject({ clientSeq: 1, origin: "manual", path: "main.py" });
    expect(String(workspace.diffs()[0].patch)).toContain("+print(1)");

    await user.type(screen.getByLabelText("Ask the assistant"), "fix it");
    await user.click(screen.getByRole("button", { name: "Send" }));
    await waitFor(() => expect(workspace.calls.some((c) => c.path === "/session/prompt")).toBe(true));
    act(() => {
      workspace.stream.send("prompt", { promptId: "p-9", sequenceNumber: 5 });
      workspace.stream.send("delta", { text: "Try:\n```python\nprint(2)\n```\n" });
      workspace.stream.send("done", { status: "completed" });
      workspace.stream.close();
    });
    await user.click(await screen.findByRole("button", { name: "Apply code" }));
    await waitFor(() => expect(workspace.diffs()).toHaveLength(2));
    expect(workspace.diffs()[1]).toMatchObject({ clientSeq: 2, origin: "ai_applied", promptId: "p-9", path: "main.py" });
    expect((editor as HTMLTextAreaElement).value).toBe("print(2)\n");
  });

  it("talks only to the Candidate Workspace", async () => {
    const { user, workspace } = await startedApp();
    await user.type(screen.getByRole("textbox", { name: "Code" }), "x");
    await user.click(screen.getByRole("button", { name: "Run" }));
    await waitFor(() => expect(workspace.pendingRuns()).toBe(1));
    act(() => workspace.finishRun(200, { executionId: "e", status: "succeeded", exitCode: 0, stdout: "", stderr: "", stdoutTruncated: false, stderrTruncated: false, durationMs: 1 }));
    await waitFor(() => expect(workspace.diffs().length).toBeGreaterThan(0));
    expect(workspace.calls.length).toBeGreaterThanOrEqual(3);
    for (const call of workspace.calls) expect(call.url.startsWith(WORKSPACE + "/session/")).toBe(true);
  });
});
