import { act, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import { App } from "../src/App";
import { ADMIN, CANDIDATE, LIVE, TOKEN, fakeBackend, timelineEvent } from "./fakeBackend";

function renderApp(backend = fakeBackend(), hash = "#/dashboard") {
  window.location.hash = hash;
  sessionStorage.setItem("ta-platform.company.session", TOKEN);
  render(<App adminUrl={ADMIN} liveUrl={LIVE} candidateUrl={CANDIDATE} fetch={backend.fetch} playStepMs={10} />);
  return { backend, user: userEvent.setup() };
}

const PATCH_1 = "--- a/main.py\n+++ b/main.py\n@@ -1,2 +1,2 @@\n def solve():\n-    pass\n+    return 42\n";
const PATCH_2 = "--- a/main.py\n+++ b/main.py\n@@ -1,2 +1,2 @@\n def solve():\n-    return 42\n+    return 6 * 7\n";

afterEach(() => {
  window.location.hash = "";
});

describe("sign in", () => {
  it("rejects a bad token and keeps a good one", async () => {
    const backend = fakeBackend();
    window.location.hash = "#/dashboard";
    const user = userEvent.setup();
    render(<App adminUrl={ADMIN} liveUrl={LIVE} candidateUrl={CANDIDATE} fetch={backend.fetch} />);
    await user.type(screen.getByLabelText("Session token"), "wrong");
    await user.click(screen.getByRole("button", { name: "Sign in" }));
    expect((await screen.findByRole("alert")).textContent).toBe("That session token is not valid.");
    await user.clear(screen.getByLabelText("Session token"));
    await user.type(screen.getByLabelText("Session token"), ` ${TOKEN} `);
    await user.click(screen.getByRole("button", { name: "Sign in" }));
    expect(await screen.findByRole("heading", { name: "Dashboard" })).toBeTruthy();
    expect(sessionStorage.getItem("ta-platform.company.session")).toBe(TOKEN);
    await user.click(screen.getByRole("button", { name: "Sign out" }));
    expect(screen.getByLabelText("Session token")).toBeTruthy();
    expect(sessionStorage.getItem("ta-platform.company.session")).toBeNull();
  });
});

describe("dashboard", () => {
  it("shows totals, a filled daily chart, and interview activity", async () => {
    const { backend, user } = renderApp();
    expect((await screen.findByTestId("tile-code.diff")).textContent).toBe("8");
    expect(screen.getByTestId("tile-session.started").textContent).toBe("2");
    expect(screen.getByTestId("tile-execution.requested").textContent).toBe("0");
    const chart = screen.getByRole("img", { name: /Events per day, 2026-09-21 to 2026-10-04/ });
    expect(chart.querySelectorAll("path.bar")).toHaveLength(2);
    // The table view lists every day, including empty ones.
    const rows = within(screen.getByText("Table view").closest("details")!).getAllByRole("row");
    expect(rows).toHaveLength(15);
    expect(await screen.findByRole("cell", { name: "Ada" })).toBeTruthy();
    expect(screen.getByRole("cell", { name: "3 (2)" })).toBeTruthy();

    await user.selectOptions(screen.getByLabelText("Range"), "7");
    await waitFor(() => expect(backend.calls.some((c) => c.path === "/dashboard/overview?days=7")).toBe(true));
    expect(backend.calls.every((c) => c.auth === `Bearer ${TOKEN}`)).toBe(true);
  });

  it("shows a hover tooltip per bar", async () => {
    renderApp();
    const chart = await screen.findByRole("img", { name: /Events per day/ });
    // The last bar is 2026-09-27: 3 edits + 2 sessions.
    const day = Array.from(chart.querySelectorAll("path.bar")).at(-1)!.parentElement!;
    fireEvent.mouseEnter(day);
    expect(screen.getByRole("status").textContent).toBe("2026-09-275 events");
    fireEvent.mouseLeave(day);
    expect(screen.queryByRole("status")).toBeNull();
  });

  it("reports an unavailable dashboard", async () => {
    const backend = fakeBackend();
    backend.state.fail.set("GET /dashboard/overview", () => new Response("{}", { status: 503 }));
    renderApp(backend);
    expect((await screen.findByRole("alert")).textContent).toBe("The service is unavailable. Try again shortly.");
  });
});

describe("interviews", () => {
  it("schedules an interview and issues a candidate invite link", async () => {
    const { backend, user } = renderApp(fakeBackend(), "#/interviews");
    await screen.findByText("Ada");
    await user.type(screen.getByLabelText("Candidate name"), "Grace");
    await user.type(screen.getByLabelText("Candidate email"), "grace@x.test");
    await user.click(screen.getByRole("button", { name: "Schedule interview" }));
    expect(await screen.findByText("Grace")).toBeTruthy();
    expect(backend.calls.find((c) => c.method === "POST" && c.path === "/interviews")?.body).toEqual({
      candidate_name: "Grace",
      candidate_email: "grace@x.test",
    });

    const graceRow = screen.getByText("Grace").closest("tr")!;
    await user.click(within(graceRow).getByRole("button", { name: "Invite candidate" }));
    const link = (await screen.findByLabelText("Invite link")) as HTMLInputElement;
    expect(link.value).toBe(`${CANDIDATE}/?invite=one-time%2Ftoken`);
    expect(backend.calls.find((c) => c.path === "/invites")?.body).toEqual({ email: "grace@x.test", interview_id: "iv-2" });
    expect(within(graceRow).getByRole("link", { name: "Watch live" }).getAttribute("href")).toBe("#/interviews/iv-2/live");
  });

  it("requests erasure only after confirmation, and shows it as requested", async () => {
    const { backend, user } = renderApp(fakeBackend(), "#/interviews");
    const row = (await screen.findByText("Ada")).closest("tr")!;
    const confirm = vi.spyOn(window, "confirm").mockReturnValueOnce(false).mockReturnValueOnce(true);
    await user.click(within(row).getByRole("button", { name: "Request erasure" }));
    expect(backend.calls.some((c) => c.path.endsWith("/erase"))).toBe(false);
    await user.click(within(row).getByRole("button", { name: "Request erasure" }));
    expect((await screen.findByRole("status")).textContent).toContain("Erasure requested");
    expect(await screen.findByText("erasure requested")).toBeTruthy();
    expect(within(screen.getByText("Ada").closest("tr")!).queryByRole("button", { name: "Request erasure" })).toBeNull();
    confirm.mockRestore();
  });

  it("shows the server's reason when a role may not act", async () => {
    const backend = fakeBackend();
    backend.state.fail.set("POST /interviews/iv-1/erase", () =>
      new Response(JSON.stringify({ code: "forbidden", message: 'role "viewer" may not interviews:erase' }), { status: 403 }),
    );
    const { user } = renderApp(backend, "#/interviews");
    const row = (await screen.findByText("Ada")).closest("tr")!;
    vi.spyOn(window, "confirm").mockReturnValueOnce(true);
    await user.click(within(row).getByRole("button", { name: "Request erasure" }));
    expect((await screen.findByRole("alert")).textContent).toBe('Not allowed: role "viewer" may not interviews:erase');
  });

  it("marks an interview complete", async () => {
    const { backend, user } = renderApp(fakeBackend(), "#/interviews");
    const row = (await screen.findByText("Ada")).closest("tr")!;
    await user.click(within(row).getByRole("button", { name: "Mark complete" }));
    await waitFor(() => expect(within(screen.getByText("Ada").closest("tr")!).getByText("completed")).toBeTruthy());
    expect(backend.calls.find((c) => c.method === "PATCH")?.body).toEqual({ status: "completed" });
  });
});

describe("live view", () => {
  it("follows events as they arrive and rebuilds the candidate's code", async () => {
    const { backend } = renderApp(fakeBackend(), "#/interviews/iv-1/live");
    expect(await screen.findByRole("heading", { name: /Ada/ })).toBeTruthy();
    await waitFor(() => expect(backend.state.streams).toHaveLength(1));
    const stream = backend.state.streams[0];
    const liveCall = backend.calls.find((c) => c.path.endsWith("/live"))!;
    expect(liveCall.path).toBe("/interviews/iv-1/live");
    expect(liveCall.auth).toBe(`Bearer ${TOKEN}`);

    act(() => {
      stream.send("event", timelineEvent(1, "session.started", {}), 1);
      stream.send("caught_up", { last_event_id: 1 });
    });
    expect((await screen.findByRole("status", { name: "Connection" })).textContent).toBe("Live");
    act(() => stream.send("event", timelineEvent(2, "code.diff", { path: "main.py", origin: "manual", patch: PATCH_1 }), 2));
    await waitFor(() => expect(screen.getByTestId("code").textContent).toContain("return 42"));
    act(() => {
      stream.send("event", timelineEvent(3, "execution.requested", { execution_id: "x1", language: "python", entrypoint: "main.py" }), 3);
      stream.send("event", timelineEvent(4, "execution.completed", { execution_id: "x1", status: "succeeded", exit_code: 0, stdout: "42\n" }), 4);
    });
    await waitFor(() => expect(screen.getByTestId("run").textContent).toContain("succeeded"));
    expect(screen.getByText("Ran main.py")).toBeTruthy();
  });

  it("reconnects from the last event after the stream drops", async () => {
    const { backend } = renderApp(fakeBackend(), "#/interviews/iv-1/live");
    await waitFor(() => expect(backend.state.streams).toHaveLength(1));
    act(() => {
      backend.state.streams[0].send("event", timelineEvent(1, "session.started", {}), 1);
      backend.state.streams[0].send("caught_up", { last_event_id: 1 });
      backend.state.streams[0].close();
    });
    await waitFor(() => expect(backend.state.streams).toHaveLength(2), { timeout: 3000 });
    expect(backend.calls.filter((c) => c.path.endsWith("/live")).map((c) => c.lastEventId)).toEqual([undefined, "1"]);
    // A replayed duplicate is not shown twice.
    act(() => backend.state.streams[1].send("event", timelineEvent(1, "session.started", {}), 1));
    act(() => backend.state.streams[1].send("event", timelineEvent(2, "prompt.submitted", { prompt_id: "p", prompt: "hint?" }), 2));
    expect(await screen.findByText("Asked the assistant: hint?")).toBeTruthy();
    expect(screen.getAllByText("Candidate started the session")).toHaveLength(1);
  });

  it("stops with a message when live monitoring is off", async () => {
    const backend = fakeBackend();
    backend.state.fail.set("GET /interviews/iv-1/live", () =>
      new Response(JSON.stringify({ code: "live_monitoring_disabled", message: "live monitoring is disabled for this company" }), {
        status: 403,
      }),
    );
    renderApp(backend, "#/interviews/iv-1/live");
    expect((await screen.findByRole("alert")).textContent).toBe("Not allowed: live monitoring is disabled for this company");
    expect(screen.getByRole("status", { name: "Connection" }).textContent).toBe("Stopped");
  });
});

describe("replay", () => {
  function withTimeline() {
    const backend = fakeBackend();
    backend.state.timeline = [
      timelineEvent(1, "session.started", {}),
      timelineEvent(2, "code.diff", { path: "main.py", origin: "manual", patch: PATCH_1 }),
      timelineEvent(3, "prompt.submitted", { prompt_id: "p1", prompt: "simpler?" }),
      timelineEvent(4, "ai.response.completed", { prompt_id: "p1", status: "completed", response_text: "Use 6 * 7." }),
      timelineEvent(5, "code.diff", { path: "main.py", origin: "ai_applied", patch: PATCH_2 }),
    ];
    return backend;
  }

  it("opens at the end and scrubs back through the session", async () => {
    renderApp(withTimeline(), "#/interviews/iv-1/replay");
    expect((await screen.findByTestId("position")).textContent).toBe("Event 5 of 5");
    expect(screen.getByTestId("code").textContent).toContain("return 6 * 7");
    expect(screen.getByTestId("prompt").textContent).toContain("Use 6 * 7.");

    fireEvent.change(screen.getByLabelText("Replay position"), { target: { value: "1" } });
    expect(screen.getByTestId("position").textContent).toBe("Event 2 of 5");
    expect(screen.getByTestId("code").textContent).toContain("return 42");
    expect(screen.queryByTestId("prompt")).toBeNull();
  });

  it("seeks from the timeline and plays to the end", async () => {
    const { user } = renderApp(withTimeline(), "#/interviews/iv-1/replay");
    await user.click(await screen.findByRole("button", { name: "Candidate started the session" }));
    expect(screen.getByTestId("position").textContent).toBe("Event 1 of 5");
    expect(screen.getByTestId("code").textContent).toContain("pass");
    await user.click(screen.getByRole("button", { name: "Play" }));
    await waitFor(() => expect(screen.getByTestId("position").textContent).toBe("Event 5 of 5"));
    await waitFor(() => expect(screen.getByRole("button", { name: "Play" })).toBeTruthy());
  });

  it("explains an empty or unavailable replay", async () => {
    renderApp(fakeBackend(), "#/interviews/iv-1/replay");
    expect(await screen.findByText("Nothing recorded for this interview yet.")).toBeTruthy();
    const backend = fakeBackend();
    backend.state.fail.set("GET /interviews/iv-1/timeline", () =>
      new Response(JSON.stringify({ code: "replay_disabled", message: "replay is disabled for this company" }), { status: 403 }),
    );
    renderApp(backend, "#/interviews/iv-1/replay");
    expect((await screen.findByRole("alert")).textContent).toBe("Not allowed: replay is disabled for this company");
  });
});

describe("settings", () => {
  it("toggles a policy", async () => {
    const { backend, user } = renderApp(fakeBackend(), "#/settings");
    const toggle = (await screen.findByLabelText("Live monitoring")) as HTMLInputElement;
    expect(toggle.checked).toBe(true);
    await user.click(toggle);
    await waitFor(() => expect((screen.getByLabelText("Live monitoring") as HTMLInputElement).checked).toBe(false));
    expect(backend.calls.find((c) => c.method === "PUT")).toMatchObject({ path: "/policies/live_monitoring", body: { enabled: false } });
    expect(screen.getByText("Acme")).toBeTruthy();
  });
});
