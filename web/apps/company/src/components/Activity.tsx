import type { ReplayState } from "@ta-platform/api-client";

/** The prompts and runs a replay or live view has reached. */
export function Activity({ state }: { state: ReplayState }) {
  const lastRun = state.runs.at(-1);
  const lastPrompt = state.prompts.at(-1);
  return (
    <div className="activity">
      <section>
        <h3>Latest run</h3>
        {lastRun ? (
          <div data-testid="run">
            <p>
              <span className={`pill ${lastRun.status === "succeeded" ? "good" : lastRun.status ? "bad" : ""}`}>
                {lastRun.status ?? "running…"}
              </span>{" "}
              {lastRun.exitCode !== undefined && <span className="muted">exit {lastRun.exitCode}</span>}
            </p>
            {lastRun.stdout && <pre className="output">{lastRun.stdout}</pre>}
            {lastRun.stderr && <pre className="output stderr">{lastRun.stderr}</pre>}
          </div>
        ) : (
          <p className="muted">No runs yet.</p>
        )}
      </section>
      <section>
        <h3>Latest prompt</h3>
        {lastPrompt ? (
          <div data-testid="prompt">
            <p className="prompt">{lastPrompt.prompt}</p>
            {lastPrompt.response !== undefined ? (
              <pre className="output">{lastPrompt.response || `(${lastPrompt.status})`}</pre>
            ) : (
              <p className="muted">Waiting for the assistant…</p>
            )}
          </div>
        ) : (
          <p className="muted">No prompts yet.</p>
        )}
      </section>
      <p className="muted">
        {state.diffs} edits ({state.aiAppliedDiffs} from AI suggestions) · {state.prompts.length} prompts · {state.runs.length} runs
        {state.failedPatches.length > 0 && ` · ${state.failedPatches.length} edits could not be reconstructed`}
      </p>
    </div>
  );
}
