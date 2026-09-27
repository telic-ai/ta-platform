import type { RunResult } from "@ta-platform/api-client";

const STATUS_LABELS: Record<RunResult["status"], string> = {
  succeeded: "Succeeded",
  failed: "Failed",
  timed_out: "Timed out",
  oom_killed: "Out of memory",
  error: "Could not run",
};

interface Props {
  running: boolean;
  result?: RunResult;
  error?: string;
}

export function RunPanel({ running, result, error }: Props) {
  return (
    <section className="run" aria-label="Run output" aria-live="polite">
      {running && <p>Running…</p>}
      {error && <p role="alert">{error}</p>}
      {result && !running && (
        <>
          <p className={`status status-${result.status}`}>
            {STATUS_LABELS[result.status]}
            {result.status !== "error" && ` · exit ${result.exitCode} · ${result.durationMs} ms`}
          </p>
          <pre aria-label="stdout">{result.stdout}</pre>
          {result.stdoutTruncated && <p className="note">Output truncated.</p>}
          {result.stderr && <pre aria-label="stderr">{result.stderr}</pre>}
        </>
      )}
    </section>
  );
}
