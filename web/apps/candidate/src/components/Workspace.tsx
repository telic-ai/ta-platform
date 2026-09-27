import { useEffect, useMemo, useState } from "react";
import { DiffDebouncer, WorkspaceError, type RunResult, type WorkspaceClient } from "@ta-platform/api-client";
import { LANGUAGES, type Language } from "../code";
import { Assistant } from "./Assistant";
import { RunPanel } from "./RunPanel";

interface Props {
  client: WorkspaceClient;
  /** Idle time before an edit is posted as a diff. */
  diffDelayMs?: number;
}

export function Workspace({ client, diffDelayMs = 750 }: Props) {
  const [language, setLanguage] = useState<Language>("python");
  const file = LANGUAGES[language].file;
  const [code, setCode] = useState<string>(LANGUAGES.python.starter);
  const [running, setRunning] = useState(false);
  const [result, setResult] = useState<RunResult>();
  const [runError, setRunError] = useState<string>();
  const diffs = useMemo(() => new DiffDebouncer({ client, delayMs: diffDelayMs }), [client, diffDelayMs]);

  useEffect(() => {
    diffs.track(LANGUAGES.python.file, LANGUAGES.python.starter);
    diffs.track(LANGUAGES.javascript.file, LANGUAGES.javascript.starter);
    return () => {
      void diffs.flush();
      diffs.dispose();
    };
  }, [diffs]);

  function edit(next: string) {
    setCode(next);
    diffs.edit(file, next);
  }

  function switchLanguage(next: Language) {
    void diffs.flush();
    setLanguage(next);
    setCode(LANGUAGES[next].starter);
    diffs.track(LANGUAGES[next].file, LANGUAGES[next].starter);
    setResult(undefined);
  }

  function applyAI(next: string, promptId: string) {
    setCode(next);
    void diffs.applyAI(file, next, promptId);
  }

  async function run() {
    setRunning(true);
    setRunError(undefined);
    try {
      // Post pending edits first so the event log shows the code that ran.
      await diffs.flush();
      setResult(await client.runCode({ language, entrypoint: file, files: { [file]: code } }));
    } catch (err) {
      setRunError(
        err instanceof WorkspaceError && err.status === 429
          ? "A run is already in progress. Wait for it to finish."
          : "Could not run your code. Try again.",
      );
    } finally {
      setRunning(false);
    }
  }

  return (
    <main className="workspace">
      <section className="editor" aria-label="Editor">
        <header>
          <select aria-label="Language" value={language} onChange={(e) => switchLanguage(e.target.value as Language)}>
            {Object.entries(LANGUAGES).map(([key, value]) => (
              <option key={key} value={key}>
                {value.label}
              </option>
            ))}
          </select>
          <span className="file">{file}</span>
          <button type="button" onClick={run} disabled={running}>
            {running ? "Running…" : "Run"}
          </button>
        </header>
        <textarea aria-label="Code" value={code} onChange={(e) => edit(e.target.value)} spellCheck={false} />
        <RunPanel running={running} result={result} error={runError} />
      </section>
      <Assistant client={client} onApply={applyAI} />
    </main>
  );
}
