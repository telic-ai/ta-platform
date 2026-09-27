import { useRef, useState, type FormEvent } from "react";
import type { ChatMessage, PromptDone, WorkspaceClient } from "@ta-platform/api-client";
import { firstCodeBlock } from "../code";

interface Turn {
  prompt: string;
  promptId?: string;
  answer: string;
  done?: PromptDone;
  error?: string;
}

interface Props {
  client: WorkspaceClient;
  /** Called when the candidate applies code from an answer. */
  onApply: (code: string, promptId: string) => void;
}

const STATUS_NOTES: Record<string, string> = {
  truncated: "The answer was cut off at its length limit.",
  refused: "The assistant declined to answer.",
  error: "The assistant failed to answer. Try again.",
  cancelled: "Stopped.",
};

export function Assistant({ client, onApply }: Props) {
  const [turns, setTurns] = useState<Turn[]>([]);
  const [prompt, setPrompt] = useState("");
  const [streaming, setStreaming] = useState(false);
  const abort = useRef<AbortController | null>(null);

  function update(index: number, change: (turn: Turn) => Turn) {
    setTurns((all) => all.map((turn, i) => (i === index ? change(turn) : turn)));
  }

  async function send(event: FormEvent) {
    event.preventDefault();
    const text = prompt.trim();
    if (!text || streaming) return;
    const history: ChatMessage[] = turns
      .filter((turn) => turn.done?.status === "completed" || turn.done?.status === "truncated")
      .flatMap((turn) => [
        { role: "user" as const, content: turn.prompt },
        ...(turn.answer ? [{ role: "assistant" as const, content: turn.answer }] : []),
      ]);
    const index = turns.length;
    setTurns((all) => [...all, { prompt: text, answer: "" }]);
    setPrompt("");
    setStreaming(true);
    const controller = new AbortController();
    abort.current = controller;
    try {
      const done = await client.submitPrompt(
        { prompt: text, history },
        {
          signal: controller.signal,
          onAccepted: (accepted) => update(index, (turn) => ({ ...turn, promptId: accepted.promptId })),
          onDelta: (delta) => update(index, (turn) => ({ ...turn, answer: turn.answer + delta })),
        },
      );
      update(index, (turn) => ({ ...turn, done }));
    } catch (err) {
      const cancelled = controller.signal.aborted;
      update(index, (turn) => ({
        ...turn,
        done: { status: cancelled ? "cancelled" : "error" },
        error: cancelled ? undefined : "Could not reach the assistant.",
      }));
    } finally {
      setStreaming(false);
      abort.current = null;
    }
  }

  return (
    <section className="assistant" aria-label="Assistant">
      <ol className="turns">
        {turns.map((turn, i) => {
          const code = firstCodeBlock(turn.answer);
          return (
            <li key={i}>
              <p className="prompt">{turn.prompt}</p>
              <pre className="answer" aria-label="answer">
                {turn.answer}
              </pre>
              {turn.done && STATUS_NOTES[turn.done.status] && <p className="note">{STATUS_NOTES[turn.done.status]}</p>}
              {turn.error && <p role="alert">{turn.error}</p>}
              {code && turn.promptId && turn.done && (
                <button type="button" onClick={() => onApply(code, turn.promptId!)}>
                  Apply code
                </button>
              )}
            </li>
          );
        })}
      </ol>
      <form onSubmit={send}>
        <textarea aria-label="Ask the assistant" value={prompt} onChange={(e) => setPrompt(e.target.value)} rows={3} />
        {streaming ? (
          <button type="button" onClick={() => abort.current?.abort()}>
            Stop
          </button>
        ) : (
          <button type="submit" disabled={prompt.trim() === ""}>
            Send
          </button>
        )}
      </form>
    </section>
  );
}
