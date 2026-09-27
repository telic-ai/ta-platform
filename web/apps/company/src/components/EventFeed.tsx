import { describeEvent, type TimelineEvent } from "@ta-platform/api-client";

interface Props {
  events: readonly TimelineEvent[];
  /** Highlights this sequence number; events after it are dimmed. */
  position?: number;
  onSelect?: (seq: number) => void;
}

export function EventFeed({ events, position, onSelect }: Props) {
  if (events.length === 0) return <p className="muted">No activity yet.</p>;
  return (
    <ol className="feed" aria-label="Events">
      {events.map((event) => {
        const seq = event.sequence_number;
        const state = position === undefined ? "" : seq === position ? "current" : seq > position ? "future" : "";
        return (
          <li key={event.event_id} className={state} aria-current={state === "current" ? "step" : undefined}>
            <span className="seq">#{seq}</span>
            <time dateTime={event.occurred_at}>{new Date(event.occurred_at).toLocaleTimeString()}</time>
            {onSelect ? (
              <button className="link" onClick={() => onSelect(seq)}>
                {describeEvent(event)}
              </button>
            ) : (
              <span>{describeEvent(event)}</span>
            )}
          </li>
        );
      })}
    </ol>
  );
}
