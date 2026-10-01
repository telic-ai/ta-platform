import { useEffect, useMemo, useState } from "react";
import { replayState, starterFiles } from "@ta-platform/api-client";
import { useLoad } from "../api";
import { Activity } from "../components/Activity";
import { CodeView } from "../components/CodeView";
import { EventFeed } from "../components/EventFeed";
import { href } from "../route";

/** Steps through a recorded interview. */
export function Replay({ interviewId, playStepMs = 700 }: { interviewId: string; playStepMs?: number }) {
  const interview = useLoad((c) => c.getInterview(interviewId), [interviewId]);
  const timeline = useLoad((c) => c.timeline(interviewId), [interviewId]);
  const events = useMemo(() => timeline.data ?? [], [timeline.data]);
  const seqs = useMemo(() => events.map((e) => e.sequence_number), [events]);
  const [index, setIndex] = useState<number>();
  const [playing, setPlaying] = useState(false);

  const last = seqs.length - 1;
  const current = index ?? last;
  const position = seqs[current] ?? 0;
  const state = useMemo(() => replayState(events, starterFiles(), position), [events, position]);

  useEffect(() => {
    if (!playing) return;
    if (current >= last) {
      setPlaying(false);
      return;
    }
    const timer = setTimeout(() => setIndex(current + 1), playStepMs);
    return () => clearTimeout(timer);
  }, [playing, current, last, playStepMs]);

  return (
    <div className="page">
      <div className="page-head">
        <h1>
          {interview.data?.candidate_name ?? "Interview"} <span className="muted">· replay</span>
        </h1>
        <a href={href({ page: "live", interviewId })}>Watch live</a>
      </div>
      {timeline.error && <p role="alert" className="error">{timeline.error}</p>}
      {timeline.data && events.length === 0 && <p className="muted">Nothing recorded for this interview yet.</p>}
      {events.length > 0 && (
        <>
          <div className="card row scrubber">
            <button
              onClick={() => {
                if (current >= last) setIndex(0);
                setPlaying((p) => !p);
              }}
            >
              {playing ? "Pause" : "Play"}
            </button>
            <input
              type="range"
              min={0}
              max={last}
              value={current}
              aria-label="Replay position"
              onChange={(e) => {
                setPlaying(false);
                setIndex(Number(e.target.value));
              }}
            />
            <span className="muted" data-testid="position">
              Event {current + 1} of {events.length}
            </span>
          </div>
          <div className="split">
            <div>
              <CodeView files={state.files} active={state.activeFile} />
              <Activity state={state} />
            </div>
            <aside className="card">
              <h2>Timeline</h2>
              <EventFeed
                events={events}
                position={position}
                onSelect={(seq) => {
                  setPlaying(false);
                  setIndex(seqs.indexOf(seq));
                }}
              />
            </aside>
          </div>
        </>
      )}
    </div>
  );
}
