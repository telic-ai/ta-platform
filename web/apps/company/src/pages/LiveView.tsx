import { useEffect, useMemo, useState } from "react";
import { replayState, starterFiles, type LiveStatus, type TimelineEvent } from "@ta-platform/api-client";
import { errorMessage, useClient, useLoad } from "../api";
import { Activity } from "../components/Activity";
import { CodeView } from "../components/CodeView";
import { EventFeed } from "../components/EventFeed";
import { href } from "../route";

const STATUS_LABEL: Record<LiveStatus, string> = { connecting: "Connecting…", live: "Live", reconnecting: "Reconnecting…" };

/** Follows an interview as it happens. */
export function LiveView({ interviewId }: { interviewId: string }) {
  const client = useClient();
  const interview = useLoad((c) => c.getInterview(interviewId), [interviewId]);
  const [events, setEvents] = useState<TimelineEvent[]>([]);
  const [status, setStatus] = useState<LiveStatus>("connecting");
  const [error, setError] = useState<string>();

  useEffect(() => {
    const controller = new AbortController();
    client
      .watchLive(interviewId, {
        signal: controller.signal,
        onEvent: (event) => setEvents((all) => [...all, event]),
        onStatus: setStatus,
      })
      .catch((e: unknown) => setError(errorMessage(e)));
    return () => controller.abort();
  }, [client, interviewId]);

  const state = useMemo(() => replayState(events, starterFiles()), [events]);
  const newestFirst = useMemo(() => [...events].reverse(), [events]);

  return (
    <div className="page">
      <div className="page-head">
        <h1>
          {interview.data?.candidate_name ?? "Interview"} <span className="muted">· live</span>
        </h1>
        <span className={`pill ${status === "live" ? "good" : ""}`} role="status" aria-label="Connection">
          {error ? "Stopped" : STATUS_LABEL[status]}
        </span>
        <a href={href({ page: "replay", interviewId })}>Open replay</a>
      </div>
      {error && <p role="alert" className="error">{error}</p>}
      <div className="split">
        <div>
          <CodeView files={state.files} active={state.activeFile} />
          <Activity state={state} />
        </div>
        <aside className="card">
          <h2>Activity</h2>
          <EventFeed events={newestFirst} />
        </aside>
      </div>
    </div>
  );
}
