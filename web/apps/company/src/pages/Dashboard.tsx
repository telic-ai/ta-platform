import { useState } from "react";
import type { DailyCount } from "../dailyChart";
import { DailyChart, fillDays } from "../dailyChart";
import { useLoad } from "../api";
import { href } from "../route";

const TILES: { label: string; eventType: string }[] = [
  { label: "Sessions started", eventType: "session.started" },
  { label: "Prompts", eventType: "prompt.submitted" },
  { label: "Code runs", eventType: "execution.requested" },
  { label: "Edits", eventType: "code.diff" },
];

export function Dashboard() {
  const [days, setDays] = useState(14);
  const overview = useLoad((c) => c.dashboardOverview(days), [days]);
  const activity = useLoad(async (c) => {
    const [rows, interviews] = await Promise.all([c.dashboardInterviews(), c.listInterviews({ limit: 200 })]);
    const names = new Map(interviews.map((i) => [i.id, i.candidate_name]));
    return rows.map((row) => ({ ...row, candidate: names.get(row.interview_id) }));
  }, []);

  return (
    <div className="page">
      <div className="page-head">
        <h1>Dashboard</h1>
        <label className="inline">
          Range
          <select value={days} onChange={(e) => setDays(Number(e.target.value))}>
            <option value={7}>Last 7 days</option>
            <option value={14}>Last 14 days</option>
            <option value={30}>Last 30 days</option>
            <option value={90}>Last 90 days</option>
          </select>
        </label>
      </div>
      {overview.error && <p role="alert" className="error">{overview.error}</p>}
      {overview.data && (
        <>
          <div className="tiles">
            {TILES.map((tile) => (
              <div key={tile.eventType} className="tile">
                <span className="tile-label">{tile.label}</span>
                <span className="tile-value" data-testid={`tile-${tile.eventType}`}>
                  {(overview.data!.totals[tile.eventType] ?? 0).toLocaleString()}
                </span>
              </div>
            ))}
          </div>
          <section className="card">
            <h2>Candidate activity per day</h2>
            <DailyChart days={fillDays(overview.data.daily as DailyCount[], overview.data.since, overview.data.days)} />
          </section>
        </>
      )}
      <section className="card">
        <h2>Interviews by activity</h2>
        {activity.error && <p role="alert" className="error">{activity.error}</p>}
        {activity.data && activity.data.length === 0 && <p className="muted">No candidate activity yet.</p>}
        {activity.data && activity.data.length > 0 && (
          <table>
            <thead>
              <tr>
                <th>Candidate</th>
                <th className="num">Prompts</th>
                <th className="num">Runs (passed)</th>
                <th className="num">Edits (AI)</th>
                <th className="num">Lines +/−</th>
                <th>Last active</th>
                <th />
              </tr>
            </thead>
            <tbody>
              {activity.data.map((row) => (
                <tr key={row.interview_id}>
                  <td>{row.candidate ?? row.interview_id.slice(0, 8)}</td>
                  <td className="num">{row.prompts}</td>
                  <td className="num">
                    {row.runs} ({row.runs_succeeded})
                  </td>
                  <td className="num">
                    {row.diffs} ({row.ai_applied_diffs})
                  </td>
                  <td className="num">
                    +{row.lines_added} −{row.lines_removed}
                  </td>
                  <td>{new Date(row.last_at).toLocaleString()}</td>
                  <td>
                    <a href={href({ page: "replay", interviewId: row.interview_id })}>Replay</a>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </section>
    </div>
  );
}
