import { useEffect, useRef, useState } from "react";

export interface DailyCount {
  day: string;
  event_type: string;
  events: number;
}

export interface DayTotal {
  day: string;
  events: number;
}

/** Sums each day's counts and fills days without events with zero. */
export function fillDays(daily: readonly DailyCount[], since: string, days: number): DayTotal[] {
  const totals = new Map<string, number>();
  for (const row of daily) totals.set(row.day, (totals.get(row.day) ?? 0) + row.events);
  const start = new Date(`${since}T00:00:00Z`);
  return Array.from({ length: days }, (_, i) => {
    const day = new Date(start.getTime() + i * 86_400_000).toISOString().slice(0, 10);
    return { day, events: totals.get(day) ?? 0 };
  });
}

const DEFAULT_W = 720;
const H = 200;

/** The element's width in CSS pixels, so the chart draws at 1:1 and text stays its real size. */
function useWidth<T extends HTMLElement>(): [React.RefObject<T | null>, number] {
  const ref = useRef<T>(null);
  const [width, setWidth] = useState(DEFAULT_W);
  useEffect(() => {
    const el = ref.current;
    if (!el || typeof ResizeObserver === "undefined") return;
    const observer = new ResizeObserver(([entry]) => {
      const next = Math.round(entry.contentRect.width);
      if (next > 0) setWidth(next);
    });
    observer.observe(el);
    return () => observer.disconnect();
  }, []);
  return [ref, width];
}
const PAD = { top: 12, right: 8, bottom: 24, left: 40 };

/** A single-series bar chart of events per day, with hover tooltips and a table view. */
export function DailyChart({ days }: { days: DayTotal[] }) {
  const [hover, setHover] = useState<number>();
  const [ref, W] = useWidth<HTMLElement>();
  const max = Math.max(1, ...days.map((d) => d.events));
  const ticks = niceTicks(max);
  const top = ticks.at(-1)!;
  const plotW = W - PAD.left - PAD.right;
  const plotH = H - PAD.top - PAD.bottom;
  const slot = plotW / days.length;
  const barW = Math.max(2, Math.min(28, slot - 2));
  const y = (v: number) => PAD.top + plotH - (v / top) * plotH;
  // Enough room for each "MM-DD" label (~48px).
  const labelEvery = Math.max(1, Math.ceil(days.length / Math.max(1, Math.floor(plotW / 48))));

  return (
    <figure className="viz-root" ref={ref}>
      <svg viewBox={`0 0 ${W} ${H}`} width={W} height={H} role="img" aria-label={`Events per day, ${days[0]?.day} to ${days.at(-1)?.day}`}>
        {ticks.map((t) => (
          <g key={t}>
            <line x1={PAD.left} x2={W - PAD.right} y1={y(t)} y2={y(t)} className="grid" />
            <text x={PAD.left - 6} y={y(t)} className="axis" textAnchor="end" dominantBaseline="middle">
              {t}
            </text>
          </g>
        ))}
        {days.map((d, i) => {
          const x = PAD.left + i * slot + (slot - barW) / 2;
          const h = y(0) - y(d.events);
          return (
            <g key={d.day} onMouseEnter={() => setHover(i)} onMouseLeave={() => setHover(undefined)}>
              {/* Hit target spans the whole slot, bigger than the mark. */}
              <rect x={PAD.left + i * slot} y={PAD.top} width={slot} height={plotH} fill="transparent" />
              {d.events > 0 && <path d={barPath(x, y(d.events), barW, h)} className={`bar${hover === i ? " hover" : ""}`} />}
              {i % labelEvery === 0 && (
                <text x={x + barW / 2} y={H - 6} className="axis" textAnchor="middle">
                  {d.day.slice(5)}
                </text>
              )}
            </g>
          );
        })}
      </svg>
      {hover !== undefined && (
        <div
          className="tooltip"
          role="status"
          style={{ left: `${((PAD.left + hover * slot + slot / 2) / W) * 100}%` }}
        >
          <strong>{days[hover].day}</strong>
          <span>{days[hover].events.toLocaleString()} events</span>
        </div>
      )}
      <details>
        <summary>Table view</summary>
        <table>
          <thead>
            <tr>
              <th>Day</th>
              <th className="num">Events</th>
            </tr>
          </thead>
          <tbody>
            {days.map((d) => (
              <tr key={d.day}>
                <td>{d.day}</td>
                <td className="num">{d.events}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </details>
    </figure>
  );
}

/** A bar with 4px-rounded top corners, square at the baseline. */
function barPath(x: number, top: number, w: number, h: number): string {
  const r = Math.min(4, w / 2, h);
  const bottom = top + h;
  return `M${x},${bottom}V${top + r}Q${x},${top} ${x + r},${top}H${x + w - r}Q${x + w},${top} ${x + w},${top + r}V${bottom}Z`;
}

/** Round axis ticks from 0 covering max. */
export function niceTicks(max: number): number[] {
  const raw = max / 4;
  const mag = 10 ** Math.floor(Math.log10(raw));
  const step = [1, 2, 5, 10].map((m) => m * mag).find((s) => s >= raw) ?? 10 * mag;
  const unit = Math.max(1, step);
  const ticks = [];
  for (let t = 0; t <= max + unit - 1e-9; t += unit) ticks.push(Math.round(t));
  if (ticks.at(-1)! < max) ticks.push(ticks.at(-1)! + unit);
  return ticks;
}
