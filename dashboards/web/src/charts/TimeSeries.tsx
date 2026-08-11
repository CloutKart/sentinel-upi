/* Daily flow over the simulation window.
 *
 * Two measures — rupee volume and flagged share — on deliberately separate stacked
 * charts sharing one x-axis, never a dual y-axis. Two y-scales on one frame let the
 * author place the crossings wherever they like, which is why the form is banned
 * outright rather than used carefully.
 *
 * Each panel is a single series, so neither carries a legend; the panel title names
 * the measure. A crosshair reads both panels at the same date.
 */

import { scaleLinear, scalePoint } from "d3-scale";
import { useState } from "react";
import { shortDate } from "../data";

export interface SeriesPanel {
  label: string;
  color: string;
  values: number[];
  format: (value: number) => string;
  /** Force the y-domain to start at zero. Areas must; rates need not. */
  zeroBased?: boolean;
}

const WIDTH = 640;
const PANEL_H = 116;
const PAD_L = 62;
const PAD_R = 18;
const PAD_B = 26;
// Headroom above each panel for its subtitle. Without it the first subtitle sits at a
// negative y and is clipped by the viewBox.
const PAD_T = 18;

export function TimeSeries({
  dates,
  panels,
}: {
  dates: string[];
  panels: SeriesPanel[];
}) {
  const [active, setActive] = useState<number | null>(null);

  const x = scalePoint<string>()
    .domain(dates)
    .range([PAD_L, WIDTH - PAD_R])
    .padding(0.5);

  const height = panels.length * (PANEL_H + PAD_B + PAD_T) + 8;

  return (
    <div className="chart-wrap">
      <svg
        viewBox={`0 0 ${WIDTH} ${height}`}
        className="chart"
        role="img"
        onMouseLeave={() => setActive(null)}
      >
        {panels.map((panel, p) => {
          const top = p * (PANEL_H + PAD_B + PAD_T) + PAD_T;
          const bottom = top + PANEL_H;
          const max = Math.max(...panel.values);
          const min = panel.zeroBased === false ? Math.min(...panel.values) : 0;
          // A flat series would collapse to a zero-height band; pad it so the line
          // still reads as a line rather than as the axis.
          const span = max - min || max || 1;
          const y = scaleLinear()
            .domain([min - span * 0.08, max + span * 0.12])
            .range([bottom, top]);

          const points = panel.values.map((value, i) => [x(dates[i]) ?? 0, y(value)] as const);
          const line = points.map(([px, py], i) => `${i ? "L" : "M"}${px},${py}`).join("");
          const area = `${line}L${points[points.length - 1][0]},${bottom}L${points[0][0]},${bottom}Z`;

          return (
            <g key={panel.label}>
              <text className="panel-subtitle" x={PAD_L} y={top - 6}>
                {panel.label}
              </text>

              <line className="grid-line" x1={PAD_L} x2={WIDTH - PAD_R} y1={bottom} y2={bottom} />
              <text className="axis-tick" x={PAD_L - 8} y={top + 10} textAnchor="end">
                {panel.format(max)}
              </text>

              <path d={area} fill={panel.color} opacity={0.12} />
              <path d={line} fill="none" stroke={panel.color} strokeWidth={2} />

              {points.map(([px, py], i) => (
                <circle
                  key={i}
                  cx={px}
                  cy={py}
                  r={active === i ? 5 : 3.5}
                  fill={panel.color}
                  stroke="var(--surface-1)"
                  strokeWidth={2}
                />
              ))}

              {active !== null ? (
                <text
                  className="bar-value"
                  x={(x(dates[active]) ?? 0) + 8}
                  y={y(panel.values[active]) - 8}
                >
                  {panel.format(panel.values[active])}
                </text>
              ) : null}

              {p === panels.length - 1
                ? dates.map((date) => (
                    <text
                      key={date}
                      className="axis-tick"
                      x={x(date) ?? 0}
                      y={bottom + 16}
                      textAnchor="middle"
                    >
                      {shortDate(date)}
                    </text>
                  ))
                : null}
            </g>
          );
        })}

        {/* Crosshair spanning both panels, so one hover reads the whole day. */}
        {active !== null ? (
          <line
            className="crosshair"
            x1={x(dates[active]) ?? 0}
            x2={x(dates[active]) ?? 0}
            y1={0}
            y2={height - PAD_B + 6}
          />
        ) : null}

        {dates.map((date, i) => {
          const px = x(date) ?? 0;
          const band = (WIDTH - PAD_L - PAD_R) / Math.max(dates.length, 1);
          return (
            <rect
              key={`hit-${date}`}
              x={px - band / 2}
              y={0}
              width={band}
              height={height}
              fill="transparent"
              onMouseEnter={() => setActive(i)}
            />
          );
        })}
      </svg>
    </div>
  );
}
