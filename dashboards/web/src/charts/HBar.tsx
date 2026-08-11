/* Horizontal bars for magnitude across a handful of named categories.
 *
 * Horizontal rather than vertical because every category here has a real name —
 * "Amount ≤ 0", "high_amount + shared_ip" — and vertical bars would either rotate
 * those labels or truncate them.
 *
 * One series, so no legend: the title names the measure. Colour is a single hue by
 * default and carries no information; where the caller passes per-row colours the row
 * label is already sitting beside the bar, so identity never rests on colour alone.
 */

import { scaleLinear } from "d3-scale";
import { useState } from "react";
import { Tooltip } from "./chrome";

export interface BarDatum {
  key: string;
  label: string;
  value: number;
  color?: string;
  /** Optional second line under the label. */
  detail?: string;
}

export function HBar({
  data,
  format,
  labelWidth = 150,
  width = 620,
  rowHeight = 30,
  tooltip,
}: {
  data: BarDatum[];
  format: (value: number) => string;
  labelWidth?: number;
  width?: number;
  rowHeight?: number;
  tooltip?: (datum: BarDatum) => React.ReactNode;
}) {
  const [hover, setHover] = useState<{ datum: BarDatum; x: number; y: number } | null>(null);

  const max = Math.max(...data.map((d) => d.value), 1);
  const height = data.length * rowHeight + 12;
  const padRight = 78;
  const x = scaleLinear().domain([0, max]).range([labelWidth, width - padRight]);
  const bar = Math.min(rowHeight - 12, 20);

  return (
    <div className="chart-wrap">
      <svg viewBox={`0 0 ${width} ${height}`} className="chart" role="img">
        {data.map((datum, i) => {
          const y = i * rowHeight + 6;
          const w = Math.max(x(datum.value) - labelWidth, 2);
          return (
            <g
              key={datum.key}
              onMouseEnter={(event) =>
                tooltip &&
                setHover({
                  datum,
                  x: event.nativeEvent.offsetX,
                  y: event.nativeEvent.offsetY,
                })
              }
              onMouseLeave={() => setHover(null)}
            >
              {/* A full-width hit target: the bar itself can be 2px wide. */}
              <rect x={0} y={y - 3} width={width} height={bar + 6} fill="transparent" />
              <text className="axis-label" x={labelWidth - 10} y={y + bar / 2 + 4} textAnchor="end">
                {datum.label}
              </text>
              <rect
                x={labelWidth}
                y={y}
                width={w}
                height={bar}
                rx={4}
                fill={datum.color ?? "var(--seq-450)"}
              />
              <text className="bar-value" x={labelWidth + w + 8} y={y + bar / 2 + 4}>
                {format(datum.value)}
              </text>
            </g>
          );
        })}
      </svg>
      {hover && tooltip ? (
        <Tooltip x={hover.x} y={hover.y}>
          {tooltip(hover.datum)}
        </Tooltip>
      ) : null}
    </div>
  );
}
