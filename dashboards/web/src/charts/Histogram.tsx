/* Column histogram with labelled reference lines.
 *
 * Used twice, for the two distributions that carry an argument:
 *
 *  - fraud scores, with the MEDIUM and HIGH band cut-offs marked, so the bands are
 *    visibly *thresholds on a distribution* rather than three unrelated buckets;
 *  - transaction amounts on a log scale, with the contaminated 99.5th percentile and
 *    the robust Tukey fence marked side by side.
 *
 * Counts run over four orders of magnitude in both cases, so the y-scale is
 * square-root: linear hides every bucket outside the first, and log on a *count* axis
 * misleads about ratios. Sqrt keeps small bars visible while area still reads roughly
 * as magnitude. The axis is labelled with it, because an unlabelled non-linear axis is
 * a lie.
 */

import { scaleBand, scaleLinear } from "d3-scale";
import { useState } from "react";
import { Tooltip } from "./chrome";

export interface Bucket {
  bucket: number;
  transactions: number;
}

export interface Marker {
  value: number;
  label: string;
  emphasis?: boolean;
}

const WIDTH = 640;
const HEIGHT = 200;
const PAD_L = 52;
const PAD_R = 16;
const PAD_B = 34;
/** Room for one row of marker labels. A second marker gets its own row — see below. */
const LABEL_ROW = 12;

export function Histogram({
  data,
  markers = [],
  formatBucket,
  formatCount,
  barColor,
}: {
  data: Bucket[];
  markers?: Marker[];
  formatBucket: (bucket: number) => string;
  formatCount: (n: number) => string;
  /** Per-bucket colour, e.g. tinting buckets by the risk band they fall in. */
  barColor?: (bucket: number) => string;
}) {
  const [hover, setHover] = useState<{ datum: Bucket; x: number; y: number } | null>(null);

  // Markers get alternating label rows. Two thresholds can land in adjacent buckets —
  // the applied cut-off and the contaminated percentile do exactly that — and a single
  // row of labels then overprints itself.
  const rows = Math.min(markers.length, 2);
  const PAD_T = 14 + Math.max(rows, 1) * LABEL_ROW;

  const x = scaleBand<number>()
    .domain(data.map((d) => d.bucket))
    .range([PAD_L, WIDTH - PAD_R])
    .paddingInner(0.18);

  const max = Math.max(...data.map((d) => d.transactions), 1);
  // Square root, not linear: see the module docstring. scaleLinear over the roots
  // rather than a hand-rolled ratio so the plot area is respected in one place.
  const y = scaleLinear()
    .domain([0, Math.sqrt(max)])
    .range([HEIGHT - PAD_B, PAD_T]);
  const scaled = (n: number) => y(Math.sqrt(n));

  /** A marker's x position: the left edge of the bucket it falls inside. */
  const markerX = (value: number): number | null => {
    for (let i = data.length - 1; i >= 0; i--) {
      if (value >= data[i].bucket) {
        const left = x(data[i].bucket);
        if (left === undefined) return null;
        return left + x.bandwidth() / 2;
      }
    }
    return null;
  };

  return (
    <div className="chart-wrap">
      <svg viewBox={`0 0 ${WIDTH} ${HEIGHT}`} className="chart" role="img">
        <line
          className="grid-line"
          x1={PAD_L}
          x2={WIDTH - PAD_R}
          y1={HEIGHT - PAD_B}
          y2={HEIGHT - PAD_B}
        />
        <text className="axis-tick" x={PAD_L - 8} y={PAD_T + 4} textAnchor="end">
          {formatCount(max)}
        </text>
        <text className="axis-caption" x={4} y={PAD_T - 12}>
          transactions (√ scale)
        </text>

        {data.map((datum) => {
          const left = x(datum.bucket);
          if (left === undefined) return null;
          const top = scaled(datum.transactions);
          return (
            <g
              key={datum.bucket}
              onMouseEnter={(event) =>
                setHover({
                  datum,
                  x: event.nativeEvent.offsetX,
                  y: event.nativeEvent.offsetY,
                })
              }
              onMouseLeave={() => setHover(null)}
            >
              <rect x={left} y={PAD_T} width={x.bandwidth()} height={HEIGHT - PAD_B - PAD_T} fill="transparent" />
              <rect
                x={left}
                y={top}
                width={x.bandwidth()}
                height={Math.max(HEIGHT - PAD_B - top, 1)}
                rx={3}
                fill={barColor ? barColor(datum.bucket) : "var(--seq-450)"}
              />
              <text
                className="axis-tick"
                x={left + x.bandwidth() / 2}
                y={HEIGHT - PAD_B + 15}
                textAnchor="middle"
              >
                {formatBucket(datum.bucket)}
              </text>
            </g>
          );
        })}

        {markers.map((marker, i) => {
          const mx = markerX(marker.value);
          if (mx === null) return null;

          // Stagger onto alternating rows, and pull the text inside the frame when the
          // marker sits near an edge — a centred label at the last bucket runs off.
          const labelY = PAD_T - 6 - (i % 2) * LABEL_ROW;
          const anchor = mx > WIDTH - 110 ? "end" : mx < PAD_L + 50 ? "start" : "middle";
          const textX = anchor === "end" ? mx - 4 : anchor === "start" ? mx + 4 : mx;

          return (
            <g key={marker.label} className={marker.emphasis ? "refline is-emphasis" : "refline"}>
              <line x1={mx} x2={mx} y1={labelY + 4} y2={HEIGHT - PAD_B} />
              <text x={textX} y={labelY} textAnchor={anchor}>
                {marker.label}
              </text>
            </g>
          );
        })}
      </svg>

      {hover ? (
        <Tooltip x={hover.x} y={hover.y}>
          <strong>{formatBucket(hover.datum.bucket)}</strong>
          <br />
          {formatCount(hover.datum.transactions)} transactions
        </Tooltip>
      ) : null}
    </div>
  );
}
