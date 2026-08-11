/* The medallion funnel: how many rows entered each layer, and what left in between.
 *
 * A plain four-bar chart would show the stages but hide the interesting part, which is
 * the *gap* — 7,889 rows do not reach Silver, and a reader should be able to see both
 * that it happened and why.
 *
 * The losses are drawn as their own indented rows between the two stages they explain,
 * on the same x-scale as the bars. They are deliberately not hung off the right-hand
 * end of the bars: the drop from Bronze to Silver is under 4% of the axis, so a bar-end
 * annotation is a two-pixel line with a label that runs off the edge of the chart.
 *
 * Stage bars use the ordinal blue ramp, not categorical hues: the stages are one
 * ordered sequence, not four unrelated identities. Losses are drawn in the critical
 * status colour and always carry their label.
 */

import { scaleLinear } from "d3-scale";
import { count } from "../data";
import type { Funnel as FunnelData } from "../data";

const WIDTH = 620;
const STAGE_ROW = 46;
const LOSS_ROW = 17;
const BAR = 28;
const LOSS_BAR = 7;
const LABEL_W = 100;
const PAD_R = 92;
const INDENT = 14;

const STAGE_FILL = ["var(--seq-250)", "var(--seq-350)", "var(--seq-450)", "var(--seq-550)"];

interface Loss {
  label: string;
  rows: number;
}

export function Funnel({ data }: { data: FunnelData }) {
  const stages = data.stages;
  const max = Math.max(...stages.map((s) => s.rows), 1);

  const x = scaleLinear()
    .domain([0, max])
    .range([LABEL_W, WIDTH - PAD_R]);

  // A loss is attributed to the gap below the stage it left from, so it renders
  // between the two bars it explains.
  const lossesAfter = (index: number): Loss[] =>
    index === 1
      ? [
          { label: "quarantined, with a reason", rows: data.quarantined },
          { label: "deduplicated retries", rows: data.deduplicated },
        ].filter((loss) => loss.rows > 0)
      : [];

  // Lay the rows out first so the SVG height is whatever the content needs.
  let cursor = 8;
  const layout = stages.map((stage, i) => {
    const y = cursor;
    cursor += STAGE_ROW;
    const losses = lossesAfter(i).map((loss) => {
      const lossY = cursor;
      cursor += LOSS_ROW;
      return { ...loss, y: lossY };
    });
    return { stage, y, losses };
  });
  const height = cursor + 4;

  return (
    <svg viewBox={`0 0 ${WIDTH} ${height}`} className="chart" role="img">
      {layout.map(({ stage, y, losses }, i) => {
        const w = Math.max(x(stage.rows) - LABEL_W, 2);
        const isLast = i === layout.length - 1;

        return (
          <g key={stage.stage}>
            <text className="axis-label" x={LABEL_W - 10} y={y + BAR / 2 + 4} textAnchor="end">
              {stage.label}
            </text>

            <rect x={LABEL_W} y={y} width={w} height={BAR} rx={4} fill={STAGE_FILL[i]} />

            <text className="bar-value" x={LABEL_W + w + 8} y={y + BAR / 2 + 4}>
              {count(stage.rows)}
            </text>

            {/* Connector down to whatever comes next — the next stage, or a loss. */}
            {!isLast ? (
              <line
                className="funnel-drop"
                x1={LABEL_W + 5}
                x2={LABEL_W + 5}
                y1={y + BAR}
                y2={losses.length ? losses[0].y + LOSS_BAR : y + STAGE_ROW}
              />
            ) : null}

            {losses.map((loss) => {
              const lossW = Math.max(x(loss.rows) - LABEL_W, 3);
              return (
                <g key={loss.label} className="funnel-branch">
                  <rect
                    x={LABEL_W + INDENT}
                    y={loss.y}
                    width={lossW}
                    height={LOSS_BAR}
                    rx={3}
                  />
                  <text x={LABEL_W + INDENT + lossW + 8} y={loss.y + LOSS_BAR}>
                    {count(loss.rows)} {loss.label}
                  </text>
                </g>
              );
            })}
          </g>
        );
      })}
    </svg>
  );
}
