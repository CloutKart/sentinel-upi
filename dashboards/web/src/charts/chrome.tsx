/* Shared chart furniture: the figure wrapper with its table toggle, the tooltip, and
 * the axis primitives every chart here draws with.
 *
 * The table toggle is not a nicety. Three of the five categorical hues sit below 3:1
 * on the light surface, which under the dataviz contrast rule obliges either direct
 * labels or a table view. Every chart gets one, so the obligation is discharged in one
 * place rather than argued about per chart.
 */

import { useId, useState, type ReactNode } from "react";

export interface Column<T> {
  key: string;
  label: string;
  align?: "left" | "right";
  render: (row: T) => ReactNode;
}

/** A chart and its table alternative, sharing one caption and one toggle. */
export function Figure<T>({
  title,
  note,
  rows,
  columns,
  children,
  wide,
}: {
  title: string;
  note?: ReactNode;
  rows: T[];
  columns: Column<T>[];
  children: ReactNode;
  wide?: boolean;
}) {
  const [showTable, setShowTable] = useState(false);
  const id = useId();

  return (
    <figure className={wide ? "panel panel-wide" : "panel"}>
      <div className="panel-head">
        <figcaption className="panel-title" id={`${id}-title`}>
          {title}
        </figcaption>
        <button
          type="button"
          className="ghost-button"
          onClick={() => setShowTable((v) => !v)}
          aria-expanded={showTable}
          aria-controls={`${id}-table`}
        >
          {showTable ? "Chart" : "Table"}
        </button>
      </div>

      {showTable ? (
        <div className="table-scroll" id={`${id}-table`}>
          <table className="data-table">
            <thead>
              <tr>
                {columns.map((column) => (
                  <th key={column.key} className={column.align === "right" ? "num" : undefined}>
                    {column.label}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {rows.map((row, i) => (
                <tr key={i}>
                  {columns.map((column) => (
                    <td key={column.key} className={column.align === "right" ? "num" : undefined}>
                      {column.render(row)}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : (
        <div className="chart-body" aria-labelledby={`${id}-title`}>
          {children}
        </div>
      )}

      {note ? <p className="panel-note">{note}</p> : null}
    </figure>
  );
}

/** Hover tooltip. Positioned by the caller in chart coordinates. */
export function Tooltip({
  x,
  y,
  children,
}: {
  x: number;
  y: number;
  children: ReactNode;
}) {
  return (
    <div className="tooltip" style={{ left: x, top: y }} role="status">
      {children}
    </div>
  );
}

/** A legend. Present whenever two or more series share a chart. */
export function Legend({ items }: { items: { label: string; color: string }[] }) {
  return (
    <ul className="legend">
      {items.map((item) => (
        <li key={item.label}>
          <span className="dot" style={{ background: item.color }} aria-hidden="true" />
          {item.label}
        </li>
      ))}
    </ul>
  );
}

/** A labelled reference line. Used for the band thresholds and the two amount cut-offs. */
export function RefLine({
  x,
  top,
  bottom,
  label,
  emphasis,
}: {
  x: number;
  top: number;
  bottom: number;
  label: string;
  emphasis?: boolean;
}) {
  return (
    <g className={emphasis ? "refline is-emphasis" : "refline"}>
      <line x1={x} x2={x} y1={top} y2={bottom} />
      <text x={x} y={top - 6} textAnchor="middle">
        {label}
      </text>
    </g>
  );
}
