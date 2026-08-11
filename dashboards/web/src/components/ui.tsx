/* Layout and text primitives. Nothing here draws data — charts live in ../charts. */

import type { ReactNode } from "react";
import { BAND_COLOR, ruleColor, ruleLabel, type RiskBand } from "../data";

export interface NavItem {
  id: string;
  label: string;
  hint: string;
}

export function Rail({
  items,
  active,
}: {
  items: NavItem[];
  active: string;
}) {
  return (
    <nav className="rail" aria-label="Sections">
      <ol>
        {items.map((item) => (
          <li key={item.id}>
            <a
              href={`#${item.id}`}
              className={item.id === active ? "is-active" : undefined}
              aria-current={item.id === active ? "true" : undefined}
            >
              <span className="rail-label">{item.label}</span>
              <span className="rail-hint">{item.hint}</span>
            </a>
          </li>
        ))}
      </ol>
    </nav>
  );
}

export function Section({
  id,
  eyebrow,
  title,
  lede,
  children,
}: {
  id: string;
  eyebrow: string;
  title: string;
  lede?: ReactNode;
  children: ReactNode;
}) {
  return (
    <section id={id} className="section">
      <header className="section-head">
        <p className="eyebrow">{eyebrow}</p>
        <h2>{title}</h2>
        {lede ? <p className="lede">{lede}</p> : null}
      </header>
      {children}
    </section>
  );
}

export function Panel({
  title,
  note,
  children,
  wide,
}: {
  title?: string;
  note?: ReactNode;
  children: ReactNode;
  wide?: boolean;
}) {
  return (
    <figure className={wide ? "panel panel-wide" : "panel"}>
      {title ? <figcaption className="panel-title">{title}</figcaption> : null}
      {children}
      {note ? <p className="panel-note">{note}</p> : null}
    </figure>
  );
}

export function Grid({ children, columns = 2 }: { children: ReactNode; columns?: number }) {
  return (
    <div className="grid" style={{ "--columns": columns } as React.CSSProperties}>
      {children}
    </div>
  );
}

/** The one number a section is about. Used at most once per section. */
export function Hero({
  value,
  unit,
  caption,
}: {
  value: string;
  unit?: string;
  caption: ReactNode;
}) {
  return (
    <div className="hero">
      <p className="hero-value">
        {value}
        {unit ? <span className="hero-unit">{unit}</span> : null}
      </p>
      <p className="hero-caption">{caption}</p>
    </div>
  );
}

export function StatRow({ children }: { children: ReactNode }) {
  return <div className="stat-row">{children}</div>;
}

export function StatTile({
  label,
  value,
  detail,
  tone,
}: {
  label: string;
  value: string;
  detail?: ReactNode;
  tone?: RiskBand;
}) {
  return (
    <div className="stat-tile">
      <p className="stat-label">
        {tone ? (
          <span className="dot" style={{ background: BAND_COLOR[tone] }} aria-hidden="true" />
        ) : null}
        {label}
      </p>
      <p className="stat-value">{value}</p>
      {detail ? <p className="stat-detail">{detail}</p> : null}
    </div>
  );
}

/** A fraud rule, as a labelled swatch.
 *
 * Never a bare colour: three of the five categorical hues sit below 3:1 on the light
 * surface, so the label is what carries identity and the colour only reinforces it.
 */
export function RuleChip({
  rule,
  onClick,
  active,
  count,
}: {
  rule: string;
  onClick?: () => void;
  active?: boolean;
  count?: number;
}) {
  const content = (
    <>
      <span className="chip-dot" style={{ background: ruleColor(rule) }} aria-hidden="true" />
      {ruleLabel(rule)}
      {count !== undefined ? <span className="chip-count">{count}</span> : null}
    </>
  );

  if (!onClick) return <span className="chip">{content}</span>;

  return (
    <button
      type="button"
      className={active ? "chip chip-button is-active" : "chip chip-button"}
      onClick={onClick}
      aria-pressed={active}
    >
      {content}
    </button>
  );
}

/** Risk band, always with its label — status colour never carries meaning alone. */
export function BandTag({ band }: { band: RiskBand }) {
  return (
    <span className={`band band-${band.toLowerCase()}`}>
      <span className="dot" style={{ background: BAND_COLOR[band] }} aria-hidden="true" />
      {band}
    </span>
  );
}

export function Callout({ title, children }: { title: string; children: ReactNode }) {
  return (
    <aside className="callout">
      <p className="callout-title">{title}</p>
      <div className="callout-body">{children}</div>
    </aside>
  );
}
