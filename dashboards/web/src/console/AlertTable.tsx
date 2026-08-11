/* The alert console: every HIGH-band transaction, filterable and sortable.
 *
 * All the rows are already in memory — the export ships the complete alert table — so
 * every filter is instant and there is no server to round-trip to. That is the whole
 * reason the dashboard is fed by a static export rather than an API.
 *
 * Filters sit in one row above the table, per the interaction spec. Rule filters are
 * OR within the group (an analyst looking at velocity wants every velocity alert) and
 * AND across groups (velocity *and* HDFC means both).
 */

import { useMemo, useState } from "react";
import {
  RULE_ORDER,
  count,
  inrExact,
  orDash,
  ruleColor,
  ruleLabel,
  type Alert,
} from "../data";
import { RuleChip } from "../components/ui";

type SortKey = "fraud_score" | "amount" | "event_time";

const PAGE = 40;

export function AlertTable({
  rows,
  weights,
}: {
  rows: Alert[];
  /** Rule -> points, from conf/base.yaml via the export. Shown in the row detail so an
   * analyst can see the arithmetic rather than being asked to trust the total. */
  weights: Record<string, number>;
}) {
  const [rules, setRules] = useState<string[]>([]);
  const [bank, setBank] = useState<string>("");
  const [query, setQuery] = useState("");
  const [sort, setSort] = useState<SortKey>("fraud_score");
  const [expanded, setExpanded] = useState<string | null>(null);
  const [shown, setShown] = useState(PAGE);

  const banks = useMemo(
    () => Array.from(new Set(rows.map((r) => r.payer_bank))).sort(),
    [rows],
  );

  const ruleCounts = useMemo(() => {
    const counts: Record<string, number> = {};
    for (const row of rows) for (const rule of row.reasons) counts[rule] = (counts[rule] ?? 0) + 1;
    return counts;
  }, [rows]);

  const filtered = useMemo(() => {
    const needle = query.trim().toLowerCase();
    const result = rows.filter((row) => {
      if (rules.length && !rules.some((rule) => row.reasons.includes(rule))) return false;
      if (bank && row.payer_bank !== bank) return false;
      if (needle) {
        const haystack = `${row.transaction_id} ${row.payer_vpa_masked} ${row.payee_vpa_masked} ${row.city ?? ""} ${row.app ?? ""}`;
        if (!haystack.toLowerCase().includes(needle)) return false;
      }
      return true;
    });

    return result.sort((a, b) => {
      if (sort === "event_time") return b.event_time.localeCompare(a.event_time);
      // Score ties are common — every rule weight is a multiple of five — so amount
      // breaks them. Without it the order shuffles between renders.
      if (sort === "fraud_score" && b.fraud_score !== a.fraud_score) {
        return b.fraud_score - a.fraud_score;
      }
      return b.amount - a.amount;
    });
  }, [rows, rules, bank, query, sort]);

  const toggleRule = (rule: string) => {
    setShown(PAGE);
    setRules((current) =>
      current.includes(rule) ? current.filter((r) => r !== rule) : [...current, rule],
    );
  };

  const active = rules.length > 0 || bank !== "" || query.trim() !== "";

  return (
    <div className="console">
      <div className="filters">
        <div className="filter-group">
          <span className="filter-label">Rule</span>
          {RULE_ORDER.filter((rule) => ruleCounts[rule]).map((rule) => (
            <RuleChip
              key={rule}
              rule={rule}
              count={ruleCounts[rule]}
              active={rules.includes(rule)}
              onClick={() => toggleRule(rule)}
            />
          ))}
        </div>

        <div className="filter-group">
          <label className="filter-label" htmlFor="bank-filter">
            Bank
          </label>
          <select
            id="bank-filter"
            value={bank}
            onChange={(event) => {
              setBank(event.target.value);
              setShown(PAGE);
            }}
          >
            <option value="">All</option>
            {banks.map((name) => (
              <option key={name} value={name}>
                {name}
              </option>
            ))}
          </select>
        </div>

        <div className="filter-group">
          <label className="filter-label" htmlFor="alert-search">
            Search
          </label>
          <input
            id="alert-search"
            type="search"
            placeholder="id, VPA, city…"
            value={query}
            onChange={(event) => {
              setQuery(event.target.value);
              setShown(PAGE);
            }}
          />
        </div>

        <div className="filter-group">
          <label className="filter-label" htmlFor="alert-sort">
            Sort
          </label>
          <select
            id="alert-sort"
            value={sort}
            onChange={(event) => setSort(event.target.value as SortKey)}
          >
            <option value="fraud_score">Score</option>
            <option value="amount">Amount</option>
            <option value="event_time">Most recent</option>
          </select>
        </div>

        {active ? (
          <button
            type="button"
            className="ghost-button"
            onClick={() => {
              setRules([]);
              setBank("");
              setQuery("");
              setShown(PAGE);
            }}
          >
            Clear
          </button>
        ) : null}
      </div>

      <p className="filter-summary" role="status">
        {count(filtered.length)} of {count(rows.length)} alerts
        {active ? " match these filters" : ""}
      </p>

      <div className="table-scroll">
        <table className="data-table alert-table">
          <thead>
            <tr>
              <th>Transaction</th>
              <th>When</th>
              <th className="num">Amount</th>
              <th>Payer</th>
              <th>Bank</th>
              <th>City</th>
              <th className="num">Score</th>
              <th>Why</th>
            </tr>
          </thead>
          <tbody>
            {filtered.slice(0, shown).map((row) => (
              <RowView
                key={row.transaction_id}
                row={row}
                weights={weights}
                expanded={expanded === row.transaction_id}
                onToggle={() =>
                  setExpanded((current) =>
                    current === row.transaction_id ? null : row.transaction_id,
                  )
                }
              />
            ))}
          </tbody>
        </table>
      </div>

      {shown < filtered.length ? (
        <button type="button" className="ghost-button more" onClick={() => setShown((n) => n + PAGE)}>
          Show {Math.min(PAGE, filtered.length - shown)} more
        </button>
      ) : null}

      {filtered.length === 0 ? (
        <p className="empty">No alert matches these filters.</p>
      ) : null}
    </div>
  );
}

function RowView({
  row,
  weights,
  expanded,
  onToggle,
}: {
  row: Alert;
  weights: Record<string, number>;
  expanded: boolean;
  onToggle: () => void;
}) {
  return (
    <>
      <tr className={expanded ? "is-expanded" : undefined}>
        <td>
          <button type="button" className="link-button mono" onClick={onToggle} aria-expanded={expanded}>
            {row.transaction_id}
          </button>
        </td>
        <td className="mono subtle">{row.event_time}</td>
        <td className="num mono">{inrExact(row.amount)}</td>
        <td className="mono subtle">{row.payer_vpa_masked}</td>
        <td>{row.payer_bank}</td>
        <td>{orDash(row.city)}</td>
        <td className="num score">{row.fraud_score}</td>
        <td>
          <span className="reason-chips">
            {row.reasons.map((rule) => (
              <RuleChip key={rule} rule={rule} />
            ))}
          </span>
        </td>
      </tr>
      {expanded ? (
        <tr className="detail-row">
          <td colSpan={8}>
            <div className="detail">
              <dl>
                <div>
                  <dt>Payee</dt>
                  <dd className="mono">{row.payee_vpa_masked}</dd>
                </div>
                <div>
                  <dt>App</dt>
                  <dd>{orDash(row.app)}</dd>
                </div>
                <div>
                  <dt>Status</dt>
                  <dd>{row.status}</dd>
                </div>
                <div>
                  <dt>IP (hashed)</dt>
                  <dd className="mono">{orDash(row.ip_hash)}</dd>
                </div>
              </dl>
              <div className="score-breakdown">
                <p className="detail-title">How this score was reached</p>
                <ul>
                  {row.reasons.map((rule) => (
                    <li key={rule}>
                      <span className="chip-dot" style={{ background: ruleColor(rule) }} aria-hidden="true" />
                      <span className="breakdown-rule">{ruleLabel(rule)}</span>
                      <span className="breakdown-weight mono">+{weights[rule] ?? "?"}</span>
                    </li>
                  ))}
                  <li className="breakdown-total">
                    <span className="breakdown-rule">Total</span>
                    <span className="breakdown-weight mono">{row.fraud_score}</span>
                  </li>
                </ul>
              </div>
            </div>
          </td>
        </tr>
      ) : null}
    </>
  );
}
