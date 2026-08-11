/* Typed loaders for the JSON that `sentinel-web-export` writes out of the Gold layer.
 *
 * The shapes here mirror the panel builders in src/sentinel/web/export.py one-for-one.
 * tests/spark/test_web_export.py asserts the emitted numbers against the Gold tables,
 * so if these types and that module ever disagree the tests fail rather than the page
 * quietly rendering a stale or wrong figure.
 */

/** Where a panel's numbers came from. Stamped by the export, never by the page. */
export interface Provenance {
  /** Config environment the export ran under: "local" or "databricks". */
  environment: string;
  /** Engine that computed them, e.g. "Spark (local)". */
  engine: string;
  /** Where Gold was read from, e.g. "Delta on local filesystem". */
  store: string;
  generated_at: string;
}

export interface Headline {
  scored: number;
  volume: number;
  success_count: number;
  success_rate: number;
  avg_ticket: number;
  distinct_payers: number;
  alerts: number;
  flagged: number;
  high_amount_threshold: number;
  /** Null when no truth labels exist — the pipeline runs fine on data it did not generate. */
  alert_precision: { true_positives: number; alerts: number } | null;
  source: Provenance;
}

export interface Funnel {
  stages: { stage: string; label: string; rows: number }[];
  quarantined: number;
  /** Derived: Bronze minus what left through the two doors we can count. */
  deduplicated: number;
  unparseable: number;
  reasons: { reject_reason: string; records: number }[];
  source: Provenance;
}

export interface DailyRow {
  event_date: string;
  txn_count: number;
  total_amount: number;
  success_count: number;
  success_rate: number;
  avg_ticket: number;
  distinct_payers: number;
  flagged_count: number;
  flagged_amount: number;
}

export interface DimensionRow {
  dimension: "payer_bank" | "app" | "city";
  dimension_value: string;
  txn_count: number;
  total_amount: number;
  avg_ticket: number;
  success_rate: number;
  flagged_count: number;
}

export type RiskBand = "LOW" | "MEDIUM" | "HIGH";

export interface Scoring {
  bands: { risk_band: RiskBand; transactions: number }[];
  band_thresholds: { medium: number; high: number };
  bucket_size: number;
  histogram: { bucket: number; transactions: number }[];
  rules: { rule: string; fired: number; in_alerts: number }[];
  rule_weights: Record<string, number>;
  combinations: { combination: string; risk_band: RiskBand; transactions: number }[];
  combinations_other: number;
  source: Provenance;
}

export interface Amounts {
  histogram: { bucket: number; transactions: number }[];
  q1: number;
  median: number;
  q3: number;
  max: number;
  /** The contaminated statistic: dragged up by the anomalies it should catch. */
  percentile_995: number;
  /** The robust one: built from quartiles the outliers cannot move. */
  tukey_fence: number;
  floor: number;
  applied_threshold: number;
  source: Provenance;
}

export interface Detection {
  available: boolean;
  labels: number;
  types: {
    injected_type: string;
    injected: number;
    not_scored: number;
    alerted: number;
    flagged: number;
    recall_flagged: number;
  }[];
  source: Provenance;
}

export interface Alert {
  transaction_id: string;
  event_time: string;
  event_date: string;
  amount: number;
  status: string;
  payer_vpa_masked: string;
  payee_vpa_masked: string;
  payer_bank: string;
  // Optional in the payload, and the generator nulls a share of them on purpose.
  // Silver quarantines rows missing a *required* field and keeps these as nulls, so
  // they reach the console as real absences and must render as such.
  app: string | null;
  city: string | null;
  ip_hash: string | null;
  fraud_score: number;
  reasons: string[];
}

/** Render a possibly-absent value. An empty cell reads as a broken table. */
export const orDash = (value: string | null | undefined): string => value ?? "—";

export interface Alerts {
  total: number;
  truncated?: boolean;
  rows: Alert[];
  source: Provenance;
}

export interface Dataset {
  headline: Headline;
  funnel: Funnel;
  daily: { rows: DailyRow[]; source: Provenance };
  dimensions: { rows: DimensionRow[]; source: Provenance };
  scoring: Scoring;
  amounts: Amounts;
  detection: Detection;
  alerts: Alerts;
}

const FILES = [
  "headline",
  "funnel",
  "daily",
  "dimensions",
  "scoring",
  "amounts",
  "detection",
  "alerts",
] as const;

async function fetchJson<T>(name: string): Promise<T> {
  // BASE_URL, not a leading slash: the build is relocatable, so the page has to work
  // from a subdirectory as well as from the root.
  const response = await fetch(`${import.meta.env.BASE_URL}data/${name}.json`);
  if (!response.ok) {
    throw new Error(
      `Could not load ${name}.json (${response.status}). Run \`make web-data\` to export the Gold layer.`,
    );
  }
  return (await response.json()) as T;
}

export async function loadDataset(): Promise<Dataset> {
  const loaded = await Promise.all(FILES.map((name) => fetchJson<unknown>(name)));
  return Object.fromEntries(FILES.map((name, i) => [name, loaded[i]])) as unknown as Dataset;
}

/* ----------------------------------------------------------------- formatting */

/** Indian digit grouping: 1,23,456 rather than 123,456. The audience is Indian. */
const INR = new Intl.NumberFormat("en-IN");
const INR_COMPACT = new Intl.NumberFormat("en-IN", { maximumFractionDigits: 2 });

export const count = (n: number): string => INR.format(Math.round(n));

/** Rupees at a readable magnitude: crore above 1cr, lakh above 1L, else plain. */
export function inr(amount: number): string {
  if (Math.abs(amount) >= 1e7) return `₹${INR_COMPACT.format(amount / 1e7)} Cr`;
  if (Math.abs(amount) >= 1e5) return `₹${INR_COMPACT.format(amount / 1e5)} L`;
  return `₹${INR.format(Math.round(amount))}`;
}

export const inrExact = (amount: number): string =>
  `₹${INR.format(Math.round(amount))}`;

export const pct = (fraction: number, digits = 1): string =>
  `${(fraction * 100).toFixed(digits)}%`;

/** "2026-08-05" -> "5 Aug". Axis ticks only; tables keep the full date. */
export function shortDate(iso: string): string {
  const date = new Date(`${iso}T00:00:00`);
  return `${date.getDate()} ${date.toLocaleString("en-GB", { month: "short" })}`;
}

/** Rule slug -> the label a human reads. */
export const RULE_LABELS: Record<string, string> = {
  high_amount: "High amount",
  shared_ip: "Shared IP",
  velocity: "Velocity",
  fanout: "Fan-out",
  odd_hour: "Odd hour",
};

/** Fixed slot order — assigned once, never cycled, never reordered by rank. */
export const RULE_ORDER = ["high_amount", "shared_ip", "velocity", "fanout", "odd_hour"];

export const ruleColor = (rule: string): string => {
  const index = RULE_ORDER.indexOf(rule);
  return index >= 0 ? `var(--cat-${index + 1})` : "var(--text-muted)";
};

export const ruleLabel = (rule: string): string => RULE_LABELS[rule] ?? rule;

/** Quarantine reason slug -> human label. */
export const REJECT_LABELS: Record<string, string> = {
  non_positive_amount: "Amount ≤ 0",
  missing_event_time: "No timestamp",
  missing_amount: "No amount",
  missing_transaction_id: "No transaction id",
  malformed_json: "Malformed JSON",
  unparseable_event_time: "Unparseable timestamp",
  unparseable_amount: "Unparseable amount",
  amount_exceeds_limit: "Above limit",
  invalid_status: "Unknown status",
  invalid_currency: "Non-INR",
};

export const rejectLabel = (reason: string): string => REJECT_LABELS[reason] ?? reason;

export const BAND_COLOR: Record<RiskBand, string> = {
  LOW: "var(--status-good)",
  MEDIUM: "var(--status-warning)",
  HIGH: "var(--status-critical)",
};
