import { useEffect, useMemo, useState } from "react";
import { Figure, Legend } from "./charts/chrome";
import { Funnel } from "./charts/Funnel";
import { HBar, type BarDatum } from "./charts/HBar";
import { Histogram } from "./charts/Histogram";
import { TimeSeries } from "./charts/TimeSeries";
import { AlertTable } from "./console/AlertTable";
import {
  BAND_COLOR,
  RULE_ORDER,
  count,
  inr,
  inrExact,
  loadDataset,
  pct,
  rejectLabel,
  ruleColor,
  ruleLabel,
  type Dataset,
  type DimensionRow,
  type RiskBand,
} from "./data";
import {
  BandTag,
  Callout,
  Grid,
  Hero,
  Panel,
  Rail,
  RuleChip,
  Section,
  StatRow,
  StatTile,
  type NavItem,
} from "./components/ui";

const NAV: NavItem[] = [
  { id: "overview", label: "Overview", hint: "What the run produced" },
  { id: "pipeline", label: "Pipeline", hint: "Four layers, and what fell out" },
  { id: "business", label: "Business", hint: "Volume and KPIs" },
  { id: "scoring", label: "Scoring", hint: "How alerts are reached" },
  { id: "quality", label: "Detection", hint: "Measured against truth" },
  { id: "alerts", label: "Alerts", hint: "The console" },
];

const DIMENSION_LABELS: Record<DimensionRow["dimension"], string> = {
  payer_bank: "Bank",
  app: "App",
  city: "City",
};

export default function App() {
  const [data, setData] = useState<Dataset | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [active, setActive] = useState("overview");
  // Start from the viewer's OS setting. Defaulting to "light" and then stamping it on
  // the root element defeats the prefers-color-scheme block entirely — a reader whose
  // system is dark got a light page, and the dark tokens were unreachable.
  const [theme, setTheme] = useState<"light" | "dark">(() =>
    typeof window !== "undefined" &&
    window.matchMedia?.("(prefers-color-scheme: dark)").matches
      ? "dark"
      : "light",
  );

  useEffect(() => {
    loadDataset()
      .then(setData)
      .catch((e: Error) => setError(e.message));
  }, []);

  useEffect(() => {
    document.documentElement.dataset.theme = theme;
  }, [theme]);

  // Scroll spy: the section whose heading is nearest the top of the viewport wins.
  useEffect(() => {
    const onScroll = () => {
      let current = NAV[0].id;
      for (const item of NAV) {
        const element = document.getElementById(item.id);
        if (element && element.getBoundingClientRect().top <= 140) current = item.id;
      }
      setActive(current);
    };
    onScroll();
    window.addEventListener("scroll", onScroll, { passive: true });
    return () => window.removeEventListener("scroll", onScroll);
  }, [data]);

  if (error) {
    return (
      <main className="state">
        <h1>Nothing to show yet</h1>
        <p>{error}</p>
        <pre>make demo &amp;&amp; make web-data</pre>
      </main>
    );
  }

  if (!data) {
    return (
      <main className="state">
        <p className="loading">Loading the Gold layer…</p>
      </main>
    );
  }

  return (
    <>
      <Header theme={theme} onToggle={() => setTheme((t) => (t === "light" ? "dark" : "light"))} />
      <div className="shell">
        <Rail items={NAV} active={active} />
        <main>
          <Overview data={data} />
          <Pipeline data={data} />
          <Business data={data} />
          <Scoring data={data} />
          <Quality data={data} />
          <Alerts data={data} />
        </main>
      </div>
      <Footer data={data} />
    </>
  );
}

function Header({ theme, onToggle }: { theme: string; onToggle: () => void }) {
  return (
    <header className="masthead">
      <div className="masthead-inner">
        <div>
          <p className="wordmark">Project Sentinel</p>
          <p className="tagline">Real-time UPI transaction &amp; fraud detection pipeline</p>
        </div>
        <button type="button" className="ghost-button" onClick={onToggle}>
          {theme === "light" ? "Dark" : "Light"}
        </button>
      </div>
    </header>
  );
}

/* ----------------------------------------------------------------- overview */

function Overview({ data }: { data: Dataset }) {
  const { headline, scoring } = data;
  const precision = headline.alert_precision;
  const bands = Object.fromEntries(
    scoring.bands.map((b) => [b.risk_band, b.transactions]),
  ) as Record<RiskBand, number>;

  return (
    <Section
      id="overview"
      eyebrow="Overview"
      title="One run, end to end"
      lede={
        <>
          Simulated UPI telemetry is generated with defects deliberately planted in it,
          then landed, structured, cleansed and scored. Everything below is that run —{" "}
          <strong>{count(headline.scored)}</strong> transactions served from the Gold layer.
        </>
      }
    >
      <Hero
        value={inr(headline.volume)}
        caption={
          <>
            in successful volume, from {count(headline.success_count)} successful transactions —{" "}
            {pct(headline.success_rate)} of everything that reached Gold
          </>
        }
      />

      <StatRow>
        <StatTile label="Transactions scored" value={count(headline.scored)} />
        <StatTile label="Average ticket" value={inrExact(headline.avg_ticket)} />
        <StatTile label="Distinct payers" value={count(headline.distinct_payers)} />
        <StatTile
          label="Alerts raised"
          value={count(headline.alerts)}
          tone="HIGH"
          detail={
            precision
              ? `${pct(precision.true_positives / Math.max(precision.alerts, 1), 1)} were genuine`
              : undefined
          }
        />
      </StatRow>

      <StatRow>
        <StatTile label="Low risk" value={count(bands.LOW ?? 0)} tone="LOW" />
        <StatTile
          label="Medium risk"
          value={count(bands.MEDIUM ?? 0)}
          tone="MEDIUM"
          detail="worth watching, not worth a phone call"
        />
        <StatTile
          label="High risk"
          value={count(bands.HIGH ?? 0)}
          tone="HIGH"
          detail="the alert queue"
        />
      </StatRow>
    </Section>
  );
}

/* ----------------------------------------------------------------- pipeline */

function Pipeline({ data }: { data: Dataset }) {
  const { funnel } = data;
  const reasons: BarDatum[] = funnel.reasons.map((row) => ({
    key: row.reject_reason,
    label: rejectLabel(row.reject_reason),
    value: row.records,
  }));

  return (
    <Section
      id="pipeline"
      eyebrow="Medallion"
      title="Four layers, and what fell out between them"
      lede={
        <>
          Raw JSON is landed untransformed, structured into Delta with every field still a
          string, then cleansed. Rows only ever leave through two doors — the quarantine or
          the deduplicator — and both are counted, because a pipeline that cannot say what it
          discarded cannot be trusted about what it kept.
        </>
      }
    >
      <Figure
        title="Rows through the medallion layers"
        wide
        rows={funnel.stages}
        columns={[
          { key: "label", label: "Layer", render: (s) => s.label },
          { key: "rows", label: "Rows", align: "right", render: (s) => count(s.rows) },
        ]}
        note={
          <>
            {count(funnel.quarantined)} rows were quarantined with a reason and{" "}
            {count(funnel.deduplicated)} were retry storms collapsing onto an existing
            transaction id. Nothing else left.
          </>
        }
      >
        <Funnel data={funnel} />
      </Figure>

      <Grid>
        <Figure
          title="Why rows were quarantined"
          rows={funnel.reasons}
          columns={[
            { key: "reason", label: "Reason", render: (r) => rejectLabel(r.reject_reason) },
            { key: "records", label: "Records", align: "right", render: (r) => count(r.records) },
          ]}
          note="Rejected rows are written to a quarantine table with their original payload attached, not dropped."
        >
          <HBar data={reasons} format={count} labelWidth={168} width={520} />
        </Figure>

        <Panel title="What the generator planted">
          <Callout title="No dataset was supplied">
            <p>
              The specification is explicit that data is generated. The generator injects
              structural corruption — amounts as both numbers and strings, three timestamp
              formats, nulls, whitespace, case noise, negative values, truncated JSON, retry
              storms — alongside four behavioural anomalies.
            </p>
            <p>
              {count(funnel.unparseable)} lines were never valid JSON at all. They are still
              stored, still counted, and quarantined under their own reason rather than
              disappearing.
            </p>
          </Callout>
        </Panel>
      </Grid>
    </Section>
  );
}

/* ----------------------------------------------------------------- business */

function Business({ data }: { data: Dataset }) {
  const rows = data.daily.rows;
  const [dimension, setDimension] = useState<DimensionRow["dimension"]>("payer_bank");

  const dimensionRows = useMemo(
    () =>
      data.dimensions.rows
        .filter((row) => row.dimension === dimension)
        .sort((a, b) => b.total_amount - a.total_amount),
    [data.dimensions.rows, dimension],
  );

  const bars: BarDatum[] = dimensionRows.map((row) => ({
    key: row.dimension_value,
    label: row.dimension_value,
    value: row.total_amount,
  }));

  return (
    <Section
      id="business"
      eyebrow="Business"
      title="Volume, and where it comes from"
      lede="Volume counts successful transactions only — a failed payment moves no money, and counting it inflates every figure downstream."
    >
      <Figure
        title="Daily volume and flagged share"
        wide
        rows={rows}
        columns={[
          { key: "date", label: "Date", render: (r) => r.event_date },
          { key: "txns", label: "Transactions", align: "right", render: (r) => count(r.txn_count) },
          { key: "volume", label: "Volume", align: "right", render: (r) => inr(r.total_amount) },
          { key: "success", label: "Success", align: "right", render: (r) => pct(r.success_rate) },
          { key: "flagged", label: "Flagged", align: "right", render: (r) => count(r.flagged_count) },
        ]}
        note="Two measures on two frames sharing one x-axis — never a dual y-axis, which would let the crossings be placed anywhere."
      >
        <TimeSeries
          dates={rows.map((r) => r.event_date)}
          panels={[
            {
              label: "Successful volume",
              color: "var(--cat-1)",
              values: rows.map((r) => r.total_amount),
              format: inr,
            },
            {
              label: "Flagged share of transactions",
              color: "var(--cat-2)",
              values: rows.map((r) => r.flagged_count / Math.max(r.txn_count, 1)),
              format: (v) => pct(v, 2),
              zeroBased: false,
            },
          ]}
        />
      </Figure>

      <Figure
        title={`Volume by ${DIMENSION_LABELS[dimension].toLowerCase()}`}
        wide
        rows={dimensionRows}
        columns={[
          { key: "value", label: DIMENSION_LABELS[dimension], render: (r) => r.dimension_value },
          { key: "txns", label: "Transactions", align: "right", render: (r) => count(r.txn_count) },
          { key: "volume", label: "Volume", align: "right", render: (r) => inr(r.total_amount) },
          { key: "ticket", label: "Avg ticket", align: "right", render: (r) => inrExact(r.avg_ticket) },
          { key: "flagged", label: "Flagged", align: "right", render: (r) => count(r.flagged_count) },
        ]}
        note={
          <span className="toggle-row">
            {(Object.keys(DIMENSION_LABELS) as DimensionRow["dimension"][]).map((key) => (
              <button
                key={key}
                type="button"
                className={key === dimension ? "ghost-button is-active" : "ghost-button"}
                onClick={() => setDimension(key)}
              >
                {DIMENSION_LABELS[key]}
              </button>
            ))}
          </span>
        }
      >
        <HBar data={bars} format={inr} labelWidth={110} width={620} />
      </Figure>
    </Section>
  );
}

/* ----------------------------------------------------------------- scoring */

function Scoring({ data }: { data: Dataset }) {
  const { scoring, amounts } = data;

  const ruleBars: BarDatum[] = RULE_ORDER.filter((rule) =>
    scoring.rules.some((r) => r.rule === rule),
  ).map((rule) => {
    const row = scoring.rules.find((r) => r.rule === rule)!;
    return {
      key: rule,
      label: ruleLabel(rule),
      value: row.fired,
      color: ruleColor(rule),
    };
  });

  const comboBars: BarDatum[] = scoring.combinations.map((row) => ({
    key: row.combination,
    label: row.combination.split(" + ").map(ruleLabel).join(" + "),
    value: row.transactions,
    color: BAND_COLOR[row.risk_band],
  }));

  const bandOf = (score: number): RiskBand =>
    score >= scoring.band_thresholds.high
      ? "HIGH"
      : score >= scoring.band_thresholds.medium
        ? "MEDIUM"
        : "LOW";

  // Both charts below encode the risk band as colour, which makes them multi-series —
  // so each carries a legend. The bands use the reserved status palette, and the
  // categorical rule palette sits in the panel beside them; without a legend the two
  // yellows are one glance apart.
  const bandLegend = (["LOW", "MEDIUM", "HIGH"] as RiskBand[]).map((band) => ({
    label: band,
    color: BAND_COLOR[band],
  }));

  return (
    <Section
      id="scoring"
      eyebrow="Scoring"
      title="No single rule raises an alert"
      lede={
        <>
          Five rules, each contributing its weight and its name to the transaction's reasons.
          Every weight is below the HIGH threshold by design: a large payment is not fraud, a
          payment at 3am is not fraud, and a shared IP is a coffee shop. A large payment at 3am
          from an IP serving forty strangers is worth a phone call.
        </>
      }
    >
      <div className="rule-legend">
        {RULE_ORDER.map((rule) => (
          <span key={rule} className="rule-weight">
            <RuleChip rule={rule} />
            <span className="mono">+{scoring.rule_weights[rule]}</span>
          </span>
        ))}
        <span className="rule-weight threshold">
          HIGH at <span className="mono">{scoring.band_thresholds.high}</span> · MEDIUM at{" "}
          <span className="mono">{scoring.band_thresholds.medium}</span>
        </span>
      </div>

      <Figure
        title="Where the scores land"
        wide
        rows={scoring.histogram}
        columns={[
          { key: "bucket", label: "Score", render: (b) => `${b.bucket}–${b.bucket + scoring.bucket_size - 1}` },
          { key: "n", label: "Transactions", align: "right", render: (b) => count(b.transactions) },
        ]}
        note="Counts span four orders of magnitude, so the height scale is square-root — linear would hide every bucket but the first."
      >
        <Histogram
          data={scoring.histogram}
          markers={[
            { value: scoring.band_thresholds.medium, label: "MEDIUM" },
            { value: scoring.band_thresholds.high, label: "HIGH", emphasis: true },
          ]}
          formatBucket={(b) => String(b)}
          formatCount={count}
          barColor={(bucket) => BAND_COLOR[bandOf(bucket)]}
        />
        <Legend items={bandLegend} />
      </Figure>

      <Grid>
        <Figure
          title="How often each rule fired"
          rows={scoring.rules}
          columns={[
            { key: "rule", label: "Rule", render: (r) => ruleLabel(r.rule) },
            { key: "fired", label: "Fired", align: "right", render: (r) => count(r.fired) },
            { key: "alerts", label: "In alerts", align: "right", render: (r) => count(r.in_alerts) },
          ]}
          note="Shared IP fires most and alerts least on its own — it is the corroborating signal, not the accusing one."
        >
          <HBar data={ruleBars} format={count} labelWidth={110} width={520} />
        </Figure>

        <Figure
          title="Which combinations reach which band"
          rows={scoring.combinations}
          columns={[
            {
              key: "combo",
              label: "Rules that fired",
              render: (r) => r.combination.split(" + ").map(ruleLabel).join(" + "),
            },
            { key: "band", label: "Band", render: (r) => <BandTag band={r.risk_band} /> },
            { key: "n", label: "Transactions", align: "right", render: (r) => count(r.transactions) },
          ]}
          note={
            <>
              Coloured by the band the combination reaches. Every HIGH is a combination; no
              single rule gets there alone.
              {scoring.combinations_other > 0
                ? ` ${count(scoring.combinations_other)} transactions fall in rarer combinations.`
                : ""}
            </>
          }
        >
          <HBar data={comboBars} format={count} labelWidth={210} width={560} rowHeight={26} />
          <Legend items={bandLegend} />
        </Figure>
      </Grid>

      <Figure
        title="Why the high-amount threshold is not a percentile"
        wide
        rows={amounts.histogram}
        columns={[
          { key: "bucket", label: "Amount from", render: (b) => inrExact(b.bucket) },
          { key: "n", label: "Transactions", align: "right", render: (b) => count(b.transactions) },
        ]}
        note={
          <>
            Both statistics are computed on this same distribution. The 99.5th percentile —{" "}
            {inrExact(amounts.percentile_995)} — is dragged up <em>into</em> the anomalies it is
            meant to catch, because they are about 0.7% of traffic. The Tukey fence,{" "}
            {inrExact(amounts.tukey_fence)}, is built from quartiles that a fraction of a
            percent of outliers cannot move. The rule applies{" "}
            {inrExact(amounts.applied_threshold)}, the greater of the fence and an absolute
            floor.
          </>
        }
      >
        <Histogram
          data={amounts.histogram}
          markers={[
            { value: amounts.percentile_995, label: "p99.5 — contaminated" },
            { value: amounts.applied_threshold, label: "applied", emphasis: true },
          ]}
          formatBucket={(b) => inr(b)}
          formatCount={count}
        />
      </Figure>
    </Section>
  );
}

/* ----------------------------------------------------------------- quality */

function Quality({ data }: { data: Dataset }) {
  const { detection, headline } = data;
  const precision = headline.alert_precision;

  if (!detection.available) {
    return (
      <Section
        id="quality"
        eyebrow="Detection"
        title="Detection cannot be measured"
        lede="No truth labels were found beside this dataset, so the pipeline can score transactions but not grade itself."
      >
        <Panel>
          <p className="empty">Run the generator to produce labelled data.</p>
        </Panel>
      </Section>
    );
  }

  return (
    <Section
      id="quality"
      eyebrow="Detection"
      title="Measured against what was actually planted"
      lede={
        <>
          The generator records which transactions it made anomalous in a file beside the
          data — never in the payload the pipeline reads. {count(detection.labels)} labels,
          and the pipeline never sees one of them.
        </>
      }
    >
      <Panel title="Detection by injected anomaly type" wide>
        <div className="table-scroll">
          <table className="data-table">
            <thead>
              <tr>
                <th>Anomaly</th>
                <th className="num">Planted</th>
                <th className="num">Never scored</th>
                <th className="num">Alerted</th>
                <th className="num">Flagged</th>
                <th>Reached the alert queue</th>
              </tr>
            </thead>
            <tbody>
              {detection.types.map((row) => {
                const scored = Math.max(row.injected - row.not_scored, 1);
                const share = row.alerted / scored;
                return (
                  <tr key={row.injected_type}>
                    <td>{ruleLabel(row.injected_type)}</td>
                    <td className="num mono">{count(row.injected)}</td>
                    <td className="num mono subtle">{count(row.not_scored)}</td>
                    <td className="num mono">{count(row.alerted)}</td>
                    <td className="num mono">{count(row.flagged)}</td>
                    <td>
                      <span className="inline-bar">
                        <span
                          className="inline-bar-fill"
                          style={{ width: `${share * 100}%`, background: BAND_COLOR.HIGH }}
                        />
                        <span className="inline-bar-label mono">{pct(share, 0)}</span>
                      </span>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
        <p className="panel-note">
          <strong>Never scored</strong> counts anomalies whose row was quarantined for a
          structural defect, or collapsed as a duplicate, before scoring happened. Folding
          those into a recall figure would hide an ingestion problem inside a detection
          metric, so they are counted separately.
        </p>
      </Panel>

      <Grid>
        <Panel title="Alert precision">
          {precision ? (
            <Hero
              value={pct(precision.true_positives / Math.max(precision.alerts, 1), 1)}
              caption={
                <>
                  {count(precision.true_positives)} of {count(precision.alerts)} alerts were
                  transactions the generator actually planted.
                </>
              }
            />
          ) : null}
        </Panel>

        <Panel title="Two honest caveats">
          <Callout title="What these numbers do and do not show">
            <p>
              Every planted anomaly operates from the same small pool of suspicious IPs, so the
              shared-IP rule contributes to all four types. High flagged-recall partly reflects
              how the anomalies were constructed, not only how well the rules work.
            </p>
            <p>
              Velocity and fan-out use trailing time windows, so the opening transactions of a
              burst are not yet a burst and do not alert. That is intended: a rule that flagged
              them would be reading the future.
            </p>
          </Callout>
        </Panel>
      </Grid>
    </Section>
  );
}

/* ----------------------------------------------------------------- alerts */

function Alerts({ data }: { data: Dataset }) {
  const { alerts, scoring } = data;
  return (
    <Section
      id="alerts"
      eyebrow="Console"
      title="The alert queue"
      lede={
        <>
          Every HIGH-band transaction, filterable by rule, bank and free text. Expand a row to
          see the arithmetic behind its score. Identifiers are masked and IPs are salted hashes
          — the console never had access to the originals.
        </>
      }
    >
      <AlertTable rows={alerts.rows} weights={scoring.rule_weights} />
    </Section>
  );
}

/* ----------------------------------------------------------------- footer */

function Footer({ data }: { data: Dataset }) {
  const source = data.headline.source;
  return (
    <footer className="colophon">
      <p>
        Exported from the <strong>{source.environment}</strong> environment ·{" "}
        {source.engine} · {source.store}
      </p>
      <p className="subtle">
        Generated {new Date(source.generated_at).toLocaleString("en-GB")} · figures are the
        Gold layer's own, asserted against it by the test suite. Data is simulated; no real
        payment ever passed through this pipeline.
      </p>
    </footer>
  );
}
