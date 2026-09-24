"""`generate_report`: the daily profitability report (SPEC 8.2 task 8).

Writes `/reports/profitability_<sim_date>.html` plus a CSV of the same data. The
HTML is what a fleet manager would actually read; the CSV is what they would
open in a spreadsheet. Both are served by the API at `/reports/{sim_date}`.

The report deliberately includes a DATA QUALITY section. A profitability report
that shows only the numbers invites the reader to trust them completely; showing
how many rows were quarantined, how many events the watermark dropped and how far
the speed layer drifted is what makes the figures defensible.
"""
from __future__ import annotations

import csv
import os
from datetime import datetime, timezone
from typing import Dict, List, Optional

from jinja2 import Template

from common import config, db, profitability
from common.logging import get_logger

log = get_logger("batch", stage="orchestration")

TEMPLATE = Template(
    """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Fleet profitability - {{ sim_date }}</title>
<style>
  :root { color-scheme: light; }
  body { font-family: -apple-system, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
         margin: 0; padding: 32px; background: #f6f7f9; color: #1d2430; line-height: 1.5; }
  .wrap { max-width: 1100px; margin: 0 auto; }
  h1 { font-size: 1.6rem; margin: 0 0 4px; }
  h2 { font-size: 1.1rem; margin: 32px 0 10px; padding-bottom: 6px;
       border-bottom: 2px solid #e2e5ea; }
  .sub { color: #5c6673; font-size: 0.9rem; margin-bottom: 24px; }
  .cards { display: flex; flex-wrap: wrap; gap: 12px; margin-bottom: 8px; }
  .card { background: #fff; border: 1px solid #e2e5ea; border-radius: 8px;
          padding: 14px 18px; min-width: 150px; flex: 1 1 150px; }
  .card .label { font-size: 0.72rem; text-transform: uppercase;
                 letter-spacing: 0.05em; color: #6b7480; }
  .card .value { font-size: 1.45rem; font-weight: 600; margin-top: 4px; }
  .neg { color: #b4232a; }
  .pos { color: #1c7a43; }
  table { border-collapse: collapse; width: 100%; background: #fff;
          border: 1px solid #e2e5ea; border-radius: 8px; overflow: hidden;
          font-size: 0.88rem; }
  th, td { padding: 8px 10px; text-align: right; border-bottom: 1px solid #eef0f3; }
  th { background: #f0f2f5; font-weight: 600; text-align: right;
       font-size: 0.78rem; text-transform: uppercase; letter-spacing: 0.03em; }
  th:first-child, td:first-child { text-align: left; }
  tr:last-child td { border-bottom: none; }
  .flag { display: inline-block; font-size: 0.72rem; padding: 1px 7px;
          border-radius: 10px; margin-right: 4px; background: #eceff3; color: #47505d; }
  .flag.bad { background: #fbe4e5; color: #9e1f26; }
  .flag.warn { background: #fdf0da; color: #8a5a11; }
  .reasons { font-size: 0.82rem; color: #5c6673; text-align: left; }
  .meta { margin-top: 36px; font-size: 0.8rem; color: #6b7480;
          border-top: 1px solid #e2e5ea; padding-top: 12px; }
  .empty { background: #fff; border: 1px solid #e2e5ea; border-radius: 8px;
           padding: 16px; color: #5c6673; font-size: 0.9rem; }
  .note { background: #eef4fb; border: 1px solid #cfe0f2; border-radius: 8px;
          padding: 12px 16px; font-size: 0.85rem; color: #23425f; }
</style>
</head>
<body>
<div class="wrap">

  <h1>Fleet profitability &mdash; {{ sim_date }}</h1>
  <div class="sub">
    Reconciled from the complete telemetry archive and expense file
    <strong>v{{ expense_version }}</strong>. All amounts in {{ currency }}.
  </div>

  <h2>Fleet summary</h2>
  <div class="cards">
    <div class="card"><div class="label">Vehicles</div>
      <div class="value">{{ summary.vehicles }}</div></div>
    <div class="card"><div class="label">Trips</div>
      <div class="value">{{ summary.trips }}</div></div>
    <div class="card"><div class="label">Revenue</div>
      <div class="value">{{ fmt(summary.revenue) }}</div></div>
    <div class="card"><div class="label">Cost</div>
      <div class="value">{{ fmt(summary.cost) }}</div></div>
    <div class="card"><div class="label">Net profit</div>
      <div class="value {{ 'neg' if summary.net_profit < 0 else 'pos' }}">
        {{ fmt(summary.net_profit) }}</div></div>
    <div class="card"><div class="label">Margin</div>
      <div class="value">{{ pct(summary.margin) }}</div></div>
    <div class="card"><div class="label">Distance (GPS)</div>
      <div class="value">{{ fmt(summary.gps_km) }} km</div></div>
    <div class="card"><div class="label">Utilization</div>
      <div class="value">{{ pct(summary.utilization) }}</div></div>
  </div>

  <h2>Requires attention</h2>
  {% if flagged %}
  <table>
    <tr><th>Vehicle</th><th>Revenue</th><th>Cost</th><th>Net</th><th>Margin</th>
        <th>Flags</th><th class="reasons">Why</th></tr>
    {% for row in flagged %}
    <tr>
      <td><strong>{{ row.vehicle_id }}</strong></td>
      <td>{{ fmt(row.revenue) }}</td>
      <td>{{ fmt(row.cost) }}</td>
      <td class="{{ 'neg' if row.net_profit < 0 else 'pos' }}">{{ fmt(row.net_profit) }}</td>
      <td>{{ pct(row.margin) }}</td>
      <td>
        {% if row.unprofitable %}<span class="flag bad">unprofitable</span>{% endif %}
        {% if row.becoming_unprofitable %}<span class="flag bad">declining</span>{% endif %}
        {% if row.low_margin %}<span class="flag warn">low margin</span>{% endif %}
        {% if row.distance_mismatch %}<span class="flag warn">distance</span>{% endif %}
        {% if row.in_service %}<span class="flag">in service</span>{% endif %}
        {% if row.missing_costs %}<span class="flag warn">no costs</span>{% endif %}
        {% if row.missing_telemetry %}<span class="flag warn">no telemetry</span>{% endif %}
      </td>
      <td class="reasons">{{ row.reasons | join('; ') }}</td>
    </tr>
    {% endfor %}
  </table>
  {% else %}
  <div class="empty">No vehicle was flagged on this date.</div>
  {% endif %}

  <h2>Most profitable</h2>
  <table>
    <tr><th>Vehicle</th><th>Trips</th><th>Revenue</th><th>Net</th><th>Margin</th>
        <th>Rev/km</th><th>Utilization</th></tr>
    {% for row in best %}
    <tr><td><strong>{{ row.vehicle_id }}</strong></td><td>{{ row.trips }}</td>
        <td>{{ fmt(row.revenue) }}</td><td class="pos">{{ fmt(row.net_profit) }}</td>
        <td>{{ pct(row.margin) }}</td><td>{{ fmt(row.revenue_per_km) }}</td>
        <td>{{ pct(row.utilization) }}</td></tr>
    {% endfor %}
  </table>

  <h2>Least profitable</h2>
  <table>
    <tr><th>Vehicle</th><th>Trips</th><th>Revenue</th><th>Net</th><th>Margin</th>
        <th>Cost/km</th><th>Utilization</th></tr>
    {% for row in worst %}
    <tr><td><strong>{{ row.vehicle_id }}</strong></td><td>{{ row.trips }}</td>
        <td>{{ fmt(row.revenue) }}</td>
        <td class="{{ 'neg' if row.net_profit < 0 else 'pos' }}">{{ fmt(row.net_profit) }}</td>
        <td>{{ pct(row.margin) }}</td><td>{{ fmt(row.cost_per_km) }}</td>
        <td>{{ pct(row.utilization) }}</td></tr>
    {% endfor %}
  </table>

  <h2>Distance mismatches</h2>
  {% if mismatches %}
  <table>
    <tr><th>Vehicle</th><th>GPS km</th><th>Reported km</th><th>Difference</th></tr>
    {% for row in mismatches %}
    <tr><td><strong>{{ row.vehicle_id }}</strong></td>
        <td>{{ fmt(row.gps_km) }}</td><td>{{ fmt(row.distance_covered) }}</td>
        <td class="neg">{{ pct(row.distance_mismatch_pct) }}</td></tr>
    {% endfor %}
  </table>
  {% else %}
  <div class="empty">
    No vehicle exceeded the {{ pct(mismatch_threshold) }} distance threshold.
  </div>
  {% endif %}

  <h2>Data quality</h2>
  <div class="cards">
    <div class="card"><div class="label">Expense rows quarantined</div>
      <div class="value">{{ dq.quarantined }}</div></div>
    <div class="card"><div class="label">Clean expense rows</div>
      <div class="value">{{ dq.clean }}</div></div>
    <div class="card"><div class="label">Speed-layer revenue</div>
      <div class="value">{{ fmt(drift.speed_revenue) }}</div></div>
    <div class="card"><div class="label">Batch revenue</div>
      <div class="value">{{ fmt(drift.batch_revenue) }}</div></div>
    <div class="card"><div class="label">Drift</div>
      <div class="value">{{ pct(drift.drift_ratio) }}</div></div>
  </div>

  {% if dq.by_reason %}
  <table style="margin-top:12px">
    <tr><th>Quarantine reason</th><th>Rows</th></tr>
    {% for reason, count in dq.by_reason.items() %}
    <tr><td>{{ reason }}</td><td>{{ count }}</td></tr>
    {% endfor %}
  </table>
  {% endif %}

  <div class="note" style="margin-top:14px">
    <strong>On the drift figure.</strong> The speed layer applies a
    {{ watermark }}-simulated-minute watermark and therefore drops events that
    arrive later than that; about 2% of events are delivered late by design. The
    batch layer reads the complete archive after the day closes, so it sees them
    all. A small positive drift (batch above speed) is expected and healthy. It is
    the reason profitability is computed by the batch layer and not from the live
    dashboard.
  </div>

  {% if expense_late %}
  <div class="note" style="margin-top:14px; background:#fdf0da; border-color:#f0d9ae; color:#8a5a11">
    <strong>Late file.</strong> The expense file for this date missed its
    {{ sla }}-simulated-minute SLA.
  </div>
  {% endif %}

  <div class="meta">
    Run id <strong>{{ run_id }}</strong> &middot;
    expense file version <strong>v{{ expense_version }}</strong> &middot;
    generated {{ generated_at }} &middot;
    simulated date {{ sim_date }}
  </div>

</div>
</body>
</html>
"""
)


def _fmt(value) -> str:
    """Thousands-separated, two decimals. Used for every money and km figure."""
    try:
        return f"{float(value):,.2f}"
    except (TypeError, ValueError):
        return "-"


def _pct(value) -> str:
    try:
        return f"{float(value) * 100:.1f}%"
    except (TypeError, ValueError):
        return "-"


def _summarise(rows: List[Dict]) -> Dict:
    """Fleet totals. Margin and utilization are computed from the TOTALS.

    Averaging per-vehicle margins would weight a vehicle that earned 20 units the
    same as one that earned 2000, which is not what "fleet margin" means.
    """
    revenue = sum(r["revenue"] for r in rows)
    cost = sum(r["cost"] for r in rows)
    online = sum(r.get("online_min", 0) or 0 for r in rows)
    on_trip = sum(r.get("on_trip_min", 0) or 0 for r in rows)
    return {
        "vehicles": len(rows),
        "trips": sum(r.get("trips", 0) or 0 for r in rows),
        "revenue": round(revenue, 2),
        "cost": round(cost, 2),
        "net_profit": round(revenue - cost, 2),
        "margin": round((revenue - cost) / revenue, 4) if revenue else 0.0,
        "gps_km": round(sum(r["gps_km"] for r in rows), 1),
        "utilization": round(on_trip / online, 4) if online else 0.0,
    }


def generate(sim_date: str, run_id: str) -> Dict[str, str]:
    """Render the HTML and CSV for a reconciled date. Returns both paths."""
    rows = db.query(
        "SELECT * FROM batch_vehicle_daily WHERE sim_date = %s ORDER BY vehicle_id",
        (sim_date,),
    )
    if not rows:
        raise ValueError(f"no reconciled rows for {sim_date}; nothing to report")

    rows = [dict(row) for row in rows]
    for row in rows:
        row["reasons"] = profitability.reasons_for(row)

    flagged = [
        row for row in rows
        if row["unprofitable"] or row["becoming_unprofitable"] or row["low_margin"]
        or row["distance_mismatch"] or row["missing_costs"] or row["missing_telemetry"]
    ]
    flagged.sort(key=lambda r: r["net_profit"])

    by_profit = sorted(rows, key=lambda r: r["net_profit"], reverse=True)
    mismatches = sorted(
        [r for r in rows if r["distance_mismatch"]],
        key=lambda r: r["distance_mismatch_pct"],
        reverse=True,
    )

    run = db.query_one("SELECT * FROM batch_runs WHERE run_id = %s", (run_id,)) or {}
    drift = db.query_one(
        "SELECT * FROM speed_batch_drift WHERE sim_date = %s", (sim_date,)
    ) or {"speed_revenue": 0, "batch_revenue": 0, "drift_ratio": 0}

    reason_rows = db.query(
        "SELECT reason, count(*) AS n FROM dq_issues "
        "WHERE sim_date = %s AND run_id = %s GROUP BY reason ORDER BY n DESC",
        (sim_date, run_id),
    )

    context = {
        "sim_date": sim_date,
        "run_id": run_id,
        "expense_version": run.get("expense_file_version", 1),
        "expense_late": run.get("expense_file_late", False),
        "sla": config.EXPENSE_SLA_SIM_MIN,
        "watermark": config.WATERMARK_SIM_MIN,
        "currency": config.CURRENCY_LABEL,
        "mismatch_threshold": config.DISTANCE_MISMATCH_THRESHOLD,
        "summary": _summarise(rows),
        "flagged": flagged,
        "best": by_profit[:5],
        "worst": by_profit[-5:][::-1],
        "mismatches": mismatches,
        "drift": drift,
        "dq": {
            "quarantined": run.get("quarantined_rows", 0),
            "clean": run.get("clean_rows", 0),
            "by_reason": {r["reason"]: r["n"] for r in reason_rows},
        },
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        "fmt": _fmt,
        "pct": _pct,
    }

    os.makedirs(config.REPORTS_DIR, exist_ok=True)
    html_path = os.path.join(config.REPORTS_DIR, f"profitability_{sim_date}.html")
    csv_path = os.path.join(config.REPORTS_DIR, f"profitability_{sim_date}.csv")

    # Written atomically, because the API serves this file and must never hand a
    # half-rendered page to a browser mid-demo.
    tmp = html_path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as handle:
        handle.write(TEMPLATE.render(**context))
    os.replace(tmp, html_path)

    csv_columns = [c for c in rows[0] if c != "reasons"]
    tmp_csv = csv_path + ".tmp"
    with open(tmp_csv, "w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=csv_columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    os.replace(tmp_csv, csv_path)

    log.info(
        "profitability report generated",
        extra={
            "event": "report_generated",
            "run_id": run_id,
            "sim_date": sim_date,
            "html": html_path,
            "csv": csv_path,
            "vehicles": len(rows),
            "flagged": len(flagged),
        },
    )
    return {"html": html_path, "csv": csv_path}
