"""Profitability rules (SPEC 8.2 task 5), shared by the batch job and the API.

Every flag the daily report shows is decided here, in pure Python over plain
numbers. The Spark job applies them through a small wrapper; the tests exercise
them directly. No flag is defined twice.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Dict, List, Optional, Sequence

from common import config


@dataclass
class Profitability:
    """One vehicle-day of reconciled economics."""

    vehicle_id: str
    sim_date: str
    revenue: float
    fuel_cost: float
    maintenance_cost: float
    cost: float
    net_profit: float
    margin: float
    gps_km: float
    distance_covered: float
    revenue_per_km: float
    cost_per_km: float
    distance_mismatch_pct: float
    unprofitable: bool
    low_margin: bool
    distance_mismatch: bool
    in_service: bool
    missing_costs: bool
    missing_telemetry: bool

    def as_dict(self) -> Dict[str, Any]:
        return asdict(self)


def _safe_div(numerator: float, denominator: float) -> float:
    """Division that yields 0.0 instead of raising when the denominator is 0.

    A vehicle can legitimately have zero revenue, zero km or zero cost on a day it
    sat in the garage. Those rows must still appear in the report, so every ratio
    degrades to 0.0 rather than dropping the row.
    """
    if not denominator:
        return 0.0
    return numerator / denominator


def reconcile(
    vehicle_id: str,
    sim_date: str,
    revenue: Optional[float],
    gps_km: Optional[float],
    fuel_cost: Optional[float],
    maintenance_cost: Optional[float],
    distance_covered: Optional[float],
    service_flag: Optional[bool],
    has_telemetry: bool,
    has_expenses: bool,
) -> Profitability:
    """Join one vehicle's telemetry-day with its expense row and flag it.

    `has_telemetry` / `has_expenses` are passed in rather than inferred from nulls
    because "no expense row at all" (missing_costs) and "an expense row of 0" are
    different findings, and the report distinguishes them.
    """
    revenue = float(revenue or 0.0)
    gps_km = float(gps_km or 0.0)
    fuel_cost = float(fuel_cost or 0.0)
    maintenance_cost = float(maintenance_cost or 0.0)
    distance_covered = float(distance_covered or 0.0)

    cost = round(fuel_cost + maintenance_cost, 2)
    net_profit = round(revenue - cost, 2)
    # Margin is net over revenue: "of every unit earned, how much is kept".
    margin = round(_safe_div(net_profit, revenue), 4)

    revenue_per_km = round(_safe_div(revenue, gps_km), 4)
    cost_per_km = round(_safe_div(cost, gps_km), 4)

    # Compared against the PARTNER's reported distance, because that is the figure
    # being audited. Using gps_km as the denominator would let a large over-report
    # hide behind a large denominator.
    if distance_covered > 0:
        mismatch = round(abs(gps_km - distance_covered) / distance_covered, 4)
    else:
        mismatch = 0.0

    return Profitability(
        vehicle_id=vehicle_id,
        sim_date=sim_date,
        revenue=round(revenue, 2),
        fuel_cost=round(fuel_cost, 2),
        maintenance_cost=round(maintenance_cost, 2),
        cost=cost,
        net_profit=net_profit,
        margin=margin,
        gps_km=round(gps_km, 3),
        distance_covered=round(distance_covered, 3),
        revenue_per_km=revenue_per_km,
        cost_per_km=cost_per_km,
        distance_mismatch_pct=mismatch,
        unprofitable=net_profit < 0,
        # Only meaningful when there was revenue: a zero-revenue day has margin 0
        # by construction and would otherwise flood the report with false positives.
        low_margin=revenue > 0 and margin < config.MARGIN_THRESHOLD,
        distance_mismatch=mismatch > config.DISTANCE_MISMATCH_THRESHOLD,
        in_service=bool(service_flag),
        missing_costs=has_telemetry and not has_expenses,
        missing_telemetry=has_expenses and not has_telemetry,
    )


def becoming_unprofitable(net_profit_history: Sequence[float]) -> bool:
    """Trend flag over a vehicle's recent daily net profit, OLDEST first.

    True when either:
      * net profit fell on three consecutive days (a slide, even if still positive), or
      * the vehicle was loss-making on 2 of the last 3 days.

    Two rules rather than one because they catch different failures: the first
    catches a healthy vehicle degrading, the second catches one that is already
    erratic. A vehicle with fewer than 3 days of history is never flagged: we
    refuse to call a trend from two points.
    """
    history = [float(x) for x in net_profit_history]
    if len(history) < 3:
        return False

    last3 = history[-3:]
    if last3[0] > last3[1] > last3[2]:
        return True
    if sum(1 for value in last3 if value < 0) >= 2:
        return True
    return False


def reasons_for(row: Dict[str, Any]) -> List[str]:
    """Human-readable reasons behind a flagged vehicle, for the HTML report."""
    out: List[str] = []
    if row.get("unprofitable"):
        out.append(
            "net profit {:.2f} {} is negative".format(
                row.get("net_profit", 0.0), config.CURRENCY_LABEL
            )
        )
    if row.get("becoming_unprofitable"):
        out.append("net profit trending down or loss-making on 2 of the last 3 days")
    if row.get("low_margin"):
        out.append(
            "margin {:.1%} is below the {:.0%} threshold".format(
                row.get("margin", 0.0), config.MARGIN_THRESHOLD
            )
        )
    if row.get("distance_mismatch"):
        out.append(
            "GPS distance differs from reported distance by {:.1%}".format(
                row.get("distance_mismatch_pct", 0.0)
            )
        )
    if row.get("in_service"):
        out.append("vehicle was serviced or in the garage")
    if row.get("missing_costs"):
        out.append("telemetry present but no expense row")
    if row.get("missing_telemetry"):
        out.append("expense row present but no telemetry")
    return out
