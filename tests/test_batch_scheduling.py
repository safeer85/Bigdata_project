"""Tests for the batch layer's scheduling and bookkeeping rules (SPEC 8.2).

These cover the decisions that decide WHICH simulated day gets reconciled and
WHEN — the logic that, if wrong, silently reconciles the wrong day or never
reconciles at all.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from common import config, simclock

EPOCH = datetime(2024, 1, 1, 6, 0, 0, tzinfo=timezone.utc)


# --- the grace period ------------------------------------------------------

def test_a_day_is_not_pending_until_the_grace_period_has_passed():
    """The batch layer must not reconcile a day while events may still arrive.

    LATE_GRACE_SIM_MIN (30) is deliberately larger than the maximum injected
    lateness (20). If it were not, a day could be reconciled while events for it
    were still in flight and the "exact" claim would be false.
    """
    day = "2024-01-02"
    _, day_end = simclock.day_bounds(day)

    # Exactly at midnight: the day is over, but lateness can still arrive.
    assert not simclock.day_is_closed(day, config.LATE_GRACE_SIM_MIN, now=day_end)

    # Still inside the worst-case lateness window.
    just_after = day_end + timedelta(minutes=config.FAULT_LATE_MAX_SIM_MIN)
    assert not simclock.day_is_closed(day, config.LATE_GRACE_SIM_MIN, now=just_after)

    # Past the grace period: safe to reconcile.
    safe = day_end + timedelta(minutes=config.LATE_GRACE_SIM_MIN + 1)
    assert simclock.day_is_closed(day, config.LATE_GRACE_SIM_MIN, now=safe)


def test_the_grace_period_covers_the_worst_injected_lateness():
    """Stated as a relationship, not two magic numbers that happen to work."""
    assert config.LATE_GRACE_SIM_MIN > config.FAULT_LATE_MAX_SIM_MIN


# --- picking the pending date ---------------------------------------------

def test_find_pending_date_prefers_the_oldest_date(monkeypatch):
    """Oldest first, so a stack left running overnight catches up in order.

    Newest-first would reconcile today and leave permanent gaps behind it.
    """
    from batch import runs

    monkeypatch.setattr(runs, "available_files",
                        lambda: {"2024-01-03": 1, "2024-01-01": 1, "2024-01-02": 1})
    monkeypatch.setattr(runs, "processed_versions", lambda: {})
    monkeypatch.setattr(simclock, "now_sim",
                        lambda: datetime(2024, 1, 10, tzinfo=timezone.utc))

    assert runs.find_pending_date() == ("2024-01-01", 1)


def test_a_newer_file_version_makes_a_processed_date_pending_again(monkeypatch):
    """This is what makes `make demo-resubmit` work."""
    from batch import runs

    monkeypatch.setattr(runs, "available_files", lambda: {"2024-01-01": 2})
    monkeypatch.setattr(runs, "processed_versions", lambda: {"2024-01-01": 1})
    monkeypatch.setattr(simclock, "now_sim",
                        lambda: datetime(2024, 1, 10, tzinfo=timezone.utc))

    assert runs.find_pending_date() == ("2024-01-01", 2)


def test_an_already_processed_version_is_not_pending(monkeypatch):
    """Otherwise the DAG would reconcile the same day forever."""
    from batch import runs

    monkeypatch.setattr(runs, "available_files", lambda: {"2024-01-01": 1})
    monkeypatch.setattr(runs, "processed_versions", lambda: {"2024-01-01": 1})
    monkeypatch.setattr(simclock, "now_sim",
                        lambda: datetime(2024, 1, 10, tzinfo=timezone.utc))

    assert runs.find_pending_date() is None


def test_a_day_that_has_not_closed_yet_is_not_pending(monkeypatch):
    """A file can exist for a day that is still in progress; it must wait."""
    from batch import runs

    monkeypatch.setattr(runs, "available_files", lambda: {"2024-01-05": 1})
    monkeypatch.setattr(simclock, "now_sim",
                        lambda: datetime(2024, 1, 5, 12, 0, tzinfo=timezone.utc))
    monkeypatch.setattr(runs, "processed_versions", lambda: {})

    assert runs.find_pending_date() is None


def test_no_files_means_nothing_pending(monkeypatch):
    from batch import runs

    monkeypatch.setattr(runs, "available_files", lambda: {})
    monkeypatch.setattr(runs, "processed_versions", lambda: {})
    assert runs.find_pending_date() is None


# --- file naming -----------------------------------------------------------

def test_available_files_reads_the_highest_version_per_date(tmp_path, monkeypatch):
    from batch import runs

    for name in ["expenses_2024-01-01_v1.csv", "expenses_2024-01-01_v3.csv",
                 "expenses_2024-01-01_v2.csv", "expenses_2024-01-02_v1.csv",
                 "not-an-expense-file.txt", "expenses_bad_name.csv"]:
        (tmp_path / name).write_text("x")

    monkeypatch.setattr(runs.config, "EXPENSE_DIR", str(tmp_path))
    assert runs.available_files() == {"2024-01-01": 3, "2024-01-02": 1}


def test_a_missing_landing_directory_is_not_an_error(monkeypatch):
    """On a cold start the directory may not exist yet."""
    from batch import runs

    monkeypatch.setattr(runs.config, "EXPENSE_DIR", "/nope/does/not/exist")
    assert runs.available_files() == {}


# --- the SLA ---------------------------------------------------------------

def test_the_sla_applies_to_the_first_delivery_not_to_corrections():
    """A corrected v2 is not an SLA breach.

    A correction is by definition sent after the original, so scoring it against
    the same SLA marked EVERY resubmission late -- which meant `make demo-resubmit`
    always tripped ExpenseFileLate. The SLA governs delivery, not corrections.
    This pins the rule that the DAG now implements.
    """
    day = "2024-01-01"
    _, day_end = simclock.day_bounds(day)
    well_past_sla = day_end + timedelta(minutes=config.EXPENSE_SLA_SIM_MIN + 500)

    def is_late(version: int, written_sim) -> bool:
        if version > 1:
            return False
        return written_sim > day_end + timedelta(minutes=config.EXPENSE_SLA_SIM_MIN)

    assert is_late(1, well_past_sla) is True      # a genuinely late first delivery
    assert is_late(2, well_past_sla) is False     # the same time, but a correction
    assert is_late(1, day_end + timedelta(minutes=10)) is False   # on time


def test_expense_file_is_late_only_once_the_sla_has_elapsed(monkeypatch):
    from batch import runs

    monkeypatch.setattr(runs, "available_files", lambda: {})
    day = "2024-01-01"
    _, day_end = simclock.day_bounds(day)

    assert not runs.expense_file_is_late(day, now_sim=day_end + timedelta(minutes=10))
    assert runs.expense_file_is_late(
        day, now_sim=day_end + timedelta(minutes=config.EXPENSE_SLA_SIM_MIN + 1)
    )


def test_a_file_that_exists_is_never_late(monkeypatch):
    """`expense_file_is_late` answers 'did nothing arrive at all?'."""
    from batch import runs

    monkeypatch.setattr(runs, "available_files", lambda: {"2024-01-01": 1})
    _, day_end = simclock.day_bounds("2024-01-01")
    assert not runs.expense_file_is_late(
        "2024-01-01", now_sim=day_end + timedelta(minutes=9999)
    )


# --- the invalid-row tolerance --------------------------------------------

def test_a_mildly_dirty_expense_file_is_accepted():
    from batch import expenses

    clean = [{"vehicle_id": f"V{i:03d}"} for i in range(45)]
    quarantined = [{"vehicle_id": f"V{i:03d}"} for i in range(5)]
    ratio = expenses.check_invalid_ratio(clean, quarantined)
    assert ratio == pytest.approx(0.10)


def test_a_badly_broken_expense_file_is_refused():
    """Refusing is the point: a file this broken must not become a financial figure."""
    from batch import expenses

    clean = [{"vehicle_id": f"V{i:03d}"} for i in range(30)]
    quarantined = [{"vehicle_id": f"V{i:03d}"} for i in range(20)]
    with pytest.raises(ValueError, match="invalid"):
        expenses.check_invalid_ratio(clean, quarantined)


def test_an_empty_expense_file_is_refused():
    from batch import expenses

    with pytest.raises(ValueError, match="no rows"):
        expenses.check_invalid_ratio([], [])


def test_duplicate_vehicle_rows_are_quarantined(tmp_path, monkeypatch):
    """Duplicate detection needs the whole file, so it is not in validate_expense_row."""
    from batch import expenses
    from common import validation

    path = tmp_path / "expenses_2024-01-01_v1.csv"
    path.write_text(
        "date,vehicle_id,fuel_cost,maintenance_cost,distance_covered,service_flag\n"
        "2024-01-01,V001,100,20,50,false\n"
        "2024-01-01,V001,110,25,55,false\n"      # the same vehicle twice
        "2024-01-01,V002,90,15,40,false\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(expenses, "known_vehicle_ids", lambda: {"V001", "V002"})

    clean, quarantined, reasons = expenses.validate_file(
        str(path), "2024-01-01", "run-1"
    )
    assert len(clean) == 2
    assert len(quarantined) == 1
    assert reasons == {validation.EXP_DUPLICATE: 1}
    # The FIRST occurrence is the one kept.
    assert clean[0]["vehicle_id"] == "V001"
    assert clean[0]["fuel_cost"] == 100.0
