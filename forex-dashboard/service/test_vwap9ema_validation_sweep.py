"""Unit tests for the VWAP+9EMA validation sweep's pass/fail predicate.

Uses synthetic stats dicts — no real market data or MT5 access needed,
since passes_acceptance() is a pure function over already-computed stats.
Acceptance rule per docs/superpowers/specs/2026-09-13-vwap9ema-mt5-validation-design.md
("Validation gate"): OOS expectancy > 0 AND OOS profit factor >= 1.2 AND
OOS trade count >= 150 AND positive in the majority of tested years.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "vwap9ema_backtest"))
from run_validation_sweep import passes_acceptance  # noqa: E402


def test_passes_when_all_criteria_met():
    oos = dict(n=200, exp_r=0.15, pf=1.5)
    years = {2023: dict(exp_r=0.1), 2024: dict(exp_r=0.2), 2025: dict(exp_r=-0.05)}
    assert passes_acceptance(oos, years) is True


def test_fails_when_too_few_trades():
    oos = dict(n=100, exp_r=0.15, pf=1.5)
    years = {2023: dict(exp_r=0.1), 2024: dict(exp_r=0.2)}
    assert passes_acceptance(oos, years) is False


def test_fails_when_expectancy_not_positive():
    oos = dict(n=200, exp_r=0.0, pf=1.5)
    years = {2023: dict(exp_r=0.1)}
    assert passes_acceptance(oos, years) is False


def test_fails_when_profit_factor_below_threshold():
    oos = dict(n=200, exp_r=0.15, pf=1.1)
    years = {2023: dict(exp_r=0.1)}
    assert passes_acceptance(oos, years) is False


def test_fails_when_majority_of_years_negative():
    oos = dict(n=200, exp_r=0.15, pf=1.5)
    years = {2023: dict(exp_r=-0.1), 2024: dict(exp_r=-0.2), 2025: dict(exp_r=0.3)}
    assert passes_acceptance(oos, years) is False


def test_fails_when_no_year_data():
    oos = dict(n=200, exp_r=0.15, pf=1.5)
    assert passes_acceptance(oos, {}) is False


def test_fails_when_oos_has_zero_trades():
    oos = dict(n=0)
    years = {2023: dict(exp_r=0.1)}
    assert passes_acceptance(oos, years) is False
