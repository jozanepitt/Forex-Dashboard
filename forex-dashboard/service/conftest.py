"""Shared pytest fixtures for the service tests."""
import pytest

import alerts


@pytest.fixture(autouse=True)
def _isolated_alert_state(tmp_path, monkeypatch):
    """Alert tests call the real _mark_sent, which persists the throttle map.
    Point it at a temp file so a test run can never rewrite the live service's
    alerts_state.json (it did: test keys ended up throttling real alerts)."""
    monkeypatch.setattr(alerts, "_STATE_FILE", tmp_path / "alerts_state.json")
    # Each strategy's own alert tests exercise its gates; the production
    # DISCORD_SIGNALS allowlist (TDI123:M15:A only) is tested on its own in
    # test_discord_allowlist.py.
    monkeypatch.setattr(alerts, "DISCORD_SIGNALS", "*")
