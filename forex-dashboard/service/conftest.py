"""Shared pytest fixtures for the service tests."""
import pytest

import alerts


@pytest.fixture(autouse=True)
def _isolated_alert_state(tmp_path, monkeypatch):
    """Alert tests call the real _mark_sent, which persists the throttle map.
    Point it at a temp file so a test run can never rewrite the live service's
    alerts_state.json (it did: test keys ended up throttling real alerts)."""
    monkeypatch.setattr(alerts, "_STATE_FILE", tmp_path / "alerts_state.json")
