#!/usr/bin/env python
"""Post-change health check for the forex signal service.

RUN THIS AFTER EVERY code or config change (and any time you suspect missing
signals). It confirms the dashboards AND the Discord signal pipeline are
actually working — so we never again sit a whole session with no signals.

    python healthcheck.py          # full read-only check (no Discord spam)
    python healthcheck.py --ping   # also POST one live test alert to Discord

Checks:
  1. Service /health responds.
  2. Scheduler is fresh — last successful refresh < 20 min ago (not stalled).
  3. Discord webhook is valid (non-destructive GET — no message posted).
  4. Each dashboard endpoint (/crt, /snr, /snr-m15) returns data.
  5. Alert pipeline runs end-to-end with zero errors (Discord stubbed).

Exit code 0 = all passed, 1 = something failed.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
import traceback
import urllib.request
from pathlib import Path

BASE = "http://127.0.0.1:3002"
STALL_MIN = 20

_results: list[tuple[str, bool, str]] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    _results.append((name, ok, detail))
    tag = "PASS" if ok else "FAIL"
    print(f"  [{tag}] {name}" + (f" — {detail}" if detail else ""))


def _get(path: str, timeout: float = 180):
    with urllib.request.urlopen(BASE + path, timeout=timeout) as r:
        return r.status, json.load(r)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--ping", action="store_true", help="POST a live test alert to Discord")
    args = ap.parse_args()

    print("Forex signal service — health check\n" + "-" * 40)

    # 1. Service reachable
    try:
        st, body = _get("/health", timeout=10)
        check("service /health", st == 200 and body.get("status") == "ok", f"HTTP {st}")
    except Exception as e:
        check("service /health", False, f"unreachable: {e}")
        print("\n❌ Service not reachable — launch it with start-dashboard.bat")
        return 1

    # 2. Scheduler freshness
    state = Path(__file__).parent / "scheduler_state.json"
    last = 0.0
    try:
        last = float(json.loads(state.read_text()).get("last_ok_refresh", 0))
    except Exception:
        pass
    if last:
        age = (time.time() - last) / 60
        check("scheduler fresh", age < STALL_MIN, f"last refresh {age:.0f} min ago")
    else:
        check("scheduler fresh", False, "no refresh recorded yet (wait one cycle after a restart)")

    # 3. Discord webhook valid — non-destructive GET (returns webhook metadata)
    try:
        import requests
        from config import DISCORD_WEBHOOK_URL as W
        if not W:
            check("Discord webhook valid", False, "DISCORD_WEBHOOK_URL not set")
        else:
            r = requests.get(W, timeout=10)
            ok = r.status_code == 200 and bool(r.json().get("id"))
            check("Discord webhook valid", ok, f"GET {r.status_code}")
    except Exception as e:
        check("Discord webhook valid", False, str(e))

    # 4. Dashboard endpoints return data
    for ep in ("/crt", "/snr", "/snr-m15"):
        try:
            st, body = _get(ep, timeout=180)
            n = len(body.get("pairs", []))
            check(f"dashboard {ep}", st == 200 and n > 0, f"{n} pairs")
        except Exception as e:
            check(f"dashboard {ep}", False, str(e))

    # 5. Alert pipeline dry-run — exercises analyze + alert builders, Discord stubbed
    try:
        import alerts
        import cache
        import snr_strategy
        import snr_m15_strategy
        import crt_strategy

        sent = []
        alerts._post_discord = lambda e: (sent.append(1) or True)  # type: ignore
        alerts._is_throttled = lambda p, r: False                  # type: ignore
        alerts._mark_sent = lambda p, r: None                      # type: ignore

        errs = 0

        def _drive(universe, bundle, builder):
            nonlocal errs
            cbp = {s: {k: cache.read_candles(s, iv, lim) for k, (iv, lim) in bundle.items()}
                   for s in universe}
            for mod_result in builder[0](cbp).get("pairs", []):
                try:
                    builder[1](mod_result["symbol"], mod_result)
                except Exception:
                    errs += 1

        _drive(snr_strategy.SNR_UNIVERSE,
               {"m15": ("15min", 400), "1d": ("1day", 60)},
               (snr_strategy.analyze_universe, alerts.alert_snr_setup))
        _drive(snr_m15_strategy.SNR_M15_UNIVERSE,
               {"1h": ("1h", 200), "m15": ("15min", 400)},
               (snr_m15_strategy.analyze_universe, alerts.alert_snr_m15_setup))
        _drive(crt_strategy.CRT_UNIVERSE,
               {"m15": ("15min", 400)},
               (crt_strategy.analyze_universe, alerts.alert_crt_setup))

        check("alert pipeline runs clean", errs == 0,
              f"{len(sent)} setups would fire, {errs} errors")
    except Exception:
        check("alert pipeline runs clean", False, traceback.format_exc().splitlines()[-1])

    # Optional: live Discord delivery test
    if args.ping:
        try:
            import requests
            from config import DISCORD_WEBHOOK_URL as W
            r = requests.post(W, json={"embeds": [{
                "title": "✅ Health check ping",
                "description": "Live Discord delivery test — pipeline OK.",
                "color": 0x4FC3F7,
            }]}, timeout=10)
            check("Discord live POST", r.status_code == 204, f"POST {r.status_code}")
        except Exception as e:
            check("Discord live POST", False, str(e))

    all_ok = all(ok for _, ok, _ in _results)
    print("-" * 40)
    print("ALL CHECKS PASSED" if all_ok else "SOME CHECKS FAILED")
    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())
