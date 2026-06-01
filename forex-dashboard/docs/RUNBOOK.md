# Forex Signal Service — Runbook

## ⚠️ MANDATORY post-change check (do this after EVERY code/config change)

Whenever we change code, config, or the universe — and any time you suspect
missing signals — run the health check. This is what guarantees we never sit a
whole session with no Discord signals again.

```bat
REM 1. Restart so the change is actually loaded (a running process keeps old code in memory)
start-dashboard.bat

REM 2. Wait one scheduler cycle (≤15 min) OR trigger a refresh, then verify
cd service
python healthcheck.py            REM read-only full check
python healthcheck.py --ping      REM also posts ONE live test alert to Discord
```

**Do not consider a change "done" until `healthcheck.py` prints `ALL CHECKS PASSED`.**

The check verifies:
1. Service `/health` responds.
2. Scheduler is **fresh** — last successful refresh < 20 min ago (not stalled).
3. Discord webhook is **valid** (non-destructive — no message posted unless `--ping`).
4. Each dashboard (`/crt`, `/snr`, `/snr-m15`) returns data for all pairs.
5. Alert pipeline runs end-to-end with **zero errors**.

Exit code `0` = healthy, `1` = something failed.

---

## Why signals can stop (and what now prevents it)

**Root cause of the 2026-06-01 blackout:** MT5's `copy_rates_from_pos` is a
blocking call with no timeout. At Sunday market reopen the terminal was busy and
one fetch hung forever. Because `refresh_all` runs with `max_instances=1`, that
single hung run blocked every later run — no fetches, no alerts, for ~15h, with
no error and no log.

**Protections now in place:**

- **Timeout-guarded fetches** (`scheduler._fetch_guarded`, 15s): a hung provider
  call is abandoned so `refresh_all` always completes and can never wedge.
- **Stall watchdog** (every 5 min): if no successful refresh in 20 min, it posts
  a **🚨 "Signal service STALLED"** warning to Discord. Silence becomes loud.
- **Rotating file log** (`service/service.log`): stalls/errors are now recorded.
- **`healthcheck.py`**: the standard verification above.

---

## If you get a 🚨 STALLED warning in Discord

1. Check `service/service.log` (tail it) for the failing symbol/provider.
2. Make sure the **MT5 terminal** is running and logged in.
3. Restart: `start-dashboard.bat`.
4. Confirm recovery: `python healthcheck.py`.

---

## Alert toggles

- BTMM Discord alerts are gated by `BTMM_ALERTS_ENABLED` (env, default off).
  CRT + SNR (H4 & M15) always alert. Re-enable BTMM: set
  `BTMM_ALERTS_ENABLED=true` in `service/.env` and restart.

## Pip/SL accuracy

- Pip/point size and per-asset stop sizing live in **`service/instruments.py`**
  (`SYMBOL_SPECS`, `_CLASS_SL`). Tune floors/ATR caps there — one place, all
  strategies. Never infer pip from price magnitude.
