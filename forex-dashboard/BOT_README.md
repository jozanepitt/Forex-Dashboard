# Backtests and the execution bot

This is the research harness and the MetaTrader 5 execution bot for the scanners already in `service/`. The backtest does not place orders. The bot does, on the machine where the Exness MT5 terminal is open, and it refuses a real account unless you set two flags.

Read `BACKTEST_RESULTS.md` before pointing the bot at a strategy. The harness is allowed to conclude that a scanner has no edge after costs. If it does, leave `BOT_STRATEGY` empty.

The run in this branch (Dukascopy BID, 2023-01-02 through 2026-10-16, ten symbols, Exness Standard costs, with the short side paying the spread on a target and a 3× spread during 21:00–22:00 UTC) did not find one. Every unfiltered pooled out-of-sample expectancy is negative, and so is every alert-gated one. The least negative unfiltered row is BTMM ID50 on M15: 1,265 out-of-sample trades, 65.9% wins, profit factor 0.70, −0.103R per trade, −0.237R if the spread is doubled, max drawdown 132R. In-sample was −0.088R. That is a loss. A few symbol cells are positive; they were not a separate test. Leave `BOT_STRATEGY` empty.

## Run the backtest

From `service/`, with the extra packages in `service/research_backtest/requirements.txt` installed (`numpy`, `matplotlib`, `dukascopy-python`):

```bash
python -m research_backtest.download
python -m research_backtest.run
```

`download` pulls Dukascopy BID M1 bars from 2023-01-02 through 2026-10-17 for ten symbols (seven majors, EUR/JPY, GBP/JPY, XAU/USD) into `service/research_backtest/data/`. Those files are gitignored. The feed is free and has no API key. Re-running skips a symbol that already covers the range.

`run` defaults to 3 worker processes. It resamples those bars, calls the live scanner functions bar by bar, and writes:

- `BACKTEST_RESULTS.md` (this folder)
- `service/research_backtest/artifacts/summary.json`
- equity-curve PNGs in that same `artifacts/` folder

A smaller run:

```bash
python -m research_backtest.run --symbols EUR/USD --strategies engulfing_h1 --workers 1
```

In-sample is every signal before 2025-04-01. Out-of-sample is the rest. Costs are a constant Exness Standard spread plus a little slippage, commission zero. The assumptions are listed in the results file and in `service/bot/costs.py`. Nothing in the scanner settings is fitted on the out-of-sample window.

M1, and a few of the heavier M5 tests, run on EUR/USD, GBP/USD, USD/JPY, and XAU/USD only. The other symbols run the native timeframes plus M5/M15 where that is cheap enough to finish. The results file says which.

## Run the bot on an Exness demo first

The bot runs on Windows, next to the MT5 terminal. This repository's Linux service cannot host it: the official `MetaTrader5` package talks to a local terminal.

1. Open an Exness **demo** account and log the MT5 terminal into it. Leave the terminal running.
2. `pip install -r service/requirements.txt` in the Python that can `import MetaTrader5`.
3. Copy `service/.env.example` to `service/.env` if you do not already have one. Fill in:

   ```
   MT5_LOGIN=your_demo_login
   MT5_PASSWORD=your_demo_password
   MT5_SERVER=Exness-MT5Trial
   MT5_SYMBOL_SUFFIX=m
   MT5_BOT_LIVE=false
   MT5_BOT_CONFIRM_LIVE=false
   BOT_STRATEGY=
   BOT_TIMEFRAME=M15
   BOT_SYMBOLS=EUR/USD
   ```

   Standard-account symbols on Exness usually end in `m` (`EURUSDm`). Do not commit `.env`.

4. Leave `BOT_STRATEGY` empty until `BACKTEST_RESULTS.md` names something you are willing to forward-test. Then set it to one of:

   `engulfing`, `vwap_bounce`, `vwap_mr`, `vwap9ema`, `id50`, `tdi123`, `btmm`, `crt_5am`, `crt_1am`

   and set `BOT_TIMEFRAME` to the timeframe you actually tested (`M1`, `M5`, `M15`, `H1`, `H4`). CRT ignores the timeframe and reads M15.

5. From `service/`:

   ```bash
   python -m bot --once
   python -m bot
   ```

   `--once` is a single poll. The loop polls every `BOT_POLL_SECONDS` (default 20).

If the terminal is already logged in you can leave login, password, and server blank and the bot attaches to that session. If the account is real, the process exits unless both live flags are `true`.

## Risk controls

| Control | Default | What it does |
|---|---|---|
| Demo gate | live flags `false` | A real account is rejected. Demo and contest accounts run. |
| `BOT_STRATEGY` empty | empty | Process exits without sending an order. |
| Risk per trade | `2` percent of equity | Hard ceiling is 2%. A higher `BOT_RISK_PCT` is cut to 2%. Lots are floored to the volume step. The stop distance includes the spread and slippage (and commission, which is $0 on Exness Standard), so a 3-pip stop is not sized as if the spread were free. Below the minimum lot, the trade is skipped. |
| Max open positions | 3 | Counts every open position and every pending order on the account. Pending orders expire at the signal's own deadline, or after 8 hours. |
| Daily loss limit | `6` percent of the UTC-day starting equity | Configurable from 2% to 6%. Default 6% so three full 2% losses halt the day. The baseline is equity, not balance. Realized plus floating. Once hit, no new trades until the next UTC day. |
| Max trades per day | 5 | Entries with this bot's magic number since UTC midnight. |
| Spread filter | 2× the assumed typical spread | Unknown symbols are skipped. The typicals live in `service/bot/costs.py`. |
| Session filter | on, 07:00–20:00 UTC | Extra gate. Turn off with `BOT_SESSION_FILTER=false` if you are testing a strategy whose hours sit outside that. |
| News filter | off | `BOT_NEWS_FILTER=true` skips a symbol when ForexFactory shows a high-impact event for either currency inside ±`BOT_NEWS_WINDOW_MIN`. A feed error skips new entries for that pass. |
| Grades | `alert` | Only rows that match the scanner's default Discord grade (and session) gate. `all`, or a list like `A,B`, overrides it. |
| Kill switch | off | `BOT_KILL_SWITCH=true` or a file at `service/bot/KILL_SWITCH`. New trades stop. Positions stay open unless `BOT_KILL_CLOSE=true`. |
| Logging | `service/bot/logs/trades.jsonl` | One JSON object per order attempt. Passwords are not written. |

The bot trades one new signal per symbol at a time and remembers the signal's id so a scanner that stays "on" does not re-enter the same setup after a restart (`service/bot/state.json`, gitignored).

## Tests

From `service/`:

```bash
python -m pytest test_bot_risk.py test_backtest_engine.py -q
```

Those cover position sizing (including a JPY pair and gold, via tick value), the daily-loss latch, position and trade caps, the kill switch, the live-account gate, the spread and news filters, and the backtest rule that a bar touching both stop and target is a loss.
