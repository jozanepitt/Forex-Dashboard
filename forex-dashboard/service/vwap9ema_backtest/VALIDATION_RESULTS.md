# VWAP+9EMA Validation Results

**0 / 48 combinations passed.**

## Acceptance criteria

A combination passes only if, on out-of-sample data: expectancy (R) > 0 AND profit factor >= 1.2 AND >= 150 trades AND positive in the majority of tested years.

## Data window

Backtest data covers **2025-04-15 to 2026-09-14** (exact start varies by a few weeks per symbol depending on when each symbol's history was fetched; end date is the fetch date).

This is a **reduced window**: the spec's originally planned range was `2021-01-01` to present (~5 years). The actual range above is what MT5 returned — the Exness terminal only retains ~100,000 M5 bars of history per symbol, discovered during Task 4. The user accepted this constraint rather than blocking on it. Practically, this means the 'positive in the majority of tested years' criterion only had **~2 partial calendar years** (2025 partial, 2026 partial) to work with, not the 5 full calendar years the original plan assumed — weaker evidence than originally intended, even though it did not end up being the deciding factor for any combination here.

## Near-miss combinations

Closest to passing: **USTECm/London/vp-climax** (OOS expR +0.043, PF 1.07) and **AUDUSDm/London/vp-climax** (OOS expR +0.017, PF 1.03) — both have positive out-of-sample expectancy but fail solely on profit factor (< 1.2).

## Results by combination

| Symbol | Session | Variant | OOS n | OOS expR | OOS PF | Result |
|---|---|---|---|---|---|---|
| USTECm | London | baseline | 944 | -0.151 | 0.78 | FAIL |
| USTECm | London | vp-filtered | 663 | -0.090 | 0.86 | FAIL |
| USTECm | London | vp-climax | 300 | +0.043 | 1.07 | FAIL |
| USTECm | NY | baseline | 690 | -0.119 | 0.82 | FAIL |
| USTECm | NY | vp-filtered | 495 | -0.022 | 0.96 | FAIL |
| USTECm | NY | vp-climax | 112 | -0.116 | 0.80 | FAIL |
| EURUSDm | London | baseline | 853 | -0.200 | 0.72 | FAIL |
| EURUSDm | London | vp-filtered | 604 | -0.060 | 0.91 | FAIL |
| EURUSDm | London | vp-climax | 253 | -0.008 | 0.99 | FAIL |
| EURUSDm | NY | baseline | 670 | -0.387 | 0.50 | FAIL |
| EURUSDm | NY | vp-filtered | 423 | -0.227 | 0.66 | FAIL |
| EURUSDm | NY | vp-climax | 152 | -0.004 | 0.99 | FAIL |
| GBPUSDm | London | baseline | 881 | -0.231 | 0.68 | FAIL |
| GBPUSDm | London | vp-filtered | 614 | -0.140 | 0.80 | FAIL |
| GBPUSDm | London | vp-climax | 227 | -0.029 | 0.95 | FAIL |
| GBPUSDm | NY | baseline | 630 | -0.331 | 0.54 | FAIL |
| GBPUSDm | NY | vp-filtered | 425 | -0.155 | 0.76 | FAIL |
| GBPUSDm | NY | vp-climax | 129 | -0.229 | 0.64 | FAIL |
| USDJPYm | London | baseline | 817 | -0.254 | 0.65 | FAIL |
| USDJPYm | London | vp-filtered | 620 | -0.156 | 0.77 | FAIL |
| USDJPYm | London | vp-climax | 254 | -0.043 | 0.94 | FAIL |
| USDJPYm | NY | baseline | 721 | -0.382 | 0.50 | FAIL |
| USDJPYm | NY | vp-filtered | 494 | -0.261 | 0.63 | FAIL |
| USDJPYm | NY | vp-climax | 203 | -0.237 | 0.65 | FAIL |
| USDCHFm | London | baseline | 788 | -0.332 | 0.57 | FAIL |
| USDCHFm | London | vp-filtered | 567 | -0.181 | 0.75 | FAIL |
| USDCHFm | London | vp-climax | 202 | -0.202 | 0.72 | FAIL |
| USDCHFm | NY | baseline | 582 | -0.367 | 0.52 | FAIL |
| USDCHFm | NY | vp-filtered | 403 | -0.243 | 0.64 | FAIL |
| USDCHFm | NY | vp-climax | 148 | -0.254 | 0.61 | FAIL |
| AUDUSDm | London | baseline | 831 | -0.271 | 0.64 | FAIL |
| AUDUSDm | London | vp-filtered | 616 | -0.170 | 0.76 | FAIL |
| AUDUSDm | London | vp-climax | 254 | +0.017 | 1.03 | FAIL |
| AUDUSDm | NY | baseline | 622 | -0.406 | 0.48 | FAIL |
| AUDUSDm | NY | vp-filtered | 414 | -0.264 | 0.62 | FAIL |
| AUDUSDm | NY | vp-climax | 129 | -0.204 | 0.68 | FAIL |
| USDCADm | London | baseline | 871 | -0.333 | 0.58 | FAIL |
| USDCADm | London | vp-filtered | 635 | -0.182 | 0.75 | FAIL |
| USDCADm | London | vp-climax | 253 | -0.028 | 0.96 | FAIL |
| USDCADm | NY | baseline | 618 | -0.447 | 0.44 | FAIL |
| USDCADm | NY | vp-filtered | 396 | -0.319 | 0.56 | FAIL |
| USDCADm | NY | vp-climax | 129 | -0.275 | 0.60 | FAIL |
| NZDUSDm | London | baseline | 786 | -0.428 | 0.48 | FAIL |
| NZDUSDm | London | vp-filtered | 537 | -0.339 | 0.57 | FAIL |
| NZDUSDm | London | vp-climax | 205 | -0.125 | 0.82 | FAIL |
| NZDUSDm | NY | baseline | 610 | -0.493 | 0.41 | FAIL |
| NZDUSDm | NY | vp-filtered | 370 | -0.270 | 0.62 | FAIL |
| NZDUSDm | NY | vp-climax | 108 | -0.309 | 0.54 | FAIL |

## Per-year breakdown (full-sample expectancy, from `year_stats`)

| Symbol | Session | Variant | 2025 | 2026 |
|---|---|---|---|---|
| USTECm | London | baseline | -0.070 (n=1237) | -0.117 (n=1151) |
| USTECm | London | vp-filtered | +0.021 (n=808) | -0.053 (n=824) |
| USTECm | London | vp-climax | +0.108 (n=379) | +0.061 (n=383) |
| USTECm | NY | baseline | -0.104 (n=871) | -0.125 (n=831) |
| USTECm | NY | vp-filtered | +0.029 (n=613) | -0.040 (n=601) |
| USTECm | NY | vp-climax | +0.054 (n=148) | -0.079 (n=130) |
| EURUSDm | London | baseline | -0.162 (n=1032) | -0.193 (n=1123) |
| EURUSDm | London | vp-filtered | -0.089 (n=738) | -0.078 (n=814) |
| EURUSDm | London | vp-climax | +0.075 (n=278) | -0.016 (n=334) |
| EURUSDm | NY | baseline | -0.371 (n=766) | -0.370 (n=845) |
| EURUSDm | NY | vp-filtered | -0.275 (n=535) | -0.228 (n=548) |
| EURUSDm | NY | vp-climax | -0.041 (n=162) | -0.034 (n=178) |
| GBPUSDm | London | baseline | -0.156 (n=1067) | -0.204 (n=1122) |
| GBPUSDm | London | vp-filtered | -0.068 (n=776) | -0.111 (n=790) |
| GBPUSDm | London | vp-climax | +0.086 (n=265) | -0.045 (n=301) |
| GBPUSDm | NY | baseline | -0.369 (n=764) | -0.280 (n=785) |
| GBPUSDm | NY | vp-filtered | -0.219 (n=518) | -0.125 (n=538) |
| GBPUSDm | NY | vp-climax | outlier† (n=165) | -0.128 (n=156) |
| USDJPYm | London | baseline | -0.155 (n=981) | -0.248 (n=1060) |
| USDJPYm | London | vp-filtered | -0.096 (n=698) | -0.153 (n=806) |
| USDJPYm | London | vp-climax | +0.024 (n=270) | -0.066 (n=331) |
| USDJPYm | NY | baseline | -0.372 (n=823) | -0.370 (n=909) |
| USDJPYm | NY | vp-filtered | -0.240 (n=566) | -0.248 (n=621) |
| USDJPYm | NY | vp-climax | -0.228 (n=187) | -0.272 (n=241) |
| USDCHFm | London | baseline | -0.268 (n=959) | -0.261 (n=1031) |
| USDCHFm | London | vp-filtered | -0.158 (n=685) | -0.117 (n=738) |
| USDCHFm | London | vp-climax | +0.063 (n=246) | -0.156 (n=272) |
| USDCHFm | NY | baseline | -0.475 (n=783) | -0.372 (n=756) |
| USDCHFm | NY | vp-filtered | -0.353 (n=533) | -0.230 (n=517) |
| USDCHFm | NY | vp-climax | -0.329 (n=170) | -0.266 (n=184) |
| AUDUSDm | London | baseline | -0.274 (n=954) | -0.239 (n=1062) |
| AUDUSDm | London | vp-filtered | -0.218 (n=703) | -0.159 (n=797) |
| AUDUSDm | London | vp-climax | +0.023 (n=300) | +0.032 (n=338) |
| AUDUSDm | NY | baseline | -0.441 (n=719) | -0.381 (n=774) |
| AUDUSDm | NY | vp-filtered | -0.286 (n=517) | -0.233 (n=524) |
| AUDUSDm | NY | vp-climax | -0.365 (n=137) | -0.186 (n=150) |
| USDCADm | London | baseline | -0.298 (n=948) | -0.312 (n=1121) |
| USDCADm | London | vp-filtered | -0.228 (n=703) | -0.171 (n=819) |
| USDCADm | London | vp-climax | -0.114 (n=318) | -0.064 (n=338) |
| USDCADm | NY | baseline | -0.412 (n=713) | -0.389 (n=772) |
| USDCADm | NY | vp-filtered | -0.268 (n=510) | -0.254 (n=494) |
| USDCADm | NY | vp-climax | -0.339 (n=169) | -0.267 (n=156) |
| NZDUSDm | London | baseline | -0.471 (n=903) | -0.413 (n=1011) |
| NZDUSDm | London | vp-filtered | -0.270 (n=603) | -0.314 (n=700) |
| NZDUSDm | London | vp-climax | -0.181 (n=244) | -0.175 (n=279) |
| NZDUSDm | NY | baseline | -0.719 (n=690) | -0.475 (n=764) |
| NZDUSDm | NY | vp-filtered | -0.523 (n=426) | -0.237 (n=465) |
| NZDUSDm | NY | vp-climax | -0.289 (n=126) | -0.334 (n=127) |

† This year's raw computed expectancy for this cell is an extreme outlier (multiple orders of magnitude outside any plausible R value) caused by a single trade whose computed stop distance rounds to ~0 (a floating-point edge case in `run_day()`'s risk calculation, not a bug in this report). It does not affect any out-of-sample number or pass/fail verdict above — the affected trade falls in the in-sample portion of the data. Flagged rather than fixed here because this report only changes how results are displayed, not how they are computed — a fix to the underlying risk calculation, if warranted, is a separate follow-up.

## Deferred scope

Full-sample win-rate, net-return, and max-drawdown, plus the parameter-sensitivity (EMA x R:R grid) and Monte Carlo drawdown analysis, were not captured for all 48 combinations in this automated sweep — this is a deliberate, known scope reduction (not an oversight); each is available per-combination by running `python backtest_mt5.py --symbol <SYM> --session <SESSION> --variant <VARIANT> --validate` individually.
