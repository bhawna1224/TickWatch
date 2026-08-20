# TickWatch

Real-time crypto tick data pipeline (kdb+/q, Python) with live Binance
ingestion and statistically-calibrated anomaly detection, cross-validated
against an Isolation Forest second-opinion layer. Built on kdb+/q's standard
industry architecture (tickerplant → RDB → HDB).

## Architecture

```
                    ┌─────────────────┐
 Binance WebSocket →│  Feed Handler    │
   (live BTC/ETH/   │   (Python)       │
    SOL trades)      └────────┬─────────┘
                               │ IPC (.u.upd)
                               ▼
                     ┌──────────────────┐
                     │   Tickerplant     │  logs every tick (crash recovery,
                     │      (q)          │  auto-rolls log file at midnight),
                     └──┬─────────┬──────┘  republishes to all subscribers
                        │         │
              (pub/sub) │         │ (pub/sub)
                        ▼         ▼
              ┌──────────────┐ ┌──────────────────┐
              │     RDB      │ │ Anomaly Detector  │
              │     (q)      │ │       (q)         │
              │ today's data │ │ rolling-window,   │
              │  in memory   │ │ percentile-rank   │
              └──────┬───────┘ │  flagging         │
                     │         └────────┬──────────┘
   scheduled EOD     │                  │
     (date-derived   │                  │ periodic batch pull (IPC)
      partitioning)  ▼                  ▼
              ┌──────────────┐ ┌──────────────────────┐
              │     HDB      │ │ Isolation Forest      │
              │ (q, on-disk) │ │ second opinion         │
              │ date-        │ │      (Python)          │
              │ partitioned  │ │ joint price+size check │
              │ archive      │ │ vs. q layer's flags    │
              └──────────────┘ └────────────────────────┘
```

Both q subscribers (RDB and Anomaly Detector) monitor their own connection
to the tickerplant and automatically reconnect and rebuild their state from
the tickerplant's log if it ever drops - see "Resilience" below.

## Components

| File | Role |
|---|---|
| `tickerplant.q` | Receives ticks over IPC, logs them (crash recovery), republishes to subscribers. Does no storage or analysis itself. Auto-rolls its log file to a new date when one arrives after midnight. |
| `rdb.q` | Subscribes to the tickerplant, replays the day's log on startup to catch up, then holds today's data live in memory. Monitors its connection and auto-reconnects/replays if the tickerplant restarts. Runs the scheduled EOD check. |
| `eod.q` | Loaded automatically by `rdb.q`. `.eod.run[]` archives the RDB's current data to the date-partitioned HDB (partition date derived from the data's own timestamps, not wall-clock time) and clears the RDB. Fires automatically once per day via a scheduled check, or can be triggered manually. |
| `anomaly.q` | Subscribes to the tickerplant. Maintains a rolling window per symbol and flags ticks whose price return or trade size falls in the extreme tail (top/bottom 0.5%) of that symbol's recent history. Monitors its connection and auto-reconnects, quietly rebuilding its rolling windows from the log without re-flagging old anomalies. See "Anomaly detection design" below. |
| `feed_handler.py` | **V1** — replays a simulated day of fake tick data (with deliberately injected anomalies) through the pipeline, for testing without touching a live exchange. |
| `feed_handler_live.py` | **V2** — connects to Binance's public WebSocket trade stream (BTC/ETH/SOL, no API key required) and publishes real trades into the tickerplant. Auto-reconnects with exponential backoff if the connection drops. |
| `isolation_forest_check.py` | **V2, optional** — periodically (every 5 minutes by default) pulls a recent window of RDB data, engineers the same features the q layer uses, fits an Isolation Forest per symbol, and reports where it agrees/disagrees with the q layer's flags. Pass `--once` for a single run instead of the schedule. |
| `test_connect.py` | Minimal one-tick IPC connectivity smoke test. |

## Setup

Requires kdb+/q (tested against KDB-X Community Edition) and Python 3 with:

```bash
pip install pykx websocket-client scikit-learn pandas --break-system-packages
```

## Running it

Each component runs in its own terminal, listening on its own port:

```bash
# Terminal 1 — tickerplant
q tickerplant.q -p 5010

# Terminal 2 — RDB (auto-loads EOD logic and its schedule)
q rdb.q -p 5011

# Terminal 3 — HDB (query-only, load on demand)
q -p 5012
q) \l hdb

# Terminal 4 — anomaly detector
q anomaly.q -p 5013
```

Then, to feed it data:

```bash
# simulated data (V1, safe to run anytime)
python3 feed_handler.py

# OR live Binance data (V2)
python3 feed_handler_live.py
```

EOD archiving happens automatically once per day. To trigger it manually
instead (e.g. for testing), in the RDB terminal:

```q
.eod.run[]
```

To run the Isolation Forest second opinion on a schedule (every 5 minutes):

```bash
python3 isolation_forest_check.py
```

Or once, immediately:

```bash
python3 isolation_forest_check.py --once
```

## Resilience

- **Tickerplant restarts:** RDB and the anomaly detector each run a health
  check every 5 seconds. If the tickerplant connection is found dead, they
  retry `hopen` until it succeeds, re-subscribe, and fully replay the
  tickerplant's log to rebuild their state exactly - rather than trust
  whatever partial state was in memory before the disconnect. The anomaly
  detector rebuilds its rolling windows quietly during this replay (no
  re-flagging of anomalies already handled before the disconnect).
- **Live feed disconnects:** `feed_handler_live.py` auto-reconnects to
  Binance with exponential backoff (1s, 2s, 4s... capped at 60s), resetting
  the backoff after any connection that stayed up more than 30s.
- **EOD archiving:** runs on a schedule (checked every 5 seconds, fires once
  per day shortly after midnight, guarded against firing twice the same
  day) rather than requiring a manual trigger.
- **Isolation Forest:** runs on a repeating schedule; a single failed run
  (e.g. RDB briefly unreachable) is logged and doesn't kill the schedule.

## Anomaly detection design

The q layer's scoring method went through several iterations while testing
against real live data, each addressing a genuine property of real tick data
that a naive approach misses:

1. **Returns, not raw price.** Raw price is a random walk (non-stationary),
   so scoring it directly causes a real jump to distort the rolling window
   for several ticks afterward, cascading into false positives around any
   genuine anomaly.
2. **Log(size), not raw size.** Real trade sizes are heavily right-skewed
   (many small trades, occasional large ones) — closer to log-normal than
   normal.
3. **Percentile rank, not z-score/modified z-score.** Any method that
   divides by a spread estimate (stddev, or MAD) breaks down on real tick
   data, which is full of exact and near-exact ties (repeated prices, common
   lot sizes) — the spread estimate collapses toward zero and blows the
   score up to a meaningless magnitude. Percentile rank ("where does this
   value sit among the last N observations") never divides by anything, so
   it can't degenerate this way, and its threshold is directly interpretable
   ("flag the most extreme 0.5%").
4. **Mid-rank tie handling.** A naive `count(x <= v)` rank formulation
   shoves every tied value toward the top of the distribution. The
   statistically standard fix splits ties evenly instead.
5. **Detrending.** A sustained short-term trend means almost every new tick
   sets a fresh local extreme just by continuing the trend, not because
   anything unusual happened. Subtracting a short moving average (the local
   momentum) before ranking isolates genuine surprises from ordinary drift.

The Isolation Forest layer required its own calibration: `contamination=
'auto'` in scikit-learn is a *fixed* offset, not a target percentage, and
over-flagged badly on this clustered data — an explicit contamination rate
is needed to get a meaningful comparison against the q layer.
