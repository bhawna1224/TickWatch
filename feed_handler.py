"""
Simulated feed handler - V1 integration test.
Replays a full simulated day of fake tick data through the tickerplant.
NOT connected to any real exchange - this is step 6 (V1), not step 7 (V2 live feed).
"""
import pykx as kx
import random
import time
from datetime import datetime, timezone

random.seed(42)  # reproducible run - same anomalies land in the same place every time

conn = kx.SyncQConnection(host='localhost', port=5010)

SYMBOLS = ['BTCUSD', 'ETHUSD', 'SOLUSD']
base_price = {'BTCUSD': 50000.0, 'ETHUSD': 3000.0, 'SOLUSD': 150.0}
base_size  = {'BTCUSD': 1.0,     'ETHUSD': 5.0,    'SOLUSD': 20.0}

N_TICKS_PER_SYMBOL = 40

# (symbol, tick index within that symbol's sequence) -> (price multiplier, size multiplier)
# deliberately placed after tick 20 so the anomaly detector's warm-up window (minN=5) is long past
ANOMALIES = {
    ('BTCUSD', 30): (1.30, 1.0),   # price spike, +30%
    ('ETHUSD', 25): (1.0,  15.0),  # volume spike, 15x normal size
    ('SOLUSD', 35): (0.60, 1.0),   # price crash, -40%
}

sent = 0
injected = []

for sym in SYMBOLS:
    price = base_price[sym]
    for i in range(N_TICKS_PER_SYMBOL):
        price *= (1 + random.uniform(-0.002, 0.002))  # small random walk
        size = base_size[sym] * random.uniform(0.8, 1.2)

        if (sym, i) in ANOMALIES:
            pmult, smult = ANOMALIES[(sym, i)]
            price *= pmult
            size *= smult
            injected.append((sym, i, round(price, 2), round(size, 2)))
            print(f"INJECTING ANOMALY: {sym} tick {i}  price={price:.2f}  size={size:.2f}")

        conn.upd('trade', (kx.TimestampAtom('now'), sym, float(price), float(size)))
        sent += 1
        time.sleep(0.02)  # small delay so timestamps stay distinct and this is watchable, not instant

conn.close()

print(f"\nDone. Sent {sent} ticks across {len(SYMBOLS)} symbols.")
print(f"Injected {len(injected)} deliberate anomalies:")
for sym, i, p, v in injected:
    print(f"  {sym} (tick #{i}): price={p} size={v}")
