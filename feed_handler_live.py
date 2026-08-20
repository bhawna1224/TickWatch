"""
Live feed handler - V2.
Connects to Binance's public WebSocket trade stream for BTC/ETH/SOL,
reformats each trade into (timestamp;ticker;price;volume), and publishes it
into the tickerplant over IPC - the same conn.upd() call the V1 simulated
feed handler used, so nothing downstream (tickerplant, RDB, HDB, anomaly
detector) needs to change at all.

No API key needed - this is Binance's public, unauthenticated market data stream.

Auto-reconnects with exponential backoff if the WebSocket connection drops
(network blip, exchange-side restart, etc.) instead of dying silently.
"""
import pykx as kx
import websocket
import json
import time
from datetime import datetime, timezone

SYMBOLS = ['btcusdt', 'ethusdt', 'solusdt']
STREAM_URL = "wss://stream.binance.com:9443/stream?streams=" + "/".join(f"{s}@trade" for s in SYMBOLS)

MIN_BACKOFF = 1       # seconds, first retry delay
MAX_BACKOFF = 60      # seconds, cap on retry delay
BACKOFF_FACTOR = 2    # doubles each consecutive failure

conn = kx.SyncQConnection(host='localhost', port=5010)


def to_ticker(binance_symbol: str) -> str:
    # BTCUSDT -> BTCUSD, matching our V1 test symbol convention
    return binance_symbol.upper().replace("USDT", "USD")


def on_message(ws, message):
    msg = json.loads(message)
    data = msg.get('data', msg)  # combined-stream mode wraps the payload under "data"
    if data.get('e') != 'trade':
        return
    sym = to_ticker(data['s'])
    price = float(data['p'])
    size = float(data['q'])
    trade_time = datetime.fromtimestamp(data['T'] / 1000, tz=timezone.utc)
    ts = kx.TimestampAtom(trade_time)
    conn.upd('trade', (ts, sym, price, size))
    print(f"LIVE {sym} price={price} size={size}")


def on_error(ws, error):
    print("WebSocket error:", error)


def on_close(ws, close_status_code, close_msg):
    print("WebSocket closed:", close_status_code, close_msg)


def on_open(ws):
    print("Connected to Binance live feed:", SYMBOLS)


def run_with_reconnect():
    backoff = MIN_BACKOFF
    while True:
        ws = websocket.WebSocketApp(
            STREAM_URL,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
        )
        connected_at = time.time()
        ws.run_forever()  # blocks until the connection drops for any reason

        # if we stayed connected a while, this was probably a real, healthy
        # session that just ended - reset backoff instead of penalizing it
        # for an unrelated earlier failure.
        if time.time() - connected_at > 30:
            backoff = MIN_BACKOFF

        print(f"Connection dropped. Reconnecting in {backoff}s...")
        time.sleep(backoff)
        backoff = min(backoff * BACKOFF_FACTOR, MAX_BACKOFF)


if __name__ == "__main__":
    try:
        run_with_reconnect()
    except KeyboardInterrupt:
        print("\nStopped by user.")
        conn.close()
