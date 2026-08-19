"""
Live feed handler - V2.
Connects to Binance's public WebSocket trade stream for BTC/ETH/SOL,
reformats each trade into (timestamp;ticker;price;volume), and publishes it
into the tickerplant over IPC - the same conn.upd() call the V1 simulated
feed handler used, so nothing downstream (tickerplant, RDB, HDB, anomaly
detector) needs to change at all.

No API key needed - this is Binance's public, unauthenticated market data stream.
"""
import pykx as kx
import websocket
import json
from datetime import datetime, timezone

SYMBOLS = ['btcusdt', 'ethusdt', 'solusdt']
STREAM_URL = "wss://stream.binance.com:9443/stream?streams=" + "/".join(f"{s}@trade" for s in SYMBOLS)

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
    conn.close()

def on_open(ws):
    print("Connected to Binance live feed:", SYMBOLS)

if __name__ == "__main__":
    ws = websocket.WebSocketApp(
        STREAM_URL,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )
    ws.run_forever()
