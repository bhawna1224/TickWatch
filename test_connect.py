import pykx as kx

conn = kx.SyncQConnection(host='localhost', port=5010)

resp = conn.upd('trade', (kx.TimestampAtom('now'), 'PYTEST', 999.0, 1.0))
print("sent tick, response:", resp)

conn.close()
print("done")
