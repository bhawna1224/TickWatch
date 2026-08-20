"""
Isolation Forest second-opinion anomaly check - V2 optional layer.
Pulls recent data from the RDB, engineers the same features the q layer
uses (return, log-size), fits an Isolation Forest per symbol, and compares
its flags against the q layer's own `anomalies` table for the same data.

Isolation Forest isolates points via random recursive splits; outliers get
isolated in far fewer splits than normal points, giving an anomaly score
with no assumption about the data's distribution. Unlike the q layer (which
scores price and size independently, one dimension at a time), this looks
at both together, so it can catch joint anomalies neither dimension shows
on its own.
"""
import pykx as kx
import pandas as pd
import numpy as np
import time
from datetime import datetime, timezone
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

RDB_PORT = 5011
ANOM_PORT = 5013
RECENT_MINUTES = 5


def fetch_table(port, name, recent_minutes=None):
    conn = kx.SyncQConnection(host='localhost', port=port)
    if recent_minutes is not None:
        q_expr = f"select from {name} where time > .z.p - 0D00:{recent_minutes:02d}:00"
        tbl = conn(q_expr)
    else:
        tbl = conn(name)
    df = tbl.pd()
    conn.close()
    return df


def engineer_features(df):
    df = df.sort_values(['sym', 'time']).reset_index(drop=True)
    df['ret'] = df.groupby('sym')['price'].pct_change()
    df['log_size'] = np.log(df['size'].clip(lower=1e-12))
    return df


def run_isolation_forest(df):
    results = []
    for sym, g in df.groupby('sym'):
        g = g.dropna(subset=['ret']).copy()
        if len(g) < 30:
            print(f"  {sym}: only {len(g)} usable rows, skipping (need >=30)")
            continue
        X_raw = g[['ret', 'log_size']].values
        X = StandardScaler().fit_transform(X_raw)  # put both features on comparable scale before splitting
        clf = IsolationForest(contamination=0.01, random_state=42)  # explicit rate - 'auto' uses a fixed offset (-0.5), not a target percentage, and over-flagged badly on our clustered real data
        preds = clf.fit_predict(X)  # -1 = anomaly, 1 = normal
        g['if_flag'] = preds == -1
        n_flagged = int((preds == -1).sum())
        print(f"  {sym}: {len(g)} rows, Isolation Forest flagged {n_flagged}")
        results.append(g)
    return pd.concat(results, ignore_index=True) if results else pd.DataFrame()


def main():
    print("Fetching recent trade data from RDB (port 5011)...")
    trade_df = fetch_table(RDB_PORT, 'trade', recent_minutes=RECENT_MINUTES)
    print(f"  {len(trade_df)} total rows\n")

    print("Fetching q layer's anomalies table (port 5013)...")
    q_anom_df = fetch_table(ANOM_PORT, 'anomalies')
    print(f"  q layer flagged {len(q_anom_df)} rows\n")

    print("Engineering features (return, log-size)...")
    feat_df = engineer_features(trade_df)

    print("Running Isolation Forest per symbol...")
    if_df = run_isolation_forest(feat_df)
    if_flagged = if_df[if_df['if_flag']] if len(if_df) else if_df
    print(f"\nIsolation Forest flagged {len(if_flagged)} rows total.\n")

    q_keys = set(zip(q_anom_df['sym'], q_anom_df['time'])) if len(q_anom_df) else set()
    if_keys = set(zip(if_flagged['sym'], if_flagged['time'])) if len(if_flagged) else set()

    both = q_keys & if_keys
    only_q = q_keys - if_keys
    only_if = if_keys - q_keys

    print("--- Comparison ---")
    print(f"Flagged by BOTH q layer and Isolation Forest: {len(both)}")
    print(f"Flagged ONLY by q layer (IF considered normal): {len(only_q)}")
    print(f"Flagged ONLY by Isolation Forest (q layer missed): {len(only_if)}")

    if only_if:
        print("\nExamples Isolation Forest caught that the q layer missed:")
        mask = if_flagged.apply(lambda r: (r['sym'], r['time']) in only_if, axis=1)
        sample = if_flagged[mask].head(5)
        print(sample[['time', 'sym', 'price', 'size', 'ret', 'log_size']].to_string(index=False))


def run_scheduled(interval_seconds):
    """Run the comparison repeatedly, sleeping between runs, instead of once."""
    print(f"Running every {interval_seconds}s. Ctrl+C to stop.\n")
    try:
        while True:
            print(f"=== Run at {datetime.now(timezone.utc).isoformat()} ===")
            try:
                main()
            except Exception as e:
                print(f"Run failed: {e}")
            print(f"\nSleeping {interval_seconds}s...\n")
            time.sleep(interval_seconds)
    except KeyboardInterrupt:
        print("\nStopped by user.")


if __name__ == "__main__":
    import sys
    if "--once" in sys.argv:
        main()
    else:
        run_scheduled(interval_seconds=300)  # every 5 minutes by default
