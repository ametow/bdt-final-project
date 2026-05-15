"""Idempotently create HBase tables via the Thrift gateway."""
import os
import sys
import time

import happybase

HOST = os.environ.get("HBASE_HOST", "hbase")
PORT = int(os.environ.get("HBASE_PORT", "9090"))

TABLES = {
    "trade_agg": {"cf": dict(max_versions=1)},
    "trade_latest": {"cf": dict(max_versions=1)},
}


def main():
    last_err = None
    for attempt in range(60):
        try:
            conn = happybase.Connection(HOST, port=PORT, autoconnect=True)
            existing = set(t.decode() for t in conn.tables())
            for name, families in TABLES.items():
                if name in existing:
                    print(f"[hbase-init] table '{name}' already exists")
                    continue
                print(f"[hbase-init] creating table '{name}'")
                conn.create_table(name, families)
            conn.close()
            print("[hbase-init] done")
            return 0
        except Exception as exc:
            last_err = exc
            print(f"[hbase-init] not ready ({exc}); retrying...")
            time.sleep(5)
    print(f"[hbase-init] FAILED after retries: {last_err}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
