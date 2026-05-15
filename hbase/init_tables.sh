#!/usr/bin/env bash
# Create HBase tables required by the pipeline (idempotent).
set -euo pipefail

echo "[hbase-init] creating tables on host=hbase"

hbase shell -n <<'EOF'
create_namespace 'bdt' rescue nil
list

# Drop+recreate is avoided to preserve data across restarts; create only if missing.
exists 'trade_agg'
exists 'trade_latest'
EOF

# Create tables if they don't exist (HBase shell lacks CREATE IF NOT EXISTS).
hbase shell -n <<'EOF' || true
create 'trade_agg', {NAME => 'cf', VERSIONS => 1}
EOF

hbase shell -n <<'EOF' || true
create 'trade_latest', {NAME => 'cf', VERSIONS => 1}
EOF

echo "[hbase-init] done"
hbase shell -n <<'EOF'
list
EOF
