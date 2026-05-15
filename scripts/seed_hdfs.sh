#!/usr/bin/env bash
# Wait for HDFS to leave safe mode then upload the static symbols.csv reference.
set -euo pipefail

export HADOOP_USER_NAME=root

echo "[seed] waiting for HDFS to leave safe mode..."
for i in {1..60}; do
  if hdfs dfsadmin -safemode get 2>/dev/null | grep -qi "OFF"; then
    break
  fi
  sleep 2
done

echo "[seed] preparing /seed directory in HDFS"
hdfs dfs -mkdir -p hdfs://namenode:9000/seed
hdfs dfs -put -f file:///seed/symbols.csv hdfs://namenode:9000/seed/symbols.csv
hdfs dfs -ls hdfs://namenode:9000/seed
echo "[seed] done"
