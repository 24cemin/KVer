#!/usr/bin/env bash
# scripts/start_cluster.sh
# Yerel makinede 3 node'luk bir kver cluster'ı başlatır.
#
# TODO: Hafta 4 — implement.
set -euo pipefail

BINARY="./bin/kver-server"
DATA_BASE="./data"

if [[ ! -f "$BINARY" ]]; then
  echo "Binary bulunamadı: $BINARY — önce 'make build' çalıştırın" >&2
  exit 1
fi

# Node 1
"$BINARY" \
  --node-id="node1" \
  --addr=":8081" \
  --data-dir="$DATA_BASE/node1" \
  --peers="node2=localhost:8082,node3=localhost:8083" \
  &

# Node 2
"$BINARY" \
  --node-id="node2" \
  --addr=":8082" \
  --data-dir="$DATA_BASE/node2" \
  --peers="node1=localhost:8081,node3=localhost:8083" \
  &

# Node 3
"$BINARY" \
  --node-id="node3" \
  --addr=":8083" \
  --data-dir="$DATA_BASE/node3" \
  --peers="node1=localhost:8081,node2=localhost:8082" \
  &

echo "3-node cluster başlatıldı. PID'leri: $(jobs -p)"
wait
