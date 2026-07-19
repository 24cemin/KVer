#!/usr/bin/env bash
# scripts/kill_node.sh
# Belirtilen node'u durdurur (NODE_ID veya PORT ile).
#
# Kullanım: ./scripts/kill_node.sh <port>
# Örnek:    ./scripts/kill_node.sh 8082

set -euo pipefail

PORT="${1:-}"
if [[ -z "$PORT" ]]; then
  echo "Kullanım: $0 <port>" >&2
  exit 1
fi

PID=$(lsof -ti :"$PORT" 2>/dev/null || true)
if [[ -z "$PID" ]]; then
  echo "Port $PORT üzerinde çalışan process bulunamadı." >&2
  exit 1
fi

echo "Port $PORT — PID $PID durduruluyor..."
kill "$PID"
echo "Durduruldu."
