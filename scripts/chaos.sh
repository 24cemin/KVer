#!/usr/bin/env bash
# scripts/chaos.sh
# Cluster'ı rastgele node durdurma/başlatma ile stres testine tabi tutar.
#
# TODO: Hafta 8 — genişlet (network partition, disk yavaşlatma vb.)

set -euo pipefail

PORTS=(8081 8082 8083)
DURATION="${1:-60}"  # saniye
INTERVAL="${2:-10}"  # kaç saniyede bir chaos

echo "Chaos testi başlatıldı — $DURATION saniye, $INTERVAL saniyelik aralıklar"

END=$((SECONDS + DURATION))
while [[ $SECONDS -lt $END ]]; do
  sleep "$INTERVAL"

  # Rastgele bir node seç
  PORT="${PORTS[$RANDOM % ${#PORTS[@]}]}"
  echo "[chaos] Port $PORT kapatılıyor..."
  ./scripts/kill_node.sh "$PORT" || true

  sleep 5

  echo "[chaos] Port $PORT yeniden başlatılıyor..."
  # TODO: Hafta 8 — node'u otomatik yeniden başlat
  echo "  (TODO: node restart implement edilmedi)"
done

echo "Chaos testi tamamlandı."
