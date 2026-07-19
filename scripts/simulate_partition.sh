#!/bin/bash

# Renk tanımlamaları
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}KVer Network Partition (Split-Brain) Simülasyonuna Hoş Geldiniz!${NC}"
echo "Bu test, kümedeki bir düğümün (node) ağ bağlantısını keserek sistemin"
echo "nasıl tutarlılığını koruduğunu ve yeni bir lider seçtiğini gösterir."
echo ""

# Ağın ve konteynerin isimlerini varsayılan olarak belirliyoruz.
NETWORK_NAME=$(docker network ls --format '{{.Name}}' | grep kver | head -n 1)
NODE1_CONTAINER=$(docker ps --format '{{.Names}}' | grep -E "node1" | head -n 1)

if [ -z "$NETWORK_NAME" ] || [ -z "$NODE1_CONTAINER" ]; then
    echo -e "${RED}Hata: KVer Docker kümesi çalışmıyor gibi görünüyor.${NC}"
    echo "Lütfen önce proje dizininde 'docker-compose up -d' komutunu çalıştırın."
    exit 1
fi

echo -e "${GREEN}[1] Sistemin Başlangıç Durumu:${NC}"
echo "Test verisi yazılıyor (Set key=split_brain_test_1)..."
curl -s -X POST http://localhost:8001/api/v1/string/set -H "Content-Type: application/json" -d '{"key":"split_brain_test_1","value":"initial_value"}'
echo -e "\nTest verisi okunuyor (Get key=split_brain_test_1)..."
curl -s "http://localhost:8001/api/v1/string/get?key=split_brain_test_1"
echo -e "\n\n"

echo -e "${YELLOW}[2] AĞ BÖLÜNMESİ (NETWORK PARTITION) BAŞLIYOR...${NC}"
echo "Node 1'in sanal ethernet kablosu çekiliyor..."
docker network disconnect $NETWORK_NAME $NODE1_CONTAINER
echo -e "${RED}Node 1 artık ağdan izole edildi! (Split-Brain simülasyonu devrede)${NC}\n"

echo -e "${GREEN}[3] Node 1 (İzole Düğüm) Testi:${NC}"
echo "Node 1'e veri yazılmaya çalışılıyor... (Quorum - Çoğunluk sağlanamayacağı için başarısız olmalı)"
# 3 saniye timeout koyuyoruz
curl -m 3 -s -X POST http://localhost:8001/api/v1/string/set -H "Content-Type: application/json" -d '{"key":"split_brain_test_2","value":"fail_value"}' || echo -e "\n-> ${RED}Beklendiği gibi ZAMAN AŞIMI (Timeout) veya HATA alındı. Çoğunluk (Quorum) yok!${NC}"
echo ""

echo -e "${YELLOW}Yeni liderin seçilmesi için 5 saniye bekleniyor...${NC}"
sleep 5

echo -e "${GREEN}[4] Kalan Küme (Node 2 ve Node 3) Testi:${NC}"
echo "Kümenin ayakta kalan çoğunluğu üzerinden (Node 2) yeni veri yazılıyor..."
curl -s -X POST http://localhost:8002/api/v1/string/set -H "Content-Type: application/json" -d '{"key":"split_brain_test_3","value":"success_value"}'
echo -e "\nNode 2'den yeni veri okunuyor..."
curl -s "http://localhost:8002/api/v1/string/get?key=split_brain_test_3"
echo -e "\n\n"

echo -e "${YELLOW}[5] AĞ BAĞLANTISI GERİ KURULUYOR (RECOVERY)...${NC}"
echo "Node 1'in kablosu tekrar takılıyor..."
docker network connect $NETWORK_NAME $NODE1_CONTAINER
echo -e "${GREEN}Node 1 ağa katıldı. Yeni lideri tanıyıp verilerini senkronize etmesi bekleniyor (Catch-up)...${NC}"
sleep 5

echo -e "${GREEN}[6] Tutarlılık (Consistency) Kontrolü:${NC}"
echo "Node 1'den, yokluğunda Node 2'ye yazılan veri okunuyor..."
curl -s "http://localhost:8001/api/v1/string/get?key=split_brain_test_3"
echo -e "\n\n-> Eğer yukarıdaki değer 'success_value' ise, Node 1 başarıyla senkronize olmuştur!"
echo ""
echo -e "${GREEN}SİMÜLASYON BAŞARIYLA TAMAMLANDI!${NC}"
