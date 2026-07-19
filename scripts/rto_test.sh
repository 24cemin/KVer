#!/bin/bash
cd /home/emin/myfiles/bitirme/kver
echo "Starting KVer cluster..."
docker-compose down -v > /dev/null 2>&1
docker-compose up -d > /dev/null 2>&1

echo "Waiting for cluster to stabilize and elect a leader (5 seconds)..."
sleep 5

# Find the leader
LEADER_PORT=""
LEADER_NODE=""
for i in 1 2 3; do
    PORT=$((8080+i))
    resp=$(curl -s -X POST http://localhost:$PORT/api/v1/string/set -H "Content-Type: application/json" -d '{"key":"test","value":"1"}')
    if echo "$resp" | grep -q "error"; then
        continue
    else
        LEADER_PORT=$PORT
        LEADER_NODE="kver_node${i}_1"
        break
    fi
done

if [ -z "$LEADER_PORT" ]; then
    echo "Could not find leader! Cluster might be down."
    docker-compose down -v > /dev/null 2>&1
    exit 1
fi

echo "Leader is $LEADER_NODE on port $LEADER_PORT"

# Kill the leader
echo "Killing $LEADER_NODE to trigger election..."
START_TIME=$(date +%s%N)
docker stop $LEADER_NODE > /dev/null

# Try to write until success
SUCCESS=0
while [ $SUCCESS -eq 0 ]; do
    for i in 1 2 3; do
        PORT=$((8080+i))
        if [ "$PORT" == "$LEADER_PORT" ]; then
            continue
        fi
        
        # Fast curl timeout
        resp=$(curl -s --max-time 1 -X POST http://localhost:$PORT/api/v1/string/set -H "Content-Type: application/json" -d '{"key":"rto","value":"test"}')
        if [ $? -eq 0 ] && ! echo "$resp" | grep -q "error"; then
            END_TIME=$(date +%s%N)
            SUCCESS=1
            echo "New leader elected! Responded on port $PORT"
            break
        fi
    done
    sleep 0.1
done

DOWNTIME=$((($END_TIME - $START_TIME)/1000000))
echo "Leader Crash RTO (Downtime): $DOWNTIME ms"

docker-compose down -v > /dev/null 2>&1
