# KVer — End-to-End Manual Test Guide

This document provides a step-by-step walkthrough for manually testing all features of the KVer distributed key-value store, including Raft consensus, data types, high availability, and dynamic cluster membership.

## 0. Setup and Clean Start

Make sure all data from previous runs is cleared and the system starts fresh.

```bash
cd /path/to/kver

# 1. Tear down any running containers and delete volumes
docker compose down -v

# 2. Rebuild Docker images with the latest code
docker compose build

# 3. Start the 3-node cluster in the background
docker compose up -d

# 4. Build the kvctl CLI tool
go build -o kvctl ./cmd/kvctl/

# 5. Set up a shorthand alias for the current terminal session
alias kv='./kvctl --nodes localhost:7001,localhost:7002,localhost:7003'
```

To verify that leader election completed successfully (press `CTRL+C` to exit):
```bash
docker compose logs -f 2>&1 | grep "became leader"
```

---

## 1. Cluster Status

Check the liveness (UP/DOWN) status of each node.

```bash
kv cluster status
```
> **Expected output:** Each node should show its role (Leader or Follower) and status (✓ UP). Leader detection is transparent via the Ping RPC.

---

## 2. String Operations and TTL

Basic key-value assignment, counters, and time-to-live expiration tests.

```bash
# Basic set and get
kv set foo bar
kv get foo
# Expected: bar

# Counter operations
kv set counter 10
kv incr counter
# Expected: 11
kv incr counter
# Expected: 12

kv decr counter
# Expected: 11

# TTL (time-to-live in seconds)
kv set tempkey myvalue --ttl 5
kv get tempkey
# Expected: myvalue
sleep 6
kv get tempkey
# Expected: Error: key not found
```

---

## 3. Hash Operations

Testing the Hash data type with nested fields.

```bash
# Add hash fields
kv hset user:1 name ali
kv hset user:1 age 25

# Read a specific field
kv hget user:1 name
# Expected: ali

# Read all fields
kv hgetall user:1
# Expected: name: ali, age: 25

# Check field existence
kv hexists user:1 name
# Expected: true

# Delete a field
kv hdel user:1 age
kv hexists user:1 age
# Expected: false
```

---

## 4. List Operations

Testing the list (queue) structure and range reads with negative index support (`-1`).

```bash
# Push elements to the list (left and right)
kv lpush queue task3
kv lpush queue task2
kv lpush queue task1
kv rpush queue task4

# Check list length
kv llen queue
# Expected: 4

# Read the full list (index 0 to -1)
kv lrange queue 0 -1
# Expected: task1, task2, task3, task4

# LPOP and RPOP
kv lpop queue
# Expected: task1
kv rpop queue
# Expected: task4

# Verify the removals
kv llen queue
# Expected: 2

kv lrange queue 0 -1
# Expected: task2, task3
```

--- 

## 5. Sorted Set Operations

Testing the sorted set structure ordered by score.

```bash
# Add elements with scores
kv zadd scores 100 ali
kv zadd scores 85 veli
kv zadd scores 95 ayse
kv zadd scores 70 mehmet

# Ascending range (lowest to highest)
kv zrange scores 0 -1
# Expected: mehmet, veli, ayse, ali

# Descending range (highest to lowest)
kv zrevrange scores 0 -1
# Expected: ali, ayse, veli, mehmet

# Query the score of a member
kv zscore scores ali
# Expected: 100

# Query the rank of a member (zero-based, highest score)
kv zrank scores ali
# Expected: 3

# Remove a member
kv zrem scores mehmet
kv zrange scores 0 -1
# Expected: veli, ayse, ali
```

---

## 6. Fault Tolerance & Catch-Up

Verifies that data is not lost when a server crashes (as long as quorum is maintained) and that the recovered node catches up on missed entries.

```bash
# Step 1: Stop node1
docker compose stop node1

# Step 2: Check cluster status (should respond within ~2 seconds)
kv cluster status
# Expected: localhost:7001 ✗ DOWN, others ✓ UP

# Step 3: Write data while node1 is down (quorum = 2/3 still satisfied)
./kvctl --nodes localhost:7002,localhost:7003 set failovertest success
./kvctl --nodes localhost:7002,localhost:7003 get failovertest
# Expected: success

# Step 4: Bring node1 back online
docker compose start node1
sleep 3

# Step 5: Verify node1 caught up and received the missed entry
kv get failovertest
# Expected: success
```

---

## 7. Dynamic Membership (Adding/Removing Nodes at Runtime)

Adding a new Node 4 (running on the host machine) to a live cluster.

### Important Note (Network Resolution)
Docker containers use internal hostnames (`node1`, `node2`, `node3`). For Node 4 running on the host to resolve these names, add a mapping to `/etc/hosts`:

**Run in Terminal 1:**
```bash
# May require sudo password
echo "127.0.0.1 node1 node2 node3" | sudo tee -a /etc/hosts
```

### Start Node 4 (Terminal 1)
Node 4 must be started with the current peer addresses so it can discover and be forwarded to the leader.

```bash
mkdir -p /tmp/kver-node4
rm -rf /tmp/kver-node4/*

go run ./cmd/server \
  --node-id node4 \
  --addr :7004 \
  --http-addr :8004 \
  --data-dir /tmp/kver-node4 \
  --peers "node1=127.0.0.1:7001,node2=127.0.0.1:7002,node3=127.0.0.1:7003"
```
*(Keep this terminal open and continue in a new terminal.)*

### Join Cluster and Test (Terminal 2)

```bash
# 1. Get the Docker bridge network gateway IP (usually 172.17.0.1 on Linux)
HOST_IP=$(ip -4 addr show docker0 | grep -oP '(?<=inet\s)\d+(\.\d+){3}')

# 2. Add node4 to the live cluster via the Raft protocol
./kvctl --nodes localhost:7001,localhost:7002,localhost:7003 cluster add-node node4 ${HOST_IP}:7004

# 3. Verify node4 appears in cluster status
./kvctl --nodes localhost:7001,localhost:7002,localhost:7003,localhost:7004 cluster status
# Expected: All 4 nodes ✓ UP

# 4. Test by connecting only to node4 (it will forward requests to the leader)
./kvctl --nodes localhost:7004 get foo
# Expected: bar

# 5. Remove node4 from the cluster
./kvctl --nodes localhost:7001,localhost:7002,localhost:7003 cluster remove-node node4
```
---

## 2b. Error Handling and Edge Cases

Testing system behavior under invalid or unexpected conditions.

```bash
# Read a non-existent key
kv get nonexistent_key
# Expected: Error: key not found

# Delete a key
kv set tobedeleted hello
kv del tobedeleted
kv get tobedeleted
# Expected: Error: key not found

# Overwrite a key
kv set mykey firstvalue
kv get mykey
# Expected: firstvalue
kv set mykey secondvalue
kv get mykey
# Expected: secondvalue (overwritten)

# Incr/Decr on a non-existent key (should start from 0)
kv del newcounter
kv incr newcounter
kv get newcounter
# Expected: 1
```

---

## 3b. Hash Edge Cases

```bash
# Update (overwrite) a hash field
kv hset product:1 price 100
kv hget product:1 price
# Expected: 100
kv hset product:1 price 150
kv hget product:1 price
# Expected: 150 (updated)

# Read from a non-existent hash key
kv hget nonexistent_hash nonexistent_field
# Expected: Error

# Hexists on a non-existent field
kv hexists user:1 nonexistent_field
# Expected: false
```

---

## 4b. List Edge Cases

```bash
# Partial range read
kv lpush mylist e d c b a
kv lrange mylist 0 -1
# Expected: a b c d e

kv lrange mylist 1 3
# Expected: b c d (index 1 to 3)

kv lrange mylist 0 0
# Expected: a (single element only)

# Empty list behavior
kv lpush templist x
kv lpop templist   # removes x
kv lpop templist   # list is already empty
# Expected: Second lpop may return an error or empty response
kv llen templist
# Expected: 0
```

---

## 5b. Sorted Set Edge Cases

```bash
# Partial range read
kv zadd myscores 10 one
kv zadd myscores 20 two
kv zadd myscores 30 three
kv zadd myscores 40 four

kv zrange myscores 1 2
# Expected: two three (index 1 and 2)

kv zrange myscores 0 0
# Expected: one (first element only)

# Score update (re-adding a member updates its score)
kv zadd myscores 99 one
kv zscore myscores one
# Expected: 99 (updated)

kv zrange myscores 0 -1
# Expected: two three four one (one now has the highest score)
```

---

## 6b. Leader Failover

Unlike Section 6 (follower crash), this scenario kills **the leader itself**. A new leader must be elected and the cluster must continue serving requests.

```bash
# Step 1: Identify the current leader (from logs or status output)
docker compose logs 2>&1 | grep "became leader" | tail -3

# Step 2: Kill the identified leader node
# (e.g., if node1 is the leader)
docker compose stop node1

# Step 3: Wait for the new leader election
sleep 3

# Step 4: Confirm a new leader was elected
docker compose logs 2>&1 | grep "became leader" | tail -3
# Expected: A new "became leader" log for node2 or node3

# Step 5: Write and read without node1
./kvctl --nodes localhost:7002,localhost:7003 set afterleaderfail works
./kvctl --nodes localhost:7002,localhost:7003 get afterleaderfail
# Expected: works

# Step 6: Bring the old leader back (it should rejoin as a follower)
docker compose start node1
sleep 3

# Step 7: Verify the old leader caught up
kv get afterleaderfail
# Expected: works
kv get foo
# Expected: bar
```

---

## 6c. Quorum Loss — Majority Failure (Expected Correct Behavior)

This test validates Raft's fundamental safety guarantee: if quorum cannot be achieved, the system **rejects writes** to preserve data integrity. This is correct behavior, not a bug.

```bash
# Write a known value first
kv set quorumtest beforefailure

# Stop two nodes (2 out of 3 = quorum loss)
docker compose stop node1
docker compose stop node2

# Cluster has lost majority
kv cluster status
# Expected: node1 and node2 ✗ DOWN, node3 ✓ UP but no leader can be elected

# Attempt a write — must be rejected (CORRECT behavior)
./kvctl --nodes localhost:7003 set quorumtest duringloss
# Expected: Error (no leader found / timeout)

# Attempt a read — may also fail (no leader known)
./kvctl --nodes localhost:7003 get quorumtest
# Expected: Error or previous value

# Restore the cluster
docker compose start node1
docker compose start node2
sleep 5

# Verify recovery
kv cluster status
# Expected: All ✓ UP

kv get quorumtest
# Expected: beforefailure (the write during quorum loss was NOT accepted)
```

---

## 6d. Data Persistence Test

Verifies that the WAL (Write-Ahead Log) and Snapshot mechanism correctly restore data after a full cluster restart.

```bash
# Step 1: Write data across all types
kv set persist_str "persistent value"
kv hset persist_hash name kerem
kv lpush persist_list x y z
kv zadd persist_zset 42 answer

# Step 2: Stop the cluster WITHOUT deleting volumes
docker compose stop
# Note: Do NOT use "docker compose down -v" — that deletes volumes!
# "stop" keeps the data on disk.

# Step 3: Restart the cluster
docker compose start
sleep 5

# Step 4: Confirm a new leader was elected
docker compose logs 2>&1 | grep "became leader" | tail -3

# Step 5: Verify all data was restored from disk
kv get persist_str
# Expected: persistent value

kv hget persist_hash name
# Expected: kerem

kv lrange persist_list 0 -1
# Expected: z y x (lpush reverses order)

kv zscore persist_zset answer
# Expected: 42
```

---


## 8. Network Partition / Split-Brain Simulation

This is the Jepsen-style split-brain test: "If the network splits, will my data get corrupted?" The answer is no — KVer's Raft implementation prevents split-brain. An automated script handles the entire scenario.

```bash
# Just run it and watch the output:
./scripts/simulate_partition.sh
```

The script automatically performs the following steps and prints colored logs:
1. Writes a clean value to `split_brain_test_1`.
2. Disconnects `node1` from the Docker network (`docker network disconnect`).
3. Attempts to write to the isolated `node1` and proves the operation **times out / is rejected** due to lost quorum. This demonstrates **split-brain protection** is working.
4. Waits for a new leader to be elected (Failover).
5. Writes new data to the remaining cluster (`split_brain_test_3`).
6. Reconnects `node1` to the network (`docker network connect`).
7. Shows that `node1` rejoins the cluster and synchronizes the missed entries.

Once all of these tests pass end-to-end, the KVer distributed system is fully operational. 🎉
