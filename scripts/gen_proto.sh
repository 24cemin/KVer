#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

# Export paths for protoc and protoc-gen-go
export PATH="$REPO_ROOT/.bin/bin:$HOME/go/bin:/usr/local/go/bin:$PATH"

# Ensure output directories exist (required after a fresh clone)
mkdir -p "${REPO_ROOT}/proto/kv/gen"
mkdir -p "${REPO_ROOT}/proto/raft/gen"

echo "Generating kv proto..."
protoc \
  --proto_path="${REPO_ROOT}/proto/kv" \
  --go_out="${REPO_ROOT}/proto/kv/gen" \
  --go_opt=paths=source_relative \
  --go-grpc_out="${REPO_ROOT}/proto/kv/gen" \
  --go-grpc_opt=paths=source_relative \
  "${REPO_ROOT}/proto/kv/kv.proto"

echo "Generating raft proto..."
protoc \
  --proto_path="${REPO_ROOT}/proto/raft" \
  --go_out="${REPO_ROOT}/proto/raft/gen" \
  --go_opt=paths=source_relative \
  --go-grpc_out="${REPO_ROOT}/proto/raft/gen" \
  --go-grpc_opt=paths=source_relative \
  "${REPO_ROOT}/proto/raft/raft.proto"

echo "✓ Proto files generated."
