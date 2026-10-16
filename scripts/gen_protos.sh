#!/usr/bin/env bash
# Regenerate gRPC stubs from protos/meshcored.proto into meshcore_daemon/grpc_api/pb.
set -euo pipefail
cd "$(dirname "$0")/.."

PY="${PYTHON:-.venv/bin/python}"
OUT=meshcore_daemon/grpc_api/pb

mkdir -p "$OUT"
"$PY" -m grpc_tools.protoc \
    -Iprotos \
    --python_out="$OUT" \
    --grpc_python_out="$OUT" \
    --pyi_out="$OUT" \
    protos/meshcored.proto

# Make the generated import package-relative.
sed -i.bak 's/^import meshcored_pb2 as/from . import meshcored_pb2 as/' "$OUT/meshcored_pb2_grpc.py"
rm -f "$OUT/meshcored_pb2_grpc.py.bak"

echo "generated stubs in $OUT"
