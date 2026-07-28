"""Docker healthcheck: exits 0 when the daemon's gRPC health endpoint is SERVING.

Usage: python -m meshcore_daemon.healthcheck
"""

import sys

import grpc
from grpc_health.v1 import health_pb2, health_pb2_grpc

from .config import Settings


def main() -> int:
    settings = Settings()
    target = f"127.0.0.1:{settings.grpc_port}"
    try:
        with grpc.insecure_channel(target) as channel:
            stub = health_pb2_grpc.HealthStub(channel)
            response = stub.Check(
                health_pb2.HealthCheckRequest(service=""), timeout=3.0
            )
    except grpc.RpcError as exc:
        print(f"healthcheck failed: {exc.code()}", file=sys.stderr)
        return 1
    if response.status == health_pb2.HealthCheckResponse.SERVING:
        return 0
    print(f"healthcheck: status={response.status}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
