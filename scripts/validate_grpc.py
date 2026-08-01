#!/usr/bin/env python3
"""Validate all daemon gRPC endpoints against a running daemon.

Usage:
    MESHCORE_HOST=192.168.250.112 MESHCORE_PORT=50051 python scripts/validate_grpc.py
"""

from __future__ import annotations

import sys
import grpc
from meshcore_daemon.grpc_api.pb import meshcored_pb2 as pb
from meshcore_daemon.grpc_api.pb.meshcored_pb2_grpc import MeshCoreDaemonStub


def test(name: str, fn, *args) -> bool:
    try:
        result = fn(*args)
        if hasattr(result, "ok") and not result.ok:
            print(f"  ✗ {name}: ok=False  error={result.error}")
            return False
        print(f"  ✓ {name}")
        return True
    except grpc.RpcError as e:
        if e.code() == grpc.StatusCode.UNIMPLEMENTED:
            print(f"  ? {name}: UNIMPLEMENTED (daemon needs restart)")
            return None
        print(f"  ✗ {name}: {e.code().name} — {e.details()}")
        return False
    except Exception as e:
        print(f"  ✗ {name}: {e}")
        return False


def main():
    target = f"{sys.argv[1]}:{sys.argv[2]}" if len(sys.argv) >= 3 else "localhost:50051"
    print(f"Validating daemon at {target}…\n")

    channel = grpc.insecure_channel(target)
    stub = MeshCoreDaemonStub(channel)

    results: dict[str, bool | None] = {}

    # ── Status ──
    print("── Status ──")
    results["GetStatus"] = test("GetStatus", stub.GetStatus, pb.Empty())
    results["GetDeviceInfo"] = test("GetDeviceInfo", stub.GetDeviceInfo, pb.Empty())
    results["GetSelfInfo"] = test("GetSelfInfo", stub.GetSelfInfo, pb.Empty())
    results["GetBattery"] = test("GetBattery", stub.GetBattery, pb.Empty())
    results["GetDeviceStats"] = test("GetDeviceStats", stub.GetDeviceStats, pb.Empty())

    # ── Config ──
    print("\n── Config ──")
    results["SendAdvert"] = test("SendAdvert", stub.SendAdvert, pb.SendAdvertRequest(flood=False))
    results["GetProxyStatus"] = test("GetProxyStatus", stub.GetProxyStatus, pb.Empty())

    # ── Contacts ──
    print("\n── Contacts ──")
    results["ListContacts"] = test("ListContacts", stub.ListContacts, pb.ListContactsRequest(limit=5))
    results["SyncContacts"] = test("SyncContacts", stub.SyncContacts, pb.Empty())

    # ── Messaging ──
    print("\n── Messaging ──")
    results["ListMessages"] = test("ListMessages", stub.ListMessages, pb.ListMessagesRequest(limit=5))
    results["ListChannels"] = test("ListChannels", stub.ListChannels, pb.Empty())

    # ── Packets ──
    print("\n── Packets ──")
    results["QueryPackets"] = test("QueryPackets", stub.QueryPackets, pb.QueryPacketsRequest(limit=5))
    results["GetPacketStats"] = test("GetPacketStats", stub.GetPacketStats, pb.PacketStatsRequest(bucket="hour"))

    # ── Summary ──
    print()
    passed = sum(1 for v in results.values() if v is True)
    failed = sum(1 for v in results.values() if v is False)
    skipped = sum(1 for v in results.values() if v is None)
    total = len(results)
    print(f"Results: {passed} passed, {failed} failed, {skipped} unimplemented  ({total} total)")

    channel.close()
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
