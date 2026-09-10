import asyncio
import json
import struct
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import grpc
from meshcore import EventType
from meshcore.events import Event
from meshcore.commands.messaging import MessagingCommands

from meshcore_daemon.device import DeviceUnavailable
from meshcore_daemon.ping import PING_TIMEOUT, PingManager, trace_route
from meshcore_daemon.grpc_api.service import MeshCoreDaemonService
from meshcore_daemon.grpc_api.pb import meshcored_pb2 as pb


KEY = "abcdef01" * 8


def contact(kind=2, path="", mode=0, key=KEY):
    return dict(public_key=key, type=kind, out_path=path,
                out_path_len=len(bytes.fromhex(path)) // (mode + 1),
                raw_json=json.dumps({"out_path_hash_mode": mode}))


def response(kwargs, *, tag=None, auth=None, flags=None, route=None):
    path = kwargs["path"] if route is None else route
    width = 1 << kwargs["flags"]
    return (bytes([0x89, 0, len(path), kwargs["flags"] if flags is None else flags])
            + struct.pack("<II", kwargs["tag"] if tag is None else tag,
                          kwargs["auth_code"] if auth is None else auth)
            + path + bytes([8] * (len(path) // width)) + b"\xfc")


class RouteTests(unittest.TestCase):
    def test_widths_and_round_trip(self):
        for width in (1, 2, 4):
            for kind in (2, 3):
                self.assertEqual(trace_route(contact(kind), [], width), bytes.fromhex(KEY)[:width])
        self.assertEqual(trace_route(contact(path="1122"), [], 1), bytes.fromhex("1122ab2211"))

    def test_widen_from_unique_full_keys_and_shorten(self):
        relay = contact(key="11223344" * 8)
        self.assertEqual(trace_route(contact(path="11"), [relay], 4),
                         bytes.fromhex("11223344abcdef0111223344"))
        self.assertEqual(trace_route(contact(path="1122", mode=1), [], 1), bytes.fromhex("11ab11"))
        for directory in ([], [relay, contact(key="11998877" * 8)]):
            with self.assertRaisesRegex(ValueError, "cannot expand"):
                trace_route(contact(path="11"), directory, 2)

    def test_invalid_type_width_unknown_malformed_and_long_routes(self):
        for c, width in [(contact(1), 1), (contact(4), 1), (contact(), 3),
                         ({**contact(), "out_path_len": -1}, 1),
                         ({**contact(), "out_path_len": 255}, 1),
                         ({**contact(path="11"), "raw_json": "{}"}, 1),
                         ({**contact(), "out_path_len": 1}, 1),
                         (contact(path="11" * 32), 1)]:
            with self.subTest(c=c, width=width), self.assertRaises(ValueError):
                trace_route(c, [], width)


class PingTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.device = Mock()
        self.device.command = AsyncMock(return_value=Event(EventType.MSG_SENT, {}))
        self.contacts = SimpleNamespace(list=AsyncMock(return_value=([contact()], 1)))
        self.manager = PingManager(self.device, self.contacts)

    async def test_installed_library_emits_verified_trace_wire_format(self):
        commands = MessagingCommands()
        commands.send = AsyncMock(return_value=Event(EventType.MSG_SENT, {}))
        for width, flags in ((1, 0), (2, 1), (4, 2)):
            route = trace_route(contact(), [], width)
            await commands.send_trace(tag=0x12345678, auth_code=0x90abcdef, flags=flags, path=route)
            self.assertEqual(commands.send.call_args.args[0],
                             b"\x24" + struct.pack("<II", 0x12345678, 0x90abcdef)
                             + bytes([flags]) + route)

    async def test_real_deadline_constant(self):
        self.assertEqual(PING_TIMEOUT, 5.0)
        start = asyncio.get_running_loop().time()
        # A command ACK alone must consume the real five-second budget and fail.
        with self.assertRaises(TimeoutError):
            await self.manager.ping(KEY, 1)
        elapsed = asyncio.get_running_loop().time() - start
        self.assertGreaterEqual(elapsed, 4.9)
        self.assertLess(elapsed, 5.3)
        self.assertFalse(self.manager.pending)

    async def test_matches_raw_response_even_before_command_ack(self):
        for width in (1, 2, 4):
            async def send(name, **kwargs):
                self.assertEqual(name, "send_trace")
                await self.manager._on_frame(response(kwargs))
                return Event(EventType.MSG_SENT, {})
            self.device.command.side_effect = send
            elapsed, snrs = await self.manager.ping(KEY, width)
            self.assertGreaterEqual(elapsed, 0)
            self.assertEqual(snrs, [2.0, -1.0])
            self.assertFalse(self.manager.pending)

    async def test_wrong_trace_and_ack_do_not_complete(self):
        async def send(name, **kwargs):
            await self.manager._on_frame(b"\x82" + b"\0" * 8)  # message ACK
            for frame in [response(kwargs, tag=kwargs["tag"] ^ 1),
                          response(kwargs, auth=kwargs["auth_code"] ^ 1),
                          response(kwargs, flags=1), response(kwargs, route=b"\x00"),
                          response(kwargs)[:-1]]:
                await self.manager._on_frame(frame)
            return Event(EventType.MSG_SENT, {})
        self.device.command.side_effect = send
        with patch("meshcore_daemon.ping.PING_TIMEOUT", 0.02), self.assertRaises(TimeoutError):
            await self.manager.ping(KEY, 1)
        self.assertFalse(self.manager.pending)

    async def test_one_deadline_includes_command_queue_and_cleanup(self):
        cancelled = asyncio.Event()
        async def queued(*args, **kwargs):
            try:
                await asyncio.Future()
            finally:
                cancelled.set()
        self.device.command.side_effect = queued
        start = asyncio.get_running_loop().time()
        with patch("meshcore_daemon.ping.PING_TIMEOUT", 0.02), self.assertRaises(TimeoutError):
            await self.manager.ping(KEY, 1)
        self.assertLess(asyncio.get_running_loop().time() - start, 0.2)
        self.assertTrue(cancelled.is_set())
        self.assertFalse(self.manager.pending)

    async def test_cancel_rejection_and_invalid_never_leak(self):
        self.device.command.return_value = Event(EventType.ERROR, {"reason": "busy"})
        with self.assertRaises(DeviceUnavailable):
            await self.manager.ping(KEY, 1)
        self.assertFalse(self.manager.pending)
        self.device.command.reset_mock()
        with self.assertRaises(ValueError):
            await self.manager.ping("bad-key", 1)
        self.device.command.assert_not_called()
        self.device.command.side_effect = asyncio.CancelledError()
        with self.assertRaises(asyncio.CancelledError):
            await self.manager.ping(KEY, 1)
        self.assertFalse(self.manager.pending)

    async def test_concurrent_probes_and_late_response(self):
        calls = []
        async def send(name, **kwargs):
            calls.append(kwargs)
            if len(calls) == 2:
                await self.manager._on_frame(response(calls[1]))
                await self.manager._on_frame(response(calls[0]))
            return Event(EventType.MSG_SENT, {})
        self.device.command.side_effect = send
        results = await asyncio.gather(self.manager.ping(KEY, 1), self.manager.ping(KEY, 1))
        self.assertEqual(len(results), 2)
        self.assertNotEqual(calls[0]["tag"], calls[1]["tag"])
        await self.manager._on_frame(response(calls[0]))
        self.assertFalse(self.manager.pending)

    async def test_rpc_defaults_and_errors_without_network(self):
        service = MeshCoreDaemonService(Mock(), self.device, Mock(), self.contacts, Mock(), None)
        service._pings.ping = AsyncMock(return_value=(12.5, [1.0]))
        ctx = Mock(abort=AsyncMock(side_effect=RuntimeError("aborted")))
        result = await service.Ping(pb.PingRequest(public_key=KEY), ctx)
        self.assertEqual(result.elapsed_ms, 12.5)
        service._pings.ping.assert_awaited_with(KEY, 1)
        for error, code in [(ValueError("bad"), grpc.StatusCode.INVALID_ARGUMENT),
                            (TimeoutError(), grpc.StatusCode.DEADLINE_EXCEEDED),
                            (DeviceUnavailable("offline"), grpc.StatusCode.UNAVAILABLE)]:
            service._pings.ping.side_effect = error
            with self.assertRaises(RuntimeError):
                await service.Ping(pb.PingRequest(public_key=KEY), ctx)
            self.assertEqual(ctx.abort.call_args.args[0], code)