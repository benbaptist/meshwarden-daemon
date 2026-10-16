"""Zero-hop TRACE probes (not status requests or message acknowledgements).

Protocol: meshcore-dev/MeshCore src/Mesh.cpp (onRecvPacket/sendDirect),
examples/companion_radio/MyMesh.cpp (CMD_SEND_TRACE_PATH/onTraceRecv).
Trace flags encode hash width as 1 << (flags & 3), unlike normal routes.
"""

import asyncio
import secrets
import struct

from meshcore import EventType

from .device import DeviceUnavailable


PING_TIMEOUT = 5.0
TRACE_FLAGS = {1: 0, 2: 1, 4: 2}


def trace_route(contact: dict, width: int) -> bytes:
    # Zero-hop: the path is only the neighbour's own hash; stored routes are irrelevant.
    if width not in TRACE_FLAGS:
        raise ValueError("hash size must be 1, 2, or 4 bytes")
    if contact["type"] not in (2, 3):
        raise ValueError("ping requires a repeater or room server")
    key = bytes.fromhex(contact["public_key"])
    if len(key) != 32:
        raise ValueError("invalid contact public key")
    return key[:width]


class PingManager:
    def __init__(self, device, contacts):
        self.device = device
        self.contacts = contacts
        self.pending = {}
        device.add_frame_listener(self._on_frame)

    async def _on_frame(self, frame: bytes) -> None:
        # PUSH_TRACE: type,reserved,path_bytes,flags,tag,auth,hashes,SNRs,final_SNR.
        # Match the complete route as well as two fresh 32-bit correlation values.
        if len(frame) < 13 or frame[0] != 0x89:
            return
        tag, auth = struct.unpack_from("<II", frame, 4)
        pending = self.pending.get((tag, auth))
        if pending is None:
            return
        route, flags, future, deadline = pending
        received = asyncio.get_running_loop().time()
        count = len(route) // (1 << flags)
        if (frame[2] != len(route) or frame[3] != flags
                or len(frame) != 13 + len(route) + count
                or frame[12:12 + len(route)] != route or future.done() or received >= deadline):
            return
        snrs = [v / 4 for v in struct.unpack(f"{count + 1}b", frame[12 + len(route):])]
        future.set_result((received, snrs))

    async def ping(self, public_key: str, width: int):
        loop = asyncio.get_running_loop()
        started = loop.time()
        deadline = started + PING_TIMEOUT
        async with asyncio.timeout_at(deadline):
            if len(public_key) != 64 or len(bytes.fromhex(public_key)) != 32:
                raise ValueError("destination must be a full public key")
            public_key = public_key.lower()
            rows, _ = await self.contacts.list(query=public_key, limit=1)
            if not rows or rows[0]["public_key"] != public_key:
                raise ValueError("contact not found")
            route = trace_route(rows[0], width)
            flags = TRACE_FLAGS[width]
            token = (secrets.randbits(32), secrets.randbits(32))
            while token in self.pending:
                token = (secrets.randbits(32), secrets.randbits(32))
            future = loop.create_future()
            self.pending[token] = (route, flags, future, deadline)
            try:
                sent = await self.device.command("send_trace", tag=token[0], auth_code=token[1],
                                                 flags=flags, path=route)
                if sent.type != EventType.MSG_SENT:
                    raise DeviceUnavailable(f"trace command rejected: {sent.payload}")
                received, snrs = await future
                if loop.time() >= deadline:
                    raise TimeoutError
                return (received - started) * 1000, snrs
            finally:
                self.pending.pop(token, None)
                future.cancel()