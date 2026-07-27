"""Transparent MeshCore TCP proxy.

Lets any number of MeshCore clients (the phone app, mccli, other tools that
speak the companion TCP protocol) share the daemon's single node connection.

* Client -> proxy frames use the standard ``0x3C + len(LE16) + frame`` framing;
  proxy -> client frames use ``0x3E + len(LE16) + frame`` — exactly what a
  real node speaks, so clients cannot tell the difference.
* Commands from all clients (and the daemon itself) are serialized through
  the device command slot; response frames are streamed back only to the
  client that issued the command.
* Push frames (code >= 0x80: adverts, ACKs, messages-waiting, RF log data...)
  are broadcast to every connected client.
* Read-only responses (contact list, device info, battery...) are cached for
  a short TTL so several apps connecting at once don't hammer a low-power
  node with identical requests. Any mutating command flushes the cache.
"""

import asyncio
import logging
import time
from typing import Optional

from meshcore.packets import CommandType

from .config import Settings
from .device import PUSH_CODE_MIN, DeviceManager, DeviceUnavailable
from .storage import MessageStore

logger = logging.getLogger(__name__)

FRAME_TO_NODE = 0x3C
FRAME_FROM_NODE = 0x3E
MAX_FRAME_SIZE = 300  # same bound the firmware/library enforce

#: read-only commands whose full response frame sequence may be cached
CACHEABLE_COMMANDS = {
    CommandType.APP_START.value,
    CommandType.GET_CONTACTS.value,
    CommandType.DEVICE_QEURY.value,
    CommandType.GET_BATT_AND_STORAGE.value,
    CommandType.GET_CHANNEL.value,
}

#: commands that change node state observable through cacheable reads
MUTATING_COMMANDS = {
    CommandType.SET_ADVERT_NAME.value,
    CommandType.ADD_UPDATE_CONTACT.value,
    CommandType.SET_RADIO_PARAMS.value,
    CommandType.SET_RADIO_TX_POWER.value,
    CommandType.RESET_PATH.value,
    CommandType.SET_ADVERT_LATLON.value,
    CommandType.REMOVE_CONTACT.value,
    CommandType.IMPORT_CONTACT.value,
    CommandType.SET_TUNING_PARAMS.value,
    CommandType.IMPORT_PRIVATE_KEY.value,
    CommandType.SET_CHANNEL.value,
    CommandType.SET_DEVICE_PIN.value,
    CommandType.SET_OTHER_PARAMS.value,
    CommandType.SET_CUSTOM_VAR.value,
    CommandType.FACTORY_RESET.value,
    CommandType.SET_FLOOD_SCOPE.value,
    CommandType.SET_AUTOADD_CONFIG.value,
    CommandType.SET_PATH_HASH_MODE.value,
    CommandType.SET_DEFAULT_FLOOD_SCOPE.value,
    CommandType.REBOOT.value,
}


class FrameParser:
    """Incremental parser for ``start_byte + len(LE16) + payload`` framing."""

    def __init__(self, start_byte: int):
        self._start = start_byte
        self._buf = bytearray()

    def feed(self, data: bytes) -> list[bytes]:
        self._buf.extend(data)
        frames: list[bytes] = []
        while True:
            idx = self._buf.find(bytes([self._start]))
            if idx < 0:
                self._buf.clear()
                break
            if idx > 0:
                del self._buf[:idx]  # discard leading junk
            if len(self._buf) < 3:
                break
            size = int.from_bytes(self._buf[1:3], "little")
            if size == 0 or size > MAX_FRAME_SIZE:
                del self._buf[:1]  # bad header, resync
                continue
            if len(self._buf) < 3 + size:
                break
            frames.append(bytes(self._buf[3 : 3 + size]))
            del self._buf[: 3 + size]
        return frames


class ResponseCache:
    def __init__(self, ttl: float):
        self._ttl = ttl
        self._entries: dict[bytes, tuple[float, list[bytes]]] = {}

    def get(self, key: bytes) -> Optional[list[bytes]]:
        entry = self._entries.get(key)
        if entry is None:
            return None
        stored_at, frames = entry
        if time.monotonic() - stored_at > self._ttl:
            del self._entries[key]
            return None
        return frames

    def set(self, key: bytes, frames: list[bytes]) -> None:
        if self._ttl > 0:
            self._entries[key] = (time.monotonic(), frames)

    def clear(self) -> None:
        self._entries.clear()


class _Client:
    def __init__(self, writer: asyncio.StreamWriter):
        self.writer = writer
        self.parser = FrameParser(FRAME_TO_NODE)
        self.addr = writer.get_extra_info("peername")
        self._write_lock = asyncio.Lock()

    async def send_frame(self, frame: bytes) -> None:
        pkt = bytes([FRAME_FROM_NODE]) + len(frame).to_bytes(2, "little") + frame
        async with self._write_lock:
            self.writer.write(pkt)
            await self.writer.drain()


class MeshCoreProxy:
    def __init__(self, settings: Settings, device: DeviceManager, messages: MessageStore):
        self._settings = settings
        self._device = device
        self._messages = messages
        self._clients: set[_Client] = set()
        self._server: Optional[asyncio.base_events.Server] = None
        self.cache = ResponseCache(settings.proxy_cache_ttl)
        self.commands_proxied = 0
        self.cache_hits = 0

        device.add_frame_listener(self._on_device_frame)
        device.proxy_client_count = self.client_count

    # ------------------------------------------------------------------ lifecycle

    async def start(self) -> None:
        self._server = await asyncio.start_server(
            self._handle_client, self._settings.proxy_host, self._settings.proxy_port
        )
        logger.info(
            "proxy listening on %s:%d",
            self._settings.proxy_host,
            self._settings.proxy_port,
        )

    async def stop(self) -> None:
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None
        for client in list(self._clients):
            client.writer.close()
        self._clients.clear()

    def client_count(self) -> int:
        return len(self._clients)

    def client_addrs(self) -> list[str]:
        return [str(c.addr) for c in self._clients]

    # ------------------------------------------------------------------ device push fan-out

    async def _on_device_frame(self, frame: bytes) -> None:
        if not frame or frame[0] < PUSH_CODE_MIN:
            return
        for client in list(self._clients):
            try:
                await client.send_frame(frame)
            except Exception:
                logger.debug("push broadcast to %s failed", client.addr, exc_info=True)

    # ------------------------------------------------------------------ client handling

    async def _handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        client = _Client(writer)
        if len(self._clients) >= self._settings.proxy_max_clients:
            logger.warning("proxy client %s rejected: max clients reached", client.addr)
            writer.close()
            return
        self._clients.add(client)
        logger.info("proxy client connected: %s (%d total)", client.addr, len(self._clients))
        try:
            while True:
                data = await reader.read(4096)
                if not data:
                    break
                for frame in client.parser.feed(data):
                    await self._handle_command(client, frame)
        except (ConnectionResetError, asyncio.IncompleteReadError):
            pass
        except Exception:
            logger.exception("proxy client %s error", client.addr)
        finally:
            self._clients.discard(client)
            writer.close()
            logger.info("proxy client disconnected: %s (%d left)", client.addr, len(self._clients))
            if not self._clients:
                # daemon may now consume queued messages ('auto' fetch mode)
                self._device.poke_message_fetch()

    async def _handle_command(self, client: _Client, frame: bytes) -> None:
        if not frame:
            return
        self.commands_proxied += 1
        cmd = frame[0]

        await self._log_outgoing_message(frame)

        if cmd in CACHEABLE_COMMANDS:
            cached = self.cache.get(frame)
            if cached is not None:
                self.cache_hits += 1
                logger.debug("cache hit for cmd 0x%02x from %s", cmd, client.addr)
                for resp in cached:
                    await client.send_frame(resp)
                return

        try:
            frames = await self._device.raw_command(frame, on_frame=client.send_frame)
        except DeviceUnavailable:
            # a real node that is unreachable answers nothing; an ERROR frame
            # at least lets well-behaved clients fail fast
            await client.send_frame(bytes([0x01]))
            return
        except TimeoutError:
            logger.warning("proxied cmd 0x%02x from %s timed out", cmd, client.addr)
            return

        if cmd in MUTATING_COMMANDS:
            self.cache.clear()
        elif cmd in CACHEABLE_COMMANDS and frames:
            self.cache.set(frame, frames)

    async def _log_outgoing_message(self, frame: bytes) -> None:
        """Best-effort capture of messages sent by proxy clients."""
        try:
            if frame[0] == CommandType.SEND_TXT_MSG.value and len(frame) > 13:
                await self._messages.add(
                    direction="out",
                    scope="contact",
                    peer=frame[7:13].hex(),
                    txt_type=frame[1],
                    sender_timestamp=int.from_bytes(frame[3:7], "little"),
                    text=frame[13:].decode("utf-8", "ignore"),
                    origin="proxy",
                )
            elif frame[0] == CommandType.SEND_CHANNEL_TXT_MSG.value and len(frame) > 7:
                await self._messages.add(
                    direction="out",
                    scope="channel",
                    channel_idx=frame[2],
                    txt_type=frame[1],
                    sender_timestamp=int.from_bytes(frame[3:7], "little"),
                    text=frame[7:].decode("utf-8", "ignore"),
                    origin="proxy",
                )
        except Exception:
            logger.debug("failed to log outgoing proxy message", exc_info=True)
