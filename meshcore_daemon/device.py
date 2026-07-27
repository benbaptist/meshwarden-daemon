"""Device connection management.

One physical MeshCore node, many consumers. This module owns:

* the single meshcore-library connection (USB serial or TCP),
* a *frame tap* that observes every de-framed companion-protocol frame the
  node emits (used for packet logging and the transparent proxy),
* a device-wide **command slot** (``asyncio.Lock``) that serializes every
  command sent to the node — whether it originates from daemon logic, the
  gRPC API or a proxy client — because the companion protocol correlates
  responses to commands purely by ordering,
* reconnect supervision, queued-message fetching and periodic contact sync.
"""

import asyncio
import logging
import time
from typing import Any, Awaitable, Callable, Optional

from meshcore import EventType, MeshCore
from meshcore.events import Event
from meshcore.packets import CommandType

from .config import Settings

logger = logging.getLogger(__name__)

#: response/push boundary of the companion protocol
PUSH_CODE_MIN = 0x80

_RESP_ERROR = 0x01
_RESP_CONTACT_START = 0x02
_RESP_CONTACT = 0x03
_RESP_CONTACT_END = 0x04

FrameListener = Callable[[bytes], Awaitable[None]]
EventCallback = Callable[[Event], Awaitable[None]]


class DeviceUnavailable(RuntimeError):
    """Raised when a command is attempted while the node is not connected."""


class _ReaderTap:
    """Sits between the connection and the library's MessageReader.

    Forwards every complete frame to the daemon *and* to the library, so
    library state (contacts, self info, events) stays consistent no matter
    who triggered a response.
    """

    def __init__(self, inner: Any, on_frame: FrameListener):
        self._inner = inner
        self._on_frame = on_frame

    async def handle_rx(self, data: bytes | bytearray) -> None:
        frame = bytes(data)
        try:
            await self._on_frame(frame)
        except Exception:
            logger.exception("frame tap listener failed")
        await self._inner.handle_rx(data)


class DeviceManager:
    def __init__(self, settings: Settings):
        self._settings = settings
        self.mc: Optional[MeshCore] = None
        self.command_lock = asyncio.Lock()

        self._frame_listeners: list[FrameListener] = []
        self._event_subs: list[tuple[Optional[EventType], EventCallback]] = []
        self._connection_listeners: list[Callable[[bool], None]] = []

        self._raw_queue: Optional[asyncio.Queue[bytes]] = None
        self._connected = asyncio.Event()
        self._conn_failed = asyncio.Event()
        self._stop = asyncio.Event()
        self._msgs_waiting = asyncio.Event()
        self._lastmod = 0
        self.connected_since: float | None = None

        #: set by the proxy so 'auto' message fetching can defer to live clients
        self.proxy_client_count: Callable[[], int] = lambda: 0

    # ------------------------------------------------------------------ wiring

    def add_frame_listener(self, listener: FrameListener) -> None:
        """Observe every frame received from the node (responses and pushes)."""
        self._frame_listeners.append(listener)

    def on_event(self, event_type: Optional[EventType], callback: EventCallback) -> None:
        """Subscribe to meshcore events; survives reconnects. None = all events."""
        self._event_subs.append((event_type, callback))
        if self.mc is not None:
            self.mc.subscribe(event_type, callback)

    def add_connection_listener(self, listener: Callable[[bool], None]) -> None:
        self._connection_listeners.append(listener)

    @property
    def is_connected(self) -> bool:
        return self.mc is not None and self._connected.is_set()

    def poke_message_fetch(self) -> None:
        """Hint that queued messages may be fetchable now (e.g. last proxy client left)."""
        self._msgs_waiting.set()

    # ------------------------------------------------------------------ commands

    async def command(self, name: str, /, *args: Any, **kwargs: Any) -> Event:
        """Run a meshcore library command while holding the device command slot."""
        mc = self.mc
        if mc is None or not self._connected.is_set():
            raise DeviceUnavailable("device is not connected")
        fn = getattr(mc.commands, name)
        async with self.command_lock:
            return await fn(*args, **kwargs)

    async def raw_command(
        self,
        frame: bytes,
        on_frame: Optional[FrameListener] = None,
        timeout: Optional[float] = None,
    ) -> list[bytes]:
        """Send a raw companion-protocol command frame and collect its response frames.

        Holds the command slot for the duration so responses are unambiguously
        attributable. Multi-frame responses (contact list) are handled. Push
        frames (>= 0x80) are never routed here — they go to frame listeners.
        """
        if not frame:
            raise ValueError("empty frame")
        mc = self.mc
        if mc is None or not self._connected.is_set():
            raise DeviceUnavailable("device is not connected")
        timeout = timeout if timeout is not None else self._settings.command_timeout

        async with self.command_lock:
            queue: asyncio.Queue[bytes] = asyncio.Queue()
            self._raw_queue = queue
            frames: list[bytes] = []
            try:
                await mc.connection_manager.send(frame)
                if frame[0] == CommandType.REBOOT.value:
                    return frames  # no response expected
                deadline = time.monotonic() + timeout
                while True:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise TimeoutError(f"raw command 0x{frame[0]:02x} timed out")
                    resp = await asyncio.wait_for(queue.get(), remaining)
                    frames.append(resp)
                    if on_frame is not None:
                        await on_frame(resp)
                    if self._is_terminal(frame[0], resp[0]):
                        return frames
            finally:
                self._raw_queue = None

    @staticmethod
    def _is_terminal(cmd: int, resp_code: int) -> bool:
        if cmd == CommandType.GET_CONTACTS.value:
            # contact list streams: CONTACT_START, CONTACT..., CONTACT_END
            return resp_code not in (_RESP_CONTACT_START, _RESP_CONTACT)
        return True

    # ------------------------------------------------------------------ frames

    async def _dispatch_frame(self, frame: bytes) -> None:
        if frame and frame[0] < PUSH_CODE_MIN and self._raw_queue is not None:
            self._raw_queue.put_nowait(frame)
        for listener in self._frame_listeners:
            try:
                await listener(frame)
            except Exception:
                logger.exception("frame listener failed")

    # ------------------------------------------------------------------ lifecycle

    async def run(self) -> None:
        """Supervision loop: connect, watch, rebuild on failure."""
        backoff = 1.0
        pump = asyncio.create_task(self._message_pump(), name="message-pump")
        contact_sync = asyncio.create_task(self._contact_sync(), name="contact-sync")
        try:
            while not self._stop.is_set():
                mc = await self._connect_once()
                if mc is None:
                    logger.warning("connection failed, retrying in %.0fs", backoff)
                    try:
                        await asyncio.wait_for(self._stop.wait(), backoff)
                    except asyncio.TimeoutError:
                        pass
                    backoff = min(backoff * 2, self._settings.reconnect_backoff_max)
                    continue

                backoff = 1.0
                self.mc = mc
                self._conn_failed.clear()
                self._connected.set()
                self.connected_since = time.time()
                self._notify_connection(True)
                self._msgs_waiting.set()  # drain anything queued while we were away
                logger.info("device connected")

                stop_task = asyncio.create_task(self._stop.wait())
                fail_task = asyncio.create_task(self._conn_failed.wait())
                try:
                    await asyncio.wait(
                        {stop_task, fail_task}, return_when=asyncio.FIRST_COMPLETED
                    )
                finally:
                    stop_task.cancel()
                    fail_task.cancel()

                self._connected.clear()
                self.connected_since = None
                self._notify_connection(False)
                try:
                    await mc.disconnect()
                except Exception:
                    logger.debug("error during disconnect", exc_info=True)
                self.mc = None
                if not self._stop.is_set():
                    logger.warning("device connection lost, rebuilding")
        finally:
            pump.cancel()
            contact_sync.cancel()

    async def stop(self) -> None:
        self._stop.set()
        # nudge waiters so run() unwinds promptly
        self._msgs_waiting.set()

    async def _connect_once(self) -> Optional[MeshCore]:
        s = self._settings
        attempts = s.reconnect_max_attempts or 1_000_000
        try:
            if s.device_transport == "tcp":
                logger.info("connecting to node via TCP %s:%d", s.device_tcp_host, s.device_tcp_port)
                mc = await MeshCore.create_tcp(
                    s.device_tcp_host,
                    s.device_tcp_port,
                    default_timeout=s.command_timeout,
                    auto_reconnect=True,
                    max_reconnect_attempts=attempts,
                )
            else:
                logger.info("connecting to node via serial %s @ %d", s.serial_port, s.serial_baud)
                mc = await MeshCore.create_serial(
                    s.serial_port,
                    s.serial_baud,
                    default_timeout=s.command_timeout,
                    auto_reconnect=True,
                    max_reconnect_attempts=attempts,
                )
        except Exception as exc:
            logger.error("connect failed: %s", exc)
            return None
        if mc is None:
            return None

        # Install the frame tap between the transport and the library reader.
        # (mc._reader is the library's MessageReader; there is no public
        # accessor, but the reader contract is stable: handle_rx(bytes).)
        tap = _ReaderTap(mc._reader, self._dispatch_frame)
        mc.connection_manager.set_reader(tap)

        # Internal subscriptions
        mc.subscribe(EventType.DISCONNECTED, self._on_disconnected_event)
        mc.subscribe(EventType.MESSAGES_WAITING, self._on_messages_waiting)
        mc.subscribe(EventType.CONTACTS, self._on_contacts_event)

        # Re-attach externally registered subscriptions
        for event_type, callback in self._event_subs:
            mc.subscribe(event_type, callback)
        return mc

    def _notify_connection(self, connected: bool) -> None:
        for listener in self._connection_listeners:
            try:
                listener(connected)
            except Exception:
                logger.exception("connection listener failed")

    async def _on_disconnected_event(self, event: Event) -> None:
        payload = event.payload or {}
        if payload.get("max_attempts_exceeded") or payload.get("reconnect_failed"):
            self._conn_failed.set()

    async def _on_messages_waiting(self, _event: Event) -> None:
        self._msgs_waiting.set()

    async def _on_contacts_event(self, event: Event) -> None:
        lastmod = (event.attributes or {}).get("lastmod")
        if lastmod:
            self._lastmod = max(self._lastmod, int(lastmod))

    # ------------------------------------------------------------------ background loops

    def _fetch_allowed(self) -> bool:
        mode = self._settings.message_fetch_mode
        if mode == "always":
            return True
        if mode == "never":
            return False
        return self.proxy_client_count() == 0

    async def _message_pump(self) -> None:
        """Drain queued messages from the node when allowed to consume them.

        Storage happens in the event recorder via CONTACT_MSG_RECV /
        CHANNEL_MSG_RECV events, so this loop only *fetches*.
        """
        while True:
            await self._msgs_waiting.wait()
            self._msgs_waiting.clear()
            if self._stop.is_set():
                return
            if not self.is_connected or not self._fetch_allowed():
                continue
            try:
                while self.is_connected and self._fetch_allowed():
                    result = await self.command("get_msg")
                    if result.type in (EventType.NO_MORE_MSGS, EventType.ERROR):
                        break
                    await asyncio.sleep(0.1)  # be gentle with low-power nodes
            except DeviceUnavailable:
                pass
            except Exception:
                logger.exception("message pump iteration failed")

    async def _contact_sync(self) -> None:
        """Periodically pull contact deltas so the DB outgrows the device limit."""
        interval = max(self._settings.contact_sync_interval, 10.0)
        while True:
            try:
                await self._connected.wait()
                await self.command("get_contacts", self._lastmod)
            except asyncio.CancelledError:
                raise
            except DeviceUnavailable:
                pass
            except Exception:
                logger.exception("contact sync failed")
            await asyncio.sleep(interval)
