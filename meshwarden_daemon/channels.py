"""Keeps the ``channels`` table in sync with the node's channel slots.

Slots are re-read on every connect and after any ``SET_CHANNEL`` that passes
through the daemon (proxy clients, SendRawCommand). After the connect-time
refresh, unattributed channel messages are backfilled.
"""

import asyncio
import logging
from typing import Any

from meshcore import EventType
from meshcore.packets import CommandType, PacketType

from .channel_backfill import ChannelBackfill
from .device import DeviceManager, DeviceUnavailable
from .storage import ChannelStore

logger = logging.getLogger(__name__)


class ChannelTracker:
    def __init__(self, device: DeviceManager, channels: ChannelStore, backfill: ChannelBackfill):
        self._device = device
        self._channels = channels
        self._backfill = backfill
        self._lock = asyncio.Lock()
        self._tasks: set[asyncio.Task] = set()

    def register(self) -> None:
        self._device.add_connection_listener(self._on_connection)
        self._device.add_command_listener(self._on_command)

    async def refresh(self) -> list[dict[str, Any]]:
        """Re-read every slot from the node; returns the channels on the device."""
        async with self._lock:
            info = await self._device.command("send_device_query")
            if info.type == EventType.ERROR:
                raise RuntimeError(f"device query failed: {info.payload}")
            max_channels = (info.payload or {}).get("max_channels", 0)
            for slot in range(max_channels):
                await self._read_slot(slot)
            await self._channels.clear_slots_from(max_channels)
        return await self._channels.list(on_device=True)

    async def _read_slot(self, slot: int) -> None:
        event = await self._device.command("get_channel", slot)
        p = event.payload or {}
        if event.type == EventType.ERROR or (not p["channel_name"] and not any(p["channel_secret"])):
            await self._channels.clear_slot(slot)
        else:
            await self._channels.assign_slot(slot, p["channel_name"], p["channel_secret"])

    def _spawn(self, coro) -> None:
        task = asyncio.create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    def _on_connection(self, connected: bool) -> None:
        if connected:
            self._spawn(self._sync_and_backfill())

    def _on_command(self, frame: bytes, responses: list[bytes]) -> None:
        ok = bool(responses) and responses[-1][0] == PacketType.OK.value
        if frame[0] == CommandType.SET_CHANNEL.value and len(frame) > 1 and ok:
            self._spawn(self._resync_slot(frame[1]))

    async def _sync_and_backfill(self) -> None:
        try:
            channels = await self.refresh()
            logger.info("channel slots synced: %d in use", len(channels))
            await self._backfill.run()
        except DeviceUnavailable:
            logger.debug("device went away during channel sync")
        except Exception:
            logger.exception("channel sync failed")

    async def _resync_slot(self, slot: int) -> None:
        try:
            async with self._lock:
                await self._read_slot(slot)
        except DeviceUnavailable:
            pass
        except Exception:
            logger.exception("channel slot %d resync failed", slot)
