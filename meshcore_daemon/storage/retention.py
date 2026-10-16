"""Periodic pruning / space reclamation so the DB stays manageable for years."""

import asyncio
import logging
import time

from ..config import Settings
from .database import Database
from .repositories import MessageStore, PacketStore

logger = logging.getLogger(__name__)


class RetentionService:
    def __init__(
        self,
        settings: Settings,
        db: Database,
        packets: PacketStore,
        messages: MessageStore,
    ):
        self._settings = settings
        self._db = db
        self._packets = packets
        self._messages = messages

    async def run(self) -> None:
        interval = max(self._settings.retention_interval_minutes, 1) * 60
        while True:
            try:
                await self._sweep()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("retention sweep failed")
            await asyncio.sleep(interval)

    async def _sweep(self) -> None:
        now = int(time.time())
        s = self._settings

        if s.packet_retention_days > 0:
            cutoff = now - s.packet_retention_days * 86400
            deleted = await self._packets.prune(cutoff)
            if deleted:
                logger.info("retention: pruned %d raw packet rows", deleted)

        if s.message_retention_days > 0:
            cutoff = now - s.message_retention_days * 86400
            deleted = await self._messages.prune(cutoff)
            if deleted:
                logger.info("retention: pruned %d message rows", deleted)

        await self._db.c.execute("PRAGMA incremental_vacuum(2000)")
        await self._db.c.execute("PRAGMA wal_checkpoint(PASSIVE)")
