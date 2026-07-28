"""Daemon orchestrator: wires storage, device, recorder, proxy and gRPC together."""

import asyncio
import logging

from .config import Settings
from .device import DeviceManager
from .grpc_api import GrpcServer
from .grpc_api.service import MeshCoreDaemonService
from .logging_setup import setup_logging
from .proxy import MeshCoreProxy
from .recorder import EventRecorder
from .storage import ContactStore, Database, MessageStore, PacketStore, RetentionService

logger = logging.getLogger(__name__)


class Daemon:
    def __init__(self, settings: Settings | None = None):
        self.settings = settings or Settings()
        self._stop = asyncio.Event()

    def request_stop(self) -> None:
        self._stop.set()

    async def run(self) -> None:
        settings = self.settings
        setup_logging(settings)
        logger.info("meshcore-daemon starting")

        db = Database(settings.db_path)
        await db.open()
        packets = PacketStore(db, store_payloads=settings.store_packet_payloads)
        contacts = ContactStore(db)
        messages = MessageStore(db)
        retention = RetentionService(settings, db, packets, messages)

        device = DeviceManager(settings)
        EventRecorder(device, packets, contacts, messages).register()

        proxy: MeshCoreProxy | None = None
        if settings.proxy_enabled:
            proxy = MeshCoreProxy(settings, device, messages)

        service = MeshCoreDaemonService(settings, device, packets, contacts, messages, proxy)
        grpc_server = GrpcServer(settings, service)
        device.add_connection_listener(grpc_server.set_device_health)

        await grpc_server.start()
        if proxy is not None:
            await proxy.start()

        device_task = asyncio.create_task(device.run(), name="device-supervisor")
        retention_task = asyncio.create_task(retention.run(), name="retention")

        try:
            await self._stop.wait()
        finally:
            logger.info("meshcore-daemon shutting down")
            retention_task.cancel()
            await device.stop()
            device_task.cancel()
            for task in (device_task, retention_task):
                try:
                    await task
                except (asyncio.CancelledError, Exception):
                    pass
            if proxy is not None:
                await proxy.stop()
            await grpc_server.stop()
            await db.close()
            logger.info("meshcore-daemon stopped")
