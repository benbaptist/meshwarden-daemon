"""gRPC server + standard health service."""

import asyncio
import logging

import grpc
from grpc_health.v1 import health_pb2, health_pb2_grpc
from grpc_health.v1.health import aio as health_aio

from ..config import Settings
from .pb import meshcored_pb2_grpc
from .service import MeshCoreDaemonService

logger = logging.getLogger(__name__)

DEVICE_SERVICE_NAME = "meshcored.v1.Device"


class GrpcServer:
    def __init__(self, settings: Settings, service: MeshCoreDaemonService):
        self._settings = settings
        self._service = service
        self._server: grpc.aio.Server | None = None
        self._health = health_aio.HealthServicer()

    async def start(self) -> None:
        server = grpc.aio.server()
        meshcored_pb2_grpc.add_MeshCoreDaemonServicer_to_server(self._service, server)
        health_pb2_grpc.add_HealthServicer_to_server(self._health, server)
        address = f"{self._settings.grpc_host}:{self._settings.grpc_port}"
        server.add_insecure_port(address)
        await server.start()
        self._server = server

        # overall health: SERVING once the daemon is up; device connectivity
        # is reported separately so orchestrators can distinguish the two
        await self._health.set("", health_pb2.HealthCheckResponse.SERVING)
        await self._health.set(
            DEVICE_SERVICE_NAME, health_pb2.HealthCheckResponse.NOT_SERVING
        )
        logger.info("gRPC listening on %s", address)

    def set_device_health(self, connected: bool) -> None:
        status = (
            health_pb2.HealthCheckResponse.SERVING
            if connected
            else health_pb2.HealthCheckResponse.NOT_SERVING
        )
        # HealthServicer.set is a coroutine on the aio variant
        asyncio.get_running_loop().create_task(
            self._health.set(DEVICE_SERVICE_NAME, status)
        )

    async def stop(self) -> None:
        if self._server is not None:
            await self._health.enter_graceful_shutdown()
            await self._server.stop(grace=3.0)
            self._server = None
