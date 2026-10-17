import asyncio
import unittest
from types import SimpleNamespace
from unittest.mock import Mock

from meshwarden_daemon.grpc_api.pb import meshcored_pb2 as pb
from meshwarden_daemon.grpc_api.service import MeshCoreDaemonService
from meshwarden_daemon.proxy import MeshCoreProxy


class ProxyShutdownTests(unittest.IsolatedAsyncioTestCase):
    async def test_stop_with_connected_client_is_prompt(self):
        settings = SimpleNamespace(
            proxy_host="127.0.0.1", proxy_port=0, proxy_cache_ttl=0, proxy_max_clients=8
        )
        proxy = MeshCoreProxy(settings, Mock(), Mock())
        await proxy.start()
        port = proxy._server.sockets[0].getsockname()[1]
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        for _ in range(50):
            if proxy.client_count():
                break
            await asyncio.sleep(0.01)
        self.assertEqual(proxy.client_count(), 1)

        await asyncio.wait_for(proxy.stop(), 2)

        self.assertEqual(await asyncio.wait_for(reader.read(), 2), b"")
        writer.close()


class EventStreamShutdownTests(unittest.IsolatedAsyncioTestCase):
    async def test_close_event_streams_ends_active_streams(self):
        service = MeshCoreDaemonService.__new__(MeshCoreDaemonService)
        service._event_streams = set()
        stream = service.StreamEvents(pb.StreamEventsRequest(), None)
        pending = asyncio.ensure_future(anext(stream))
        await asyncio.sleep(0)
        self.assertEqual(len(service._event_streams), 1)

        service.close_event_streams()

        with self.assertRaises(StopAsyncIteration):
            await asyncio.wait_for(pending, 2)
        self.assertEqual(service._event_streams, set())


if __name__ == "__main__":
    unittest.main()
