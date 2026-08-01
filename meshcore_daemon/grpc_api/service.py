"""gRPC service implementation."""

import asyncio
import json
import logging
import time
from enum import Enum
from typing import Any, AsyncIterator

import grpc
from meshcore import EventType
from meshcore.events import Event

from .. import __version__
from ..config import Settings
from ..device import DeviceManager, DeviceUnavailable
from ..proxy import MeshCoreProxy
from ..storage import ContactStore, MessageStore, PacketStore
from .pb import meshcored_pb2 as pb
from .pb import meshcored_pb2_grpc

logger = logging.getLogger(__name__)


def _json_safe(obj: Any) -> Any:
    if isinstance(obj, (bytes, bytearray)):
        return obj.hex()
    if isinstance(obj, Enum):
        return obj.value
    return str(obj)


def _dumps(payload: Any) -> str:
    return json.dumps(payload, default=_json_safe)


class MeshCoreDaemonService(meshcored_pb2_grpc.MeshCoreDaemonServicer):
    def __init__(
        self,
        settings: Settings,
        device: DeviceManager,
        packets: PacketStore,
        contacts: ContactStore,
        messages: MessageStore,
        proxy: MeshCoreProxy | None,
    ):
        self._settings = settings
        self._device = device
        self._packets = packets
        self._contacts = contacts
        self._messages = messages
        self._proxy = proxy
        self._event_streams: set[asyncio.Queue] = set()
        # single wildcard subscription feeds every StreamEvents client
        device.on_event(None, self._on_any_event)

    # ------------------------------------------------------------------ helpers

    async def _run(self, name: str, *args: Any, **kwargs: Any) -> tuple[Event | None, str]:
        """Run a library command; returns (event, error_string)."""
        try:
            event = await self._device.command(name, *args, **kwargs)
        except DeviceUnavailable as exc:
            return None, str(exc)
        except Exception as exc:
            logger.exception("command %s failed", name)
            return None, str(exc)
        if event.type == EventType.ERROR:
            return event, _dumps(event.payload)
        return event, ""

    async def _command_result(self, name: str, *args: Any, **kwargs: Any) -> pb.CommandResult:
        event, error = await self._run(name, *args, **kwargs)
        return pb.CommandResult(
            ok=not error,
            error=error,
            json=_dumps(event.payload) if event is not None and not error else "",
        )

    async def _json_response(self, name: str, *args: Any) -> pb.JsonResponse:
        event, error = await self._run(name, *args)
        if error:
            return pb.JsonResponse(json=_dumps({"error": error}))
        return pb.JsonResponse(json=_dumps(event.payload if event else {}))

    # ------------------------------------------------------------------ status

    async def GetStatus(self, request, context) -> pb.StatusResponse:
        mc = self._device.mc
        self_info = mc.self_info if mc is not None else {}
        return pb.StatusResponse(
            device_connected=self._device.is_connected,
            transport=self._settings.device_transport,
            device_name=self_info.get("name", ""),
            public_key=self_info.get("public_key", ""),
            connected_since=int(self._device.connected_since or 0),
            proxy_clients=self._proxy.client_count() if self._proxy else 0,
            packets_logged=await self._packets.count(),
            messages_stored=await self._messages.count(),
            contacts_stored=await self._contacts.count(),
            daemon_version=__version__,
        )

    async def GetDeviceInfo(self, request, context) -> pb.JsonResponse:
        return await self._json_response("send_device_query")

    async def GetSelfInfo(self, request, context) -> pb.JsonResponse:
        return await self._json_response("send_appstart")

    async def GetBattery(self, request, context) -> pb.JsonResponse:
        return await self._json_response("get_bat")

    async def GetDeviceStats(self, request, context) -> pb.JsonResponse:
        combined: dict[str, Any] = {}
        for name, key in (
            ("get_stats_core", "core"),
            ("get_stats_radio", "radio"),
            ("get_stats_packets", "packets"),
        ):
            event, error = await self._run(name)
            combined[key] = {"error": error} if error else (event.payload if event else {})
        return pb.JsonResponse(json=_dumps(combined))

    # ------------------------------------------------------------------ device configuration

    async def SetName(self, request, context) -> pb.CommandResult:
        return await self._command_result("set_name", request.name)

    async def SetRadio(self, request, context) -> pb.CommandResult:
        return await self._command_result(
            "set_radio", request.freq, request.bw, request.sf, request.cr
        )

    async def SetTxPower(self, request, context) -> pb.CommandResult:
        return await self._command_result("set_tx_power", request.tx_power)

    async def SetCoords(self, request, context) -> pb.CommandResult:
        return await self._command_result("set_coords", request.lat, request.lon)

    async def SetTime(self, request, context) -> pb.CommandResult:
        epoch = request.epoch_secs or int(time.time())
        return await self._command_result("set_time", epoch)

    async def SendAdvert(self, request, context) -> pb.CommandResult:
        return await self._command_result("send_advert", flood=request.flood)

    async def Reboot(self, request, context) -> pb.CommandResult:
        try:
            await self._device.raw_command(bytes([19]))  # CMD_REBOOT, no response
            return pb.CommandResult(ok=True)
        except (DeviceUnavailable, TimeoutError) as exc:
            return pb.CommandResult(ok=False, error=str(exc))

    # ------------------------------------------------------------------ contacts

    async def ListContacts(self, request, context) -> pb.ListContactsResponse:
        rows, total = await self._contacts.list(
            query=request.query,
            device_only=request.device_only,
            limit=request.limit or 100,
            offset=request.offset,
        )
        return pb.ListContactsResponse(
            total=total,
            contacts=[
                pb.Contact(
                    public_key=r["public_key"] or "",
                    adv_name=r["adv_name"] or "",
                    type=r["type"] or 0,
                    flags=r["flags"] or 0,
                    out_path_len=r["out_path_len"] if r["out_path_len"] is not None else -1,
                    out_path=r["out_path"] or "",
                    adv_lat=r["adv_lat"] or 0.0,
                    adv_lon=r["adv_lon"] or 0.0,
                    last_advert=r["last_advert"] or 0,
                    lastmod=r["lastmod"] or 0,
                    first_seen=r["first_seen"] or 0,
                    last_seen=r["last_seen"] or 0,
                    on_device=bool(r["on_device"]),
                )
                for r in rows
            ],
        )

    async def SyncContacts(self, request, context) -> pb.CommandResult:
        return await self._command_result("get_contacts")

    async def RemoveContact(self, request, context) -> pb.CommandResult:
        result = await self._command_result("remove_contact", request.public_key)
        if result.ok:
            await self._contacts.mark_removed_from_device(request.public_key)
        return result

    # ------------------------------------------------------------------ messaging

    async def SendMessage(self, request, context) -> pb.SendMessageResponse:
        event, error = await self._run("send_msg", request.destination, request.text)
        if error:
            return pb.SendMessageResponse(ok=False, error=error)
        expected_ack = ""
        if event is not None and isinstance(event.payload, dict):
            ack = event.payload.get("expected_ack")
            expected_ack = ack.hex() if isinstance(ack, (bytes, bytearray)) else str(ack or "")
        message_id = await self._messages.add(
            direction="out",
            scope="contact",
            peer=request.destination[:12],
            text=request.text,
            expected_ack=expected_ack or None,
            origin="daemon",
        )
        return pb.SendMessageResponse(ok=True, expected_ack=expected_ack, message_id=message_id)

    async def SendChannelMessage(self, request, context) -> pb.SendMessageResponse:
        event, error = await self._run("send_chan_msg", request.channel_idx, request.text)
        if error:
            return pb.SendMessageResponse(ok=False, error=error)
        message_id = await self._messages.add(
            direction="out",
            scope="channel",
            channel_idx=request.channel_idx,
            text=request.text,
            origin="daemon",
        )
        return pb.SendMessageResponse(ok=True, message_id=message_id)

    async def ListMessages(self, request, context) -> pb.ListMessagesResponse:
        rows, total = await self._messages.list(
            peer=request.peer,
            channel_idx=request.channel_idx if request.HasField("channel_idx") else None,
            direction=request.direction,
            after_ts=request.after_ts,
            before_ts=request.before_ts,
            limit=request.limit or 100,
            offset=request.offset,
        )
        return pb.ListMessagesResponse(
            total=total,
            messages=[
                pb.MessageRecord(
                    id=r["id"],
                    ts=r["ts"],
                    direction=r["direction"],
                    scope=r["scope"],
                    peer=r["peer"] or "",
                    channel_idx=r["channel_idx"] or 0,
                    txt_type=r["txt_type"] or 0,
                    sender_timestamp=r["sender_timestamp"] or 0,
                    text=r["text"],
                    snr=r["snr"] or 0.0,
                    expected_ack=r["expected_ack"] or "",
                    acked=bool(r["acked"]),
                    origin=r["origin"],
                )
                for r in rows
            ],
        )

    # ------------------------------------------------------------------ channels

    async def ListChannels(self, request, context) -> pb.ListChannelsResponse:
        channels: list[pb.ChannelInfo] = []
        for idx in range(8):  # MeshCore supports up to 8 channels
            try:
                event = await self._device.command("get_channel", idx)
            except DeviceUnavailable:
                break
            except Exception:
                continue
            if event.type == EventType.ERROR:
                break  # No more channels
            p = event.payload or {}
            channels.append(pb.ChannelInfo(
                index=p.get("channel_idx", idx),
                name=p.get("channel_name", ""),
            ))
        return pb.ListChannelsResponse(channels=channels)

    # ------------------------------------------------------------------ packets

    async def QueryPackets(self, request, context) -> pb.QueryPacketsResponse:
        rows, total = await self._packets.query(
            after_ts=request.after_ts,
            before_ts=request.before_ts,
            payload_type=request.payload_type if request.HasField("payload_type") else None,
            route_type=request.route_type if request.HasField("route_type") else None,
            limit=request.limit or 100,
            offset=request.offset,
        )
        return pb.QueryPacketsResponse(
            total=total,
            packets=[
                pb.PacketRecord(
                    id=r["id"],
                    ts=r["ts"],
                    kind=r["kind"],
                    snr=r["snr"] or 0.0,
                    rssi=r["rssi"] or 0,
                    route_type=r["route_type"] if r["route_type"] is not None else -1,
                    payload_type=r["payload_type"] if r["payload_type"] is not None else -1,
                    path_len=r["path_len"] if r["path_len"] is not None else -1,
                    path=r["path"] or "",
                    payload_len=r["payload_len"] or 0,
                    payload_hex=r["payload_hex"] or "",
                )
                for r in rows
            ],
        )

    async def GetPacketStats(self, request, context) -> pb.PacketStatsResponse:
        buckets = await self._packets.stats(
            after_ts=request.after_ts,
            before_ts=request.before_ts,
            bucket=request.bucket or "hour",
        )
        return pb.PacketStatsResponse(
            buckets=[
                pb.PacketStatsBucket(
                    bucket_ts=b["bucket_ts"],
                    payload_type=b["payload_type"],
                    route_type=b["route_type"],
                    count=b["count"],
                    total_bytes=b["total_bytes"],
                    avg_snr=b["avg_snr"] or 0.0,
                    avg_rssi=b["avg_rssi"] or 0.0,
                    min_snr=b["min_snr"] if b["min_snr"] is not None else 0.0,
                    max_snr=b["max_snr"] if b["max_snr"] is not None else 0.0,
                )
                for b in buckets
            ]
        )

    # ------------------------------------------------------------------ events

    async def _on_any_event(self, event: Event) -> None:
        if not self._event_streams:
            return
        item = pb.DaemonEvent(
            type=str(event.type.value),
            ts=int(time.time()),
            payload_json=_dumps(event.payload),
        )
        for queue in list(self._event_streams):
            if queue.qsize() < 1000:  # drop for slow consumers instead of ballooning
                queue.put_nowait(item)

    async def StreamEvents(self, request, context) -> AsyncIterator[pb.DaemonEvent]:
        wanted = set(request.types)
        queue: asyncio.Queue[pb.DaemonEvent] = asyncio.Queue()
        self._event_streams.add(queue)
        try:
            while True:
                item = await queue.get()
                if not wanted or item.type in wanted:
                    yield item
        finally:
            self._event_streams.discard(queue)

    # ------------------------------------------------------------------ raw / proxy

    async def SendRawCommand(self, request, context) -> pb.RawCommandResponse:
        try:
            frames = await self._device.raw_command(
                bytes(request.frame), timeout=request.timeout or None
            )
        except DeviceUnavailable as exc:
            await context.abort(grpc.StatusCode.UNAVAILABLE, str(exc))
        except TimeoutError as exc:
            await context.abort(grpc.StatusCode.DEADLINE_EXCEEDED, str(exc))
        except ValueError as exc:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(exc))
        return pb.RawCommandResponse(frames=frames)

    async def GetProxyStatus(self, request, context) -> pb.ProxyStatusResponse:
        if self._proxy is None:
            return pb.ProxyStatusResponse(enabled=False)
        return pb.ProxyStatusResponse(
            enabled=True,
            clients=self._proxy.client_count(),
            client_addrs=self._proxy.client_addrs(),
            commands_proxied=self._proxy.commands_proxied,
            cache_hits=self._proxy.cache_hits,
        )
