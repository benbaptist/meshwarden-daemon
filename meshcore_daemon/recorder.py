"""Glue between meshcore events and persistent storage.

Because the frame tap forwards *every* frame into the library reader, these
events fire regardless of who triggered them (daemon, gRPC caller, or a proxy
client), giving a single capture point for packets, messages and contacts.
"""

import logging
import time
from typing import Any

from meshcore import EventType
from meshcore.events import Event

from .device import DeviceManager
from .storage import ContactStore, MessageStore, PacketStore

logger = logging.getLogger(__name__)


class EventRecorder:
    def __init__(
        self,
        device: DeviceManager,
        packets: PacketStore,
        contacts: ContactStore,
        messages: MessageStore,
    ):
        self._device = device
        self._packets = packets
        self._contacts = contacts
        self._messages = messages

    def register(self) -> None:
        d = self._device
        d.on_event(EventType.RX_LOG_DATA, self._on_rx_log)
        d.on_event(EventType.RAW_DATA, self._on_raw_data)
        d.on_event(EventType.CONTACT_MSG_RECV, self._on_contact_msg)
        d.on_event(EventType.CHANNEL_MSG_RECV, self._on_channel_msg)
        d.on_event(EventType.CONTACTS, self._on_contacts)
        d.on_event(EventType.NEW_CONTACT, self._on_new_contact)
        d.on_event(EventType.ADVERTISEMENT, self._on_advert)
        d.on_event(EventType.CONTACT_DELETED, self._on_contact_deleted)
        d.on_event(EventType.ACK, self._on_ack)

    # ------------------------------------------------------------------ packets

    async def _on_rx_log(self, event: Event) -> None:
        p: dict[str, Any] = event.payload or {}
        payload_hex = p.get("payload")
        await self._packets.log(
            ts=int(p.get("recv_time") or time.time()),
            kind="rx_log",
            snr=p.get("snr"),
            rssi=p.get("rssi"),
            route_type=p.get("route_type"),
            payload_type=p.get("payload_type"),
            path_len=p.get("path_len"),
            path=p.get("path"),
            payload_len=p.get("payload_length") or (len(payload_hex) // 2 if payload_hex else 0),
            payload_hex=payload_hex,
        )

    async def _on_raw_data(self, event: Event) -> None:
        p: dict[str, Any] = event.payload or {}
        payload_hex = p.get("payload")
        await self._packets.log(
            ts=int(time.time()),
            kind="raw",
            snr=p.get("SNR"),
            rssi=p.get("RSSI"),
            route_type=None,
            payload_type=None,
            path_len=None,
            path=None,
            payload_len=len(payload_hex) // 2 if payload_hex else 0,
            payload_hex=payload_hex,
        )

    # ------------------------------------------------------------------ messages

    async def _on_contact_msg(self, event: Event) -> None:
        p: dict[str, Any] = event.payload or {}
        await self._messages.add(
            direction="in",
            scope="contact",
            peer=p.get("pubkey_prefix"),
            txt_type=p.get("txt_type"),
            sender_timestamp=p.get("sender_timestamp"),
            text=p.get("text", ""),
            snr=p.get("SNR"),
            path_len=p.get("path_len"),
            origin="device",
        )

    async def _on_channel_msg(self, event: Event) -> None:
        p: dict[str, Any] = event.payload or {}
        await self._messages.add(
            direction="in",
            scope="channel",
            channel_idx=p.get("channel_idx"),
            txt_type=p.get("txt_type"),
            sender_timestamp=p.get("sender_timestamp"),
            text=p.get("text", ""),
            snr=p.get("SNR"),
            path_len=p.get("path_len"),
            origin="device",
        )

    async def _on_ack(self, event: Event) -> None:
        code = (event.payload or {}).get("code")
        if code:
            await self._messages.mark_acked(code)

    # ------------------------------------------------------------------ contacts

    async def _on_contacts(self, event: Event) -> None:
        for contact in (event.payload or {}).values():
            await self._contacts.upsert(contact, on_device=True)

    async def _on_new_contact(self, event: Event) -> None:
        contact = event.payload or {}
        if contact.get("public_key"):
            await self._contacts.upsert(contact, on_device=True)

    async def _on_advert(self, event: Event) -> None:
        public_key = (event.payload or {}).get("public_key")
        if public_key:
            await self._contacts.touch(public_key)

    async def _on_contact_deleted(self, event: Event) -> None:
        public_key = (event.payload or {}).get("pubkey")
        if public_key:
            await self._contacts.mark_removed_from_device(public_key)
            logger.info("contact %s evicted from device (kept in DB)", public_key[:12])
