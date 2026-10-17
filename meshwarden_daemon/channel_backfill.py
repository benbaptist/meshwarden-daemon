"""Attribute stored channel messages to the channel secret they came from.

Exact attribution decrypts the raw GRP_TXT packets still kept in ``packets``
and matches them to messages; when no packet is available the message is
attributed to the channel currently occupying its device slot ('inferred').
"""

import asyncio
import logging
from collections import defaultdict
from dataclasses import dataclass

from Crypto.Cipher import AES
from Crypto.Hash import HMAC, SHA256
from meshcore.meshcore_parser import PAYLOAD_TYPENAMES, MeshcorePacketParser

from .storage import ChannelStore, MessageStore, PacketStore

logger = logging.getLogger(__name__)

GRP_TXT = PAYLOAD_TYPENAMES.index("GRP_TXT")
#: how far around unresolved messages to look for their packets (seconds)
_SCAN_BEFORE = 86400
_SCAN_AFTER = 3600
#: max receive-time distance for messages without a sender timestamp
_TS_TOLERANCE = 60


@dataclass
class BackfillResult:
    decrypted: int = 0
    inferred: int = 0
    unresolved: int = 0


@dataclass
class _Decrypted:
    channel_id: int
    ts: int
    sender_timestamp: int


def decrypt_group_text(body: bytes, secrets: dict[int, list[tuple[int, bytes]]]) -> tuple[int, int, str] | None:
    """Decrypt a GRP_TXT packet payload (chan_hash | mac | ciphertext).

    ``secrets`` maps channel hash byte -> [(channel_id, secret)]. Returns
    (channel_id, sender_timestamp, text) for the secret whose MAC verifies.
    """
    if len(body) < 3 + 16:
        return None
    mac, cipher = body[1:3], body[3:]
    if len(cipher) % 16:
        return None
    for channel_id, secret in secrets.get(body[0], ()):
        if HMAC.new(secret, cipher, digestmod=SHA256).digest()[:2] != mac:
            continue
        plain = AES.new(secret, AES.MODE_ECB).decrypt(cipher)
        text = plain[5:].strip(b"\0").decode("utf-8", "ignore")
        return channel_id, int.from_bytes(plain[:4], "little"), text
    return None


def _channel_hash(secret: bytes) -> int:
    return SHA256.new(secret).digest()[0]


class ChannelBackfill:
    def __init__(self, channels: ChannelStore, messages: MessageStore, packets: PacketStore):
        self._channels = channels
        self._messages = messages
        self._packets = packets
        self._lock = asyncio.Lock()

    async def run(self, *, include_inferred: bool = False) -> BackfillResult:
        async with self._lock:
            return await self._run(include_inferred)

    async def _run(self, include_inferred: bool) -> BackfillResult:
        result = BackfillResult()
        targets = await self._messages.channel_backfill_candidates(include_inferred=include_inferred)
        if not targets:
            return result

        channels = await self._channels.list()
        by_slot = {c["slot"]: c["id"] for c in channels if c["slot"] is not None}
        index = await self._decrypt_packets(
            channels,
            after_ts=min(m["ts"] for m in targets) - _SCAN_BEFORE,
            before_ts=max(m["ts"] for m in targets) + _SCAN_AFTER,
        )

        updates: list[tuple[int, str, int]] = []
        for m in targets:
            slot_channel = by_slot.get(m["channel_idx"])
            matched = {d.channel_id for d in index.get(m["text"].strip("\0"), ()) if self._same_send(m, d)}
            if slot_channel in matched or len(matched) == 1:
                channel_id = slot_channel if slot_channel in matched else matched.pop()
                updates.append((channel_id, "decrypted", m["id"]))
                result.decrypted += 1
            elif slot_channel is not None and not matched:
                updates.append((slot_channel, "inferred", m["id"]))
                result.inferred += 1
            else:
                result.unresolved += 1
        await self._messages.set_channels(updates)
        logger.info(
            "channel backfill: %d decrypted, %d inferred, %d unresolved",
            result.decrypted, result.inferred, result.unresolved,
        )
        return result

    async def _decrypt_packets(
        self, channels: list[dict], *, after_ts: int, before_ts: int
    ) -> dict[str, list[_Decrypted]]:
        """Index decrypted packets by message text.

        Plaintext is ``"<sender>: <text>"``; received messages store it whole,
        sent messages store only ``<text>``, so both forms are indexed.
        """
        secrets: dict[int, list[tuple[int, bytes]]] = defaultdict(list)
        for c in channels:
            secrets[_channel_hash(c["secret"])].append((c["id"], c["secret"]))
        parser = MeshcorePacketParser()
        index: dict[str, list[_Decrypted]] = defaultdict(list)
        async for ts, payload_hex in self._packets.iter_payloads(
            payload_type=GRP_TXT, after_ts=after_ts, before_ts=before_ts
        ):
            log = await parser.parsePacketPayload(bytes.fromhex(payload_hex), {})
            found = decrypt_group_text(log["pkt_payload"], secrets)
            if found is None:
                continue
            channel_id, sender_timestamp, text = found
            entry = _Decrypted(channel_id, ts, sender_timestamp)
            index[text].append(entry)
            _, sep, body = text.partition(": ")
            if sep:
                index[body].append(entry)
        return index

    @staticmethod
    def _same_send(message: dict, packet: _Decrypted) -> bool:
        if message["sender_timestamp"]:
            return message["sender_timestamp"] == packet.sender_timestamp
        return abs(message["ts"] - packet.ts) <= _TS_TOLERANCE
