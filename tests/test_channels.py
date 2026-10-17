import asyncio
import sqlite3
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, Mock

from Crypto.Cipher import AES
from Crypto.Hash import HMAC, SHA256
from meshcore import EventType
from meshcore.events import Event
from meshcore.packets import CommandType, PacketType

from meshwarden_daemon.channel_backfill import GRP_TXT, ChannelBackfill
from meshwarden_daemon.channels import ChannelTracker
from meshwarden_daemon.storage import ChannelStore, Database, MessageStore, PacketStore
from meshwarden_daemon.storage.database import MIGRATIONS, SCHEMA

SECRET_A = bytes(range(16))
SECRET_B = bytes(range(16, 32))


def grp_txt_packet(secret: bytes, sender_timestamp: int, text: str) -> str:
    """Raw on-air GRP_TXT flood packet, as stored in packets.payload_hex."""
    plain = sender_timestamp.to_bytes(4, "little") + b"\0" + text.encode()
    plain += b"\0" * (-len(plain) % 16)
    cipher = AES.new(secret, AES.MODE_ECB).encrypt(plain)
    mac = HMAC.new(secret, cipher, digestmod=SHA256).digest()[:2]
    header = bytes([0x01 | (GRP_TXT << 2), 0x00])  # FLOOD, no hops
    return (header + SHA256.new(secret).digest()[:1] + mac + cipher).hex()


class ChannelAttributionTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.path = Path(self._tmp.name) / "test.db"
        self.db = Database(self.path)
        await self.db.open()
        self.channels = ChannelStore(self.db)
        self.messages = MessageStore(self.db)
        self.packets = PacketStore(self.db)
        self.backfill = ChannelBackfill(self.channels, self.messages, self.packets)

    async def asyncTearDown(self):
        await self.db.close()
        self._tmp.cleanup()

    async def log_packet(self, payload_hex: str, ts: int):
        await self.packets.log(
            ts=ts, kind="rx_log", snr=1.0, rssi=-90, route_type=1, payload_type=GRP_TXT,
            path_len=0, path=None, payload_len=len(payload_hex) // 2, payload_hex=payload_hex,
        )

    async def by_id(self) -> dict[int, dict]:
        rows, _ = await self.messages.list(limit=1000)
        return {r["id"]: r for r in rows}

    async def test_live_message_uses_channel_in_slot(self):
        a = await self.channels.assign_slot(0, "Public", SECRET_A)
        msg = await self.messages.add(direction="in", scope="channel", channel_idx=0, text="x: hi")
        unknown = await self.messages.add(direction="in", scope="channel", channel_idx=5, text="y")
        rows = await self.by_id()
        self.assertEqual((rows[msg]["channel_id"], rows[msg]["channel_source"]), (a, "slot"))
        self.assertEqual(rows[msg]["channel_name"], "Public")
        self.assertIsNone(rows[unknown]["channel_id"])

    async def test_assign_slot_moves_and_keeps_identity(self):
        a = await self.channels.assign_slot(0, "Public", SECRET_A)
        b = await self.channels.assign_slot(0, "Secret", SECRET_B)
        self.assertEqual(await self.channels.assign_slot(1, "Public", SECRET_A), a)
        slots = {c["slot"]: c["id"] for c in await self.channels.list(on_device=True)}
        self.assertEqual(slots, {0: b, 1: a})

    async def test_backfill_decrypts_infers_and_upgrades_with_extra_secret(self):
        now = int(time.time())
        retired = await self.messages.add(
            direction="in", scope="channel", channel_idx=0, sender_timestamp=now - 50, text="Bob: old"
        )
        no_packet = await self.messages.add(
            direction="in", scope="channel", channel_idx=0, sender_timestamp=now - 40, text="Bob: gone"
        )
        sent = await self.messages.add(direction="out", scope="channel", channel_idx=0, text="hello")
        orphan = await self.messages.add(direction="in", scope="channel", channel_idx=3, text="Eve: ?")
        current = await self.messages.add(
            direction="in", scope="channel", channel_idx=0, sender_timestamp=now - 30, text="Ann: new"
        )
        await self.log_packet(grp_txt_packet(SECRET_B, now - 50, "Bob: old"), now - 50)
        await self.log_packet(grp_txt_packet(SECRET_A, now - 5, "Me: hello"), now - 5)
        await self.log_packet(grp_txt_packet(SECRET_A, now - 30, "Ann: new"), now - 30)

        a = await self.channels.assign_slot(0, "Public", SECRET_A)
        result = await self.backfill.run()
        self.assertEqual((result.decrypted, result.inferred, result.unresolved), (2, 2, 1))
        rows = await self.by_id()
        self.assertEqual((rows[sent]["channel_id"], rows[sent]["channel_source"]), (a, "decrypted"))
        self.assertEqual((rows[current]["channel_id"], rows[current]["channel_source"]), (a, "decrypted"))
        self.assertEqual((rows[retired]["channel_id"], rows[retired]["channel_source"]), (a, "inferred"))
        self.assertEqual(rows[no_packet]["channel_source"], "inferred")
        self.assertIsNone(rows[orphan]["channel_id"])

        b = await self.channels.upsert("Retired", SECRET_B)
        result = await self.backfill.run(include_inferred=True)
        self.assertEqual((result.decrypted, result.inferred, result.unresolved), (1, 1, 1))
        rows = await self.by_id()
        self.assertEqual((rows[retired]["channel_id"], rows[retired]["channel_source"]), (b, "decrypted"))
        self.assertEqual(rows[retired]["channel_name"], "Retired")

    async def test_tracker_syncs_slots_from_device(self):
        slots = {
            0: Event(EventType.CHANNEL_INFO, {"channel_name": "Public", "channel_secret": SECRET_A}),
            1: Event(EventType.CHANNEL_INFO, {"channel_name": "", "channel_secret": bytes(16)}),
            2: Event(EventType.ERROR, {}),
        }

        async def command(name, *args):
            if name == "send_device_query":
                return Event(EventType.DEVICE_INFO, {"max_channels": 3})
            return slots[args[0]]

        device = Mock(command=AsyncMock(side_effect=command))
        tracker = ChannelTracker(device, self.channels, self.backfill)
        await self.channels.assign_slot(1, "Stale", SECRET_B)
        await self.channels.assign_slot(7, "Gone", bytes(range(32, 48)))

        on_device = await tracker.refresh()
        self.assertEqual([(c["slot"], c["name"]) for c in on_device], [(0, "Public")])

        slots[2] = Event(EventType.CHANNEL_INFO, {"channel_name": "Secret", "channel_secret": SECRET_B})
        tracker._on_command(bytes([CommandType.SET_CHANNEL.value, 2]), [bytes([PacketType.OK.value])])
        await asyncio.gather(*tracker._tasks)
        on_device = await self.channels.list(on_device=True)
        self.assertEqual([(c["slot"], c["name"]) for c in on_device], [(0, "Public"), (2, "Secret")])


class MigrationTest(unittest.IsolatedAsyncioTestCase):
    async def test_upgrades_v1_database(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "old.db"
            conn = sqlite3.connect(path)
            conn.executescript(SCHEMA)
            conn.execute(
                "INSERT INTO messages (ts, direction, scope, channel_idx, text, origin)"
                " VALUES (1, 'in', 'channel', 0, 'x', 'device')"
            )
            conn.commit()
            conn.close()

            for _ in range(2):  # second open must be a no-op
                db = Database(path)
                await db.open()
                rows, _ = await MessageStore(db).list()
                self.assertIsNone(rows[0]["channel_id"])
                cur = await db.c.execute("SELECT value FROM meta WHERE key = 'schema_version'")
                self.assertEqual((await cur.fetchone())[0], str(len(MIGRATIONS) + 1))
                await db.close()


if __name__ == "__main__":
    unittest.main()
