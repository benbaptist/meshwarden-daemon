import sqlite3
import tempfile
import time
import unittest
from pathlib import Path

from meshwarden_daemon.storage import ContactStore, Database
from meshwarden_daemon.storage.database import MIGRATIONS, SCHEMA

DAY = 86400


def contact(key: str, last_advert: int, lastmod: int) -> dict:
    return {"public_key": key, "adv_name": key, "type": 1,
            "last_advert": last_advert, "lastmod": lastmod}


class ContactLastSeenTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.path = Path(self._tmp.name) / "test.db"
        self.db = Database(self.path)
        await self.db.open()
        self.contacts = ContactStore(self.db)
        self.now = int(time.time())

    async def asyncTearDown(self):
        await self.db.close()
        self._tmp.cleanup()

    async def last_seen(self, key: str) -> int:
        rows, _ = await self.contacts.list(query=key)
        return rows[0]["last_seen"]

    async def test_sync_uses_device_reported_time_not_sync_time(self):
        old = self.now - 150 * DAY
        await self.contacts.upsert(contact("aa", old, old - 60))
        self.assertEqual(await self.last_seen("aa"), old)

    async def test_ignores_unsynced_and_future_clocks(self):
        advert = self.now - 10 * DAY
        boot_default = 1715770351  # companion RTC before time sync
        await self.contacts.upsert(contact("bb", advert, boot_default))
        self.assertEqual(await self.last_seen("bb"), advert)

        await self.contacts.upsert(contact("cc", self.now + 365 * DAY, advert))
        self.assertEqual(await self.last_seen("cc"), advert)

    async def test_live_observation_survives_later_sync(self):
        old = self.now - 30 * DAY
        await self.contacts.upsert(contact("dd", old, old), heard=True)
        self.assertGreaterEqual(await self.last_seen("dd"), self.now)
        await self.contacts.upsert(contact("dd", old, old))
        self.assertGreaterEqual(await self.last_seen("dd"), self.now)

    async def test_touch_marks_seen_now(self):
        old = self.now - 30 * DAY
        await self.contacts.upsert(contact("ee", old, old))
        await self.contacts.touch(contact("ee", 0, 0)["public_key"])
        self.assertGreaterEqual(await self.last_seen("ee"), self.now)


class ContactLastSeenMigrationTest(unittest.IsolatedAsyncioTestCase):
    async def test_v3_rebuilds_last_seen_from_device_times(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "test.db"
            now = int(time.time())
            sync_time = now - 600
            advert = now - 150 * DAY
            conn = sqlite3.connect(path)
            conn.executescript(SCHEMA)
            conn.executescript(MIGRATIONS[0])
            conn.execute("INSERT INTO meta VALUES ('schema_version', '2')")
            conn.executemany(
                "INSERT INTO contacts (public_key, last_advert, lastmod,"
                " first_seen, last_seen) VALUES (?, ?, ?, ?, ?)",
                [
                    ("dead", advert, 1715770351, sync_time, sync_time),
                    ("future", now + 365 * DAY, advert, sync_time, sync_time),
                    ("unknown", None, None, sync_time, sync_time),
                ],
            )
            conn.commit()
            conn.close()

            db = Database(path)
            await db.open()
            cur = await db.c.execute("SELECT public_key, last_seen FROM contacts")
            seen = {k: v for k, v in await cur.fetchall()}
            await db.close()

        self.assertEqual(seen, {"dead": advert, "future": advert, "unknown": 0})


if __name__ == "__main__":
    unittest.main()
