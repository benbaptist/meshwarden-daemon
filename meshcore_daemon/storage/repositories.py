"""Repositories: typed access to the SQLite tables."""

import json
import logging
import time
from typing import Any

from .database import Database

logger = logging.getLogger(__name__)


def _rows_to_dicts(rows) -> list[dict[str, Any]]:
    return [dict(r) for r in rows]


class PacketStore:
    """Raw packet log + always-on hourly aggregation."""

    def __init__(self, db: Database, store_payloads: bool = True):
        self._db = db
        self._store_payloads = store_payloads

    async def log(
        self,
        *,
        ts: int,
        kind: str,
        snr: float | None,
        rssi: int | None,
        route_type: int | None,
        payload_type: int | None,
        path_len: int | None,
        path: Any | None,
        payload_len: int,
        payload_hex: str | None,
    ) -> None:
        c = self._db.c
        await c.execute(
            "INSERT INTO packets (ts, kind, snr, rssi, route_type, payload_type,"
            " path_len, path, payload_len, payload_hex)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                ts,
                kind,
                snr,
                rssi,
                route_type,
                payload_type,
                path_len,
                json.dumps(path) if path is not None else None,
                payload_len,
                payload_hex if self._store_payloads else None,
            ),
        )
        bucket = ts - ts % 3600
        await c.execute(
            "INSERT INTO packet_stats_hourly"
            " (bucket, payload_type, route_type, count, total_bytes, sum_snr, sum_rssi, min_snr, max_snr)"
            " VALUES (?, ?, ?, 1, ?, ?, ?, ?, ?)"
            " ON CONFLICT(bucket, payload_type, route_type) DO UPDATE SET"
            "  count = count + 1,"
            "  total_bytes = total_bytes + excluded.total_bytes,"
            "  sum_snr = sum_snr + excluded.sum_snr,"
            "  sum_rssi = sum_rssi + excluded.sum_rssi,"
            "  min_snr = MIN(COALESCE(min_snr, excluded.min_snr), excluded.min_snr),"
            "  max_snr = MAX(COALESCE(max_snr, excluded.max_snr), excluded.max_snr)",
            (
                bucket,
                payload_type if payload_type is not None else -1,
                route_type if route_type is not None else -1,
                payload_len,
                snr or 0.0,
                float(rssi) if rssi is not None else 0.0,
                snr,
                snr,
            ),
        )

    async def query(
        self,
        *,
        after_ts: int = 0,
        before_ts: int = 0,
        payload_type: int | None = None,
        route_type: int | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> tuple[list[dict[str, Any]], int]:
        where, params = self._filters(after_ts, before_ts, payload_type, route_type)
        c = self._db.c
        cur = await c.execute(f"SELECT COUNT(*) FROM packets{where}", params)
        (total,) = await cur.fetchone()  # type: ignore[misc]
        cur = await c.execute(
            f"SELECT * FROM packets{where} ORDER BY ts DESC, id DESC LIMIT ? OFFSET ?",
            params + [min(max(limit, 1), 1000), offset],
        )
        return _rows_to_dicts(await cur.fetchall()), total

    @staticmethod
    def _filters(after_ts, before_ts, payload_type, route_type) -> tuple[str, list]:
        clauses, params = [], []
        if after_ts:
            clauses.append("ts >= ?")
            params.append(after_ts)
        if before_ts:
            clauses.append("ts < ?")
            params.append(before_ts)
        if payload_type is not None:
            clauses.append("payload_type = ?")
            params.append(payload_type)
        if route_type is not None:
            clauses.append("route_type = ?")
            params.append(route_type)
        return (" WHERE " + " AND ".join(clauses)) if clauses else "", params

    async def stats(
        self, *, after_ts: int = 0, before_ts: int = 0, bucket: str = "hour"
    ) -> list[dict[str, Any]]:
        clauses, params = [], []
        if after_ts:
            clauses.append("bucket >= ?")
            params.append(after_ts)
        if before_ts:
            clauses.append("bucket < ?")
            params.append(before_ts)
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        group_expr = "bucket - bucket % 86400" if bucket == "day" else "bucket"
        cur = await self._db.c.execute(
            f"SELECT {group_expr} AS bucket_ts, payload_type, route_type,"
            " SUM(count) AS count, SUM(total_bytes) AS total_bytes,"
            " SUM(sum_snr) / SUM(count) AS avg_snr,"
            " SUM(sum_rssi) / SUM(count) AS avg_rssi,"
            " MIN(min_snr) AS min_snr, MAX(max_snr) AS max_snr"
            f" FROM packet_stats_hourly{where}"
            " GROUP BY bucket_ts, payload_type, route_type"
            " ORDER BY bucket_ts",
            params,
        )
        return _rows_to_dicts(await cur.fetchall())

    async def count(self) -> int:
        cur = await self._db.c.execute("SELECT COUNT(*) FROM packets")
        (n,) = await cur.fetchone()  # type: ignore[misc]
        return n

    async def prune(self, cutoff_ts: int, batch: int = 5000) -> int:
        """Delete raw packets older than cutoff in batches. Aggregates are kept."""
        total = 0
        while True:
            cur = await self._db.c.execute(
                "DELETE FROM packets WHERE id IN"
                " (SELECT id FROM packets WHERE ts < ? LIMIT ?)",
                (cutoff_ts, batch),
            )
            total += cur.rowcount
            if cur.rowcount < batch:
                break
        return total


class ContactStore:
    """Contacts beyond the on-device limit, with first/last-seen tracking."""

    def __init__(self, db: Database):
        self._db = db

    async def upsert(self, contact: dict[str, Any], *, on_device: bool = True) -> None:
        now = int(time.time())
        await self._db.c.execute(
            "INSERT INTO contacts (public_key, adv_name, type, flags, out_path,"
            " out_path_len, adv_lat, adv_lon, last_advert, lastmod, first_seen,"
            " last_seen, on_device, raw_json)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
            " ON CONFLICT(public_key) DO UPDATE SET"
            "  adv_name = excluded.adv_name, type = excluded.type,"
            "  flags = excluded.flags, out_path = excluded.out_path,"
            "  out_path_len = excluded.out_path_len, adv_lat = excluded.adv_lat,"
            "  adv_lon = excluded.adv_lon, last_advert = excluded.last_advert,"
            "  lastmod = excluded.lastmod, last_seen = excluded.last_seen,"
            "  on_device = excluded.on_device, raw_json = excluded.raw_json",
            (
                contact.get("public_key"),
                contact.get("adv_name"),
                contact.get("type"),
                contact.get("flags"),
                contact.get("out_path"),
                contact.get("out_path_len"),
                contact.get("adv_lat"),
                contact.get("adv_lon"),
                contact.get("last_advert"),
                contact.get("lastmod"),
                now,
                now,
                1 if on_device else 0,
                json.dumps(contact, default=str),
            ),
        )

    async def touch(self, public_key: str) -> None:
        """Bump last_seen for a contact seen via advertisement."""
        await self._db.c.execute(
            "UPDATE contacts SET last_seen = ? WHERE public_key = ?",
            (int(time.time()), public_key),
        )

    async def mark_removed_from_device(self, public_key: str) -> None:
        await self._db.c.execute(
            "UPDATE contacts SET on_device = 0 WHERE public_key = ?", (public_key,)
        )

    async def list(
        self,
        *,
        query: str = "",
        device_only: bool = False,
        limit: int = 100,
        offset: int = 0,
    ) -> tuple[list[dict[str, Any]], int]:
        clauses, params = [], []
        if query:
            clauses.append("(adv_name LIKE ? OR public_key LIKE ?)")
            params += [f"%{query}%", f"{query}%"]
        if device_only:
            clauses.append("on_device = 1")
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        c = self._db.c
        cur = await c.execute(f"SELECT COUNT(*) FROM contacts{where}", params)
        (total,) = await cur.fetchone()  # type: ignore[misc]
        cur = await c.execute(
            f"SELECT * FROM contacts{where} ORDER BY last_seen DESC LIMIT ? OFFSET ?",
            params + [min(max(limit, 1), 1000), offset],
        )
        return _rows_to_dicts(await cur.fetchall()), total

    async def count(self) -> int:
        cur = await self._db.c.execute("SELECT COUNT(*) FROM contacts")
        (n,) = await cur.fetchone()  # type: ignore[misc]
        return n


class MessageStore:
    """Full message history (direct + channel, inbound + outbound)."""

    def __init__(self, db: Database):
        self._db = db

    async def add(
        self,
        *,
        direction: str,
        scope: str,
        text: str,
        peer: str | None = None,
        channel_idx: int | None = None,
        txt_type: int | None = None,
        sender_timestamp: int | None = None,
        snr: float | None = None,
        path_len: int | None = None,
        expected_ack: str | None = None,
        origin: str = "device",
    ) -> int:
        cur = await self._db.c.execute(
            "INSERT INTO messages (ts, direction, scope, peer, channel_idx,"
            " txt_type, sender_timestamp, text, snr, path_len, expected_ack, origin)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                int(time.time()),
                direction,
                scope,
                peer,
                channel_idx,
                txt_type,
                sender_timestamp,
                text,
                snr,
                path_len,
                expected_ack,
                origin,
            ),
        )
        return cur.lastrowid or 0

    async def mark_acked(self, ack_code: str) -> bool:
        cur = await self._db.c.execute(
            "UPDATE messages SET acked = 1 WHERE expected_ack = ? AND acked = 0",
            (ack_code,),
        )
        return cur.rowcount > 0

    async def list(
        self,
        *,
        peer: str = "",
        channel_idx: int | None = None,
        direction: str = "",
        after_ts: int = 0,
        before_ts: int = 0,
        limit: int = 100,
        offset: int = 0,
    ) -> tuple[list[dict[str, Any]], int]:
        clauses, params = [], []
        if peer:
            clauses.append("peer LIKE ?")
            params.append(f"{peer}%")
        if channel_idx is not None:
            clauses.append("channel_idx = ?")
            params.append(channel_idx)
        if direction:
            clauses.append("direction = ?")
            params.append(direction)
        if after_ts:
            clauses.append("ts >= ?")
            params.append(after_ts)
        if before_ts:
            clauses.append("ts < ?")
            params.append(before_ts)
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        c = self._db.c
        cur = await c.execute(f"SELECT COUNT(*) FROM messages{where}", params)
        (total,) = await cur.fetchone()  # type: ignore[misc]
        cur = await c.execute(
            f"SELECT * FROM messages{where} ORDER BY ts DESC, id DESC LIMIT ? OFFSET ?",
            params + [min(max(limit, 1), 1000), offset],
        )
        return _rows_to_dicts(await cur.fetchall()), total

    async def count(self) -> int:
        cur = await self._db.c.execute("SELECT COUNT(*) FROM messages")
        (n,) = await cur.fetchone()  # type: ignore[misc]
        return n

    async def prune(self, cutoff_ts: int) -> int:
        cur = await self._db.c.execute("DELETE FROM messages WHERE ts < ?", (cutoff_ts,))
        return cur.rowcount
