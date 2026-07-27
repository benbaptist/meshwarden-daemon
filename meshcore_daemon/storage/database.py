"""SQLite database: connection, pragmas and schema.

Design notes for long-term sustainability:

* WAL journal + NORMAL sync keeps writes cheap and readers unblocked.
* ``auto_vacuum=INCREMENTAL`` lets the retention job actually reclaim disk
  space without a full ``VACUUM``.
* Raw packets are kept for a bounded window (``packet_retention_days``);
  per-hour aggregates in ``packet_stats_hourly`` are tiny and kept forever,
  so statistics survive pruning.
"""

import logging
from pathlib import Path

import aiosqlite

logger = logging.getLogger(__name__)

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS packets (
    id           INTEGER PRIMARY KEY,
    ts           INTEGER NOT NULL,          -- unix seconds (receive time)
    kind         TEXT    NOT NULL,          -- 'rx_log' | 'raw'
    snr          REAL,
    rssi         INTEGER,
    route_type   INTEGER,
    payload_type INTEGER,
    path_len     INTEGER,
    path         TEXT,                      -- json list of hop hashes
    payload_len  INTEGER,
    payload_hex  TEXT                       -- NULL when payload storage disabled
);
CREATE INDEX IF NOT EXISTS idx_packets_ts ON packets(ts);
CREATE INDEX IF NOT EXISTS idx_packets_type_ts ON packets(payload_type, ts);

CREATE TABLE IF NOT EXISTS packet_stats_hourly (
    bucket       INTEGER NOT NULL,          -- unix seconds, hour aligned
    payload_type INTEGER NOT NULL,          -- -1 when unknown
    route_type   INTEGER NOT NULL,          -- -1 when unknown
    count        INTEGER NOT NULL,
    total_bytes  INTEGER NOT NULL,
    sum_snr      REAL    NOT NULL,
    sum_rssi     REAL    NOT NULL,
    min_snr      REAL,
    max_snr      REAL,
    PRIMARY KEY (bucket, payload_type, route_type)
);

CREATE TABLE IF NOT EXISTS contacts (
    public_key   TEXT PRIMARY KEY,
    adv_name     TEXT,
    type         INTEGER,
    flags        INTEGER,
    out_path     TEXT,
    out_path_len INTEGER,
    adv_lat      REAL,
    adv_lon      REAL,
    last_advert  INTEGER,
    lastmod      INTEGER,
    first_seen   INTEGER NOT NULL,
    last_seen    INTEGER NOT NULL,
    on_device    INTEGER NOT NULL DEFAULT 1,
    raw_json     TEXT
);
CREATE INDEX IF NOT EXISTS idx_contacts_name ON contacts(adv_name);

CREATE TABLE IF NOT EXISTS messages (
    id               INTEGER PRIMARY KEY,
    ts               INTEGER NOT NULL,      -- unix seconds (store time)
    direction        TEXT    NOT NULL,      -- 'in' | 'out'
    scope            TEXT    NOT NULL,      -- 'contact' | 'channel'
    peer             TEXT,                  -- pubkey prefix (contact scope)
    channel_idx      INTEGER,
    txt_type         INTEGER,
    sender_timestamp INTEGER,
    text             TEXT    NOT NULL,
    snr              REAL,
    path_len         INTEGER,
    expected_ack     TEXT,
    acked            INTEGER NOT NULL DEFAULT 0,
    origin           TEXT    NOT NULL       -- 'device' | 'daemon' | 'proxy'
);
CREATE INDEX IF NOT EXISTS idx_messages_ts ON messages(ts);
CREATE INDEX IF NOT EXISTS idx_messages_peer ON messages(peer, ts);
CREATE INDEX IF NOT EXISTS idx_messages_ack ON messages(expected_ack) WHERE expected_ack IS NOT NULL;
"""


class Database:
    def __init__(self, path: Path):
        self._path = path
        self.conn: aiosqlite.Connection | None = None

    async def open(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        # isolation_level=None -> autocommit; fine for our single-writer daemon
        self.conn = await aiosqlite.connect(self._path, isolation_level=None)
        self.conn.row_factory = aiosqlite.Row
        await self.conn.execute("PRAGMA journal_mode=WAL")
        await self.conn.execute("PRAGMA synchronous=NORMAL")
        await self.conn.execute("PRAGMA auto_vacuum=INCREMENTAL")
        await self.conn.executescript(SCHEMA)
        logger.info("database opened at %s", self._path)

    async def close(self) -> None:
        if self.conn is not None:
            await self.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            await self.conn.close()
            self.conn = None
            logger.info("database closed")

    @property
    def c(self) -> aiosqlite.Connection:
        assert self.conn is not None, "database not opened"
        return self.conn
