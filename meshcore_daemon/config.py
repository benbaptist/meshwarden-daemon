"""Daemon configuration.

All settings come from the environment (prefix ``MESHCORED_``) or a local
``.env`` file. This module is the single source of truth for configuration.
"""

from pathlib import Path
from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="MESHCORED_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # --- Device connection -------------------------------------------------
    device_transport: Literal["serial", "tcp"] = "serial"
    serial_port: str = "/dev/ttyUSB0"
    serial_baud: int = 115200
    device_tcp_host: str = "192.168.1.100"
    device_tcp_port: int = 5000
    command_timeout: float = 15.0
    #: 0 = retry forever (library-level auto reconnect attempts per outage)
    reconnect_max_attempts: int = 0
    reconnect_backoff_max: float = 60.0

    # --- Data / storage ----------------------------------------------------
    data_dir: Path = Path("./data")
    #: raw packet rows older than this are pruned (hourly aggregates are kept forever)
    packet_retention_days: int = 90
    #: store full raw packet payload hex alongside metadata
    store_packet_payloads: bool = True
    #: 0 = keep messages forever
    message_retention_days: int = 0
    retention_interval_minutes: int = 60

    # --- Behaviour ---------------------------------------------------------
    #: auto   = daemon fetches queued messages only while no proxy client is connected
    #: always = daemon always fetches (proxy clients will not receive queued messages)
    #: never  = daemon never fetches (messages are still captured when clients fetch)
    message_fetch_mode: Literal["auto", "always", "never"] = "auto"
    contact_sync_interval: float = 300.0

    # --- Logging -------------------------------------------------------------
    log_level: str = "INFO"
    log_file_max_bytes: int = 10 * 1024 * 1024
    log_file_backups: int = 5

    # --- gRPC API ------------------------------------------------------------
    grpc_host: str = "0.0.0.0"
    grpc_port: int = 50051

    # --- MeshCore TCP proxy ----------------------------------------------------
    proxy_enabled: bool = True
    proxy_host: str = "0.0.0.0"
    proxy_port: int = 5000
    #: TTL in seconds for cached read-only responses (contacts, device info, ...)
    proxy_cache_ttl: float = 30.0
    proxy_max_clients: int = 8

    @property
    def db_path(self) -> Path:
        return self.data_dir / "meshcored.db"

    @property
    def log_dir(self) -> Path:
        return self.data_dir / "logs"
