from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Empty(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class StatusResponse(_message.Message):
    __slots__ = ("device_connected", "transport", "device_name", "public_key", "connected_since", "proxy_clients", "packets_logged", "messages_stored", "contacts_stored", "daemon_version")
    DEVICE_CONNECTED_FIELD_NUMBER: _ClassVar[int]
    TRANSPORT_FIELD_NUMBER: _ClassVar[int]
    DEVICE_NAME_FIELD_NUMBER: _ClassVar[int]
    PUBLIC_KEY_FIELD_NUMBER: _ClassVar[int]
    CONNECTED_SINCE_FIELD_NUMBER: _ClassVar[int]
    PROXY_CLIENTS_FIELD_NUMBER: _ClassVar[int]
    PACKETS_LOGGED_FIELD_NUMBER: _ClassVar[int]
    MESSAGES_STORED_FIELD_NUMBER: _ClassVar[int]
    CONTACTS_STORED_FIELD_NUMBER: _ClassVar[int]
    DAEMON_VERSION_FIELD_NUMBER: _ClassVar[int]
    device_connected: bool
    transport: str
    device_name: str
    public_key: str
    connected_since: int
    proxy_clients: int
    packets_logged: int
    messages_stored: int
    contacts_stored: int
    daemon_version: str
    def __init__(self, device_connected: _Optional[bool] = ..., transport: _Optional[str] = ..., device_name: _Optional[str] = ..., public_key: _Optional[str] = ..., connected_since: _Optional[int] = ..., proxy_clients: _Optional[int] = ..., packets_logged: _Optional[int] = ..., messages_stored: _Optional[int] = ..., contacts_stored: _Optional[int] = ..., daemon_version: _Optional[str] = ...) -> None: ...

class JsonResponse(_message.Message):
    __slots__ = ("json",)
    JSON_FIELD_NUMBER: _ClassVar[int]
    json: str
    def __init__(self, json: _Optional[str] = ...) -> None: ...

class CommandResult(_message.Message):
    __slots__ = ("ok", "error", "json")
    OK_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    JSON_FIELD_NUMBER: _ClassVar[int]
    ok: bool
    error: str
    json: str
    def __init__(self, ok: _Optional[bool] = ..., error: _Optional[str] = ..., json: _Optional[str] = ...) -> None: ...

class SetNameRequest(_message.Message):
    __slots__ = ("name",)
    NAME_FIELD_NUMBER: _ClassVar[int]
    name: str
    def __init__(self, name: _Optional[str] = ...) -> None: ...

class SetRadioRequest(_message.Message):
    __slots__ = ("freq", "bw", "sf", "cr")
    FREQ_FIELD_NUMBER: _ClassVar[int]
    BW_FIELD_NUMBER: _ClassVar[int]
    SF_FIELD_NUMBER: _ClassVar[int]
    CR_FIELD_NUMBER: _ClassVar[int]
    freq: float
    bw: float
    sf: int
    cr: int
    def __init__(self, freq: _Optional[float] = ..., bw: _Optional[float] = ..., sf: _Optional[int] = ..., cr: _Optional[int] = ...) -> None: ...

class SetTxPowerRequest(_message.Message):
    __slots__ = ("tx_power",)
    TX_POWER_FIELD_NUMBER: _ClassVar[int]
    tx_power: int
    def __init__(self, tx_power: _Optional[int] = ...) -> None: ...

class SetCoordsRequest(_message.Message):
    __slots__ = ("lat", "lon")
    LAT_FIELD_NUMBER: _ClassVar[int]
    LON_FIELD_NUMBER: _ClassVar[int]
    lat: float
    lon: float
    def __init__(self, lat: _Optional[float] = ..., lon: _Optional[float] = ...) -> None: ...

class SetTimeRequest(_message.Message):
    __slots__ = ("epoch_secs",)
    EPOCH_SECS_FIELD_NUMBER: _ClassVar[int]
    epoch_secs: int
    def __init__(self, epoch_secs: _Optional[int] = ...) -> None: ...

class SendAdvertRequest(_message.Message):
    __slots__ = ("flood",)
    FLOOD_FIELD_NUMBER: _ClassVar[int]
    flood: bool
    def __init__(self, flood: _Optional[bool] = ...) -> None: ...

class RemoveContactRequest(_message.Message):
    __slots__ = ("public_key",)
    PUBLIC_KEY_FIELD_NUMBER: _ClassVar[int]
    public_key: str
    def __init__(self, public_key: _Optional[str] = ...) -> None: ...

class ListContactsRequest(_message.Message):
    __slots__ = ("query", "device_only", "limit", "offset")
    QUERY_FIELD_NUMBER: _ClassVar[int]
    DEVICE_ONLY_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    OFFSET_FIELD_NUMBER: _ClassVar[int]
    query: str
    device_only: bool
    limit: int
    offset: int
    def __init__(self, query: _Optional[str] = ..., device_only: _Optional[bool] = ..., limit: _Optional[int] = ..., offset: _Optional[int] = ...) -> None: ...

class Contact(_message.Message):
    __slots__ = ("public_key", "adv_name", "type", "flags", "out_path_len", "out_path", "adv_lat", "adv_lon", "last_advert", "lastmod", "first_seen", "last_seen", "on_device")
    PUBLIC_KEY_FIELD_NUMBER: _ClassVar[int]
    ADV_NAME_FIELD_NUMBER: _ClassVar[int]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    FLAGS_FIELD_NUMBER: _ClassVar[int]
    OUT_PATH_LEN_FIELD_NUMBER: _ClassVar[int]
    OUT_PATH_FIELD_NUMBER: _ClassVar[int]
    ADV_LAT_FIELD_NUMBER: _ClassVar[int]
    ADV_LON_FIELD_NUMBER: _ClassVar[int]
    LAST_ADVERT_FIELD_NUMBER: _ClassVar[int]
    LASTMOD_FIELD_NUMBER: _ClassVar[int]
    FIRST_SEEN_FIELD_NUMBER: _ClassVar[int]
    LAST_SEEN_FIELD_NUMBER: _ClassVar[int]
    ON_DEVICE_FIELD_NUMBER: _ClassVar[int]
    public_key: str
    adv_name: str
    type: int
    flags: int
    out_path_len: int
    out_path: str
    adv_lat: float
    adv_lon: float
    last_advert: int
    lastmod: int
    first_seen: int
    last_seen: int
    on_device: bool
    def __init__(self, public_key: _Optional[str] = ..., adv_name: _Optional[str] = ..., type: _Optional[int] = ..., flags: _Optional[int] = ..., out_path_len: _Optional[int] = ..., out_path: _Optional[str] = ..., adv_lat: _Optional[float] = ..., adv_lon: _Optional[float] = ..., last_advert: _Optional[int] = ..., lastmod: _Optional[int] = ..., first_seen: _Optional[int] = ..., last_seen: _Optional[int] = ..., on_device: _Optional[bool] = ...) -> None: ...

class ListContactsResponse(_message.Message):
    __slots__ = ("contacts", "total")
    CONTACTS_FIELD_NUMBER: _ClassVar[int]
    TOTAL_FIELD_NUMBER: _ClassVar[int]
    contacts: _containers.RepeatedCompositeFieldContainer[Contact]
    total: int
    def __init__(self, contacts: _Optional[_Iterable[_Union[Contact, _Mapping]]] = ..., total: _Optional[int] = ...) -> None: ...

class SendMessageRequest(_message.Message):
    __slots__ = ("destination", "text")
    DESTINATION_FIELD_NUMBER: _ClassVar[int]
    TEXT_FIELD_NUMBER: _ClassVar[int]
    destination: str
    text: str
    def __init__(self, destination: _Optional[str] = ..., text: _Optional[str] = ...) -> None: ...

class SendChannelMessageRequest(_message.Message):
    __slots__ = ("channel_idx", "text")
    CHANNEL_IDX_FIELD_NUMBER: _ClassVar[int]
    TEXT_FIELD_NUMBER: _ClassVar[int]
    channel_idx: int
    text: str
    def __init__(self, channel_idx: _Optional[int] = ..., text: _Optional[str] = ...) -> None: ...

class SendMessageResponse(_message.Message):
    __slots__ = ("ok", "error", "expected_ack", "message_id")
    OK_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    EXPECTED_ACK_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_ID_FIELD_NUMBER: _ClassVar[int]
    ok: bool
    error: str
    expected_ack: str
    message_id: int
    def __init__(self, ok: _Optional[bool] = ..., error: _Optional[str] = ..., expected_ack: _Optional[str] = ..., message_id: _Optional[int] = ...) -> None: ...

class ListMessagesRequest(_message.Message):
    __slots__ = ("peer", "channel_idx", "direction", "after_ts", "before_ts", "limit", "offset")
    PEER_FIELD_NUMBER: _ClassVar[int]
    CHANNEL_IDX_FIELD_NUMBER: _ClassVar[int]
    DIRECTION_FIELD_NUMBER: _ClassVar[int]
    AFTER_TS_FIELD_NUMBER: _ClassVar[int]
    BEFORE_TS_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    OFFSET_FIELD_NUMBER: _ClassVar[int]
    peer: str
    channel_idx: int
    direction: str
    after_ts: int
    before_ts: int
    limit: int
    offset: int
    def __init__(self, peer: _Optional[str] = ..., channel_idx: _Optional[int] = ..., direction: _Optional[str] = ..., after_ts: _Optional[int] = ..., before_ts: _Optional[int] = ..., limit: _Optional[int] = ..., offset: _Optional[int] = ...) -> None: ...

class MessageRecord(_message.Message):
    __slots__ = ("id", "ts", "direction", "scope", "peer", "channel_idx", "txt_type", "sender_timestamp", "text", "snr", "expected_ack", "acked", "origin")
    ID_FIELD_NUMBER: _ClassVar[int]
    TS_FIELD_NUMBER: _ClassVar[int]
    DIRECTION_FIELD_NUMBER: _ClassVar[int]
    SCOPE_FIELD_NUMBER: _ClassVar[int]
    PEER_FIELD_NUMBER: _ClassVar[int]
    CHANNEL_IDX_FIELD_NUMBER: _ClassVar[int]
    TXT_TYPE_FIELD_NUMBER: _ClassVar[int]
    SENDER_TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    TEXT_FIELD_NUMBER: _ClassVar[int]
    SNR_FIELD_NUMBER: _ClassVar[int]
    EXPECTED_ACK_FIELD_NUMBER: _ClassVar[int]
    ACKED_FIELD_NUMBER: _ClassVar[int]
    ORIGIN_FIELD_NUMBER: _ClassVar[int]
    id: int
    ts: int
    direction: str
    scope: str
    peer: str
    channel_idx: int
    txt_type: int
    sender_timestamp: int
    text: str
    snr: float
    expected_ack: str
    acked: bool
    origin: str
    def __init__(self, id: _Optional[int] = ..., ts: _Optional[int] = ..., direction: _Optional[str] = ..., scope: _Optional[str] = ..., peer: _Optional[str] = ..., channel_idx: _Optional[int] = ..., txt_type: _Optional[int] = ..., sender_timestamp: _Optional[int] = ..., text: _Optional[str] = ..., snr: _Optional[float] = ..., expected_ack: _Optional[str] = ..., acked: _Optional[bool] = ..., origin: _Optional[str] = ...) -> None: ...

class ListMessagesResponse(_message.Message):
    __slots__ = ("messages", "total")
    MESSAGES_FIELD_NUMBER: _ClassVar[int]
    TOTAL_FIELD_NUMBER: _ClassVar[int]
    messages: _containers.RepeatedCompositeFieldContainer[MessageRecord]
    total: int
    def __init__(self, messages: _Optional[_Iterable[_Union[MessageRecord, _Mapping]]] = ..., total: _Optional[int] = ...) -> None: ...

class QueryPacketsRequest(_message.Message):
    __slots__ = ("after_ts", "before_ts", "payload_type", "route_type", "limit", "offset")
    AFTER_TS_FIELD_NUMBER: _ClassVar[int]
    BEFORE_TS_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_TYPE_FIELD_NUMBER: _ClassVar[int]
    ROUTE_TYPE_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    OFFSET_FIELD_NUMBER: _ClassVar[int]
    after_ts: int
    before_ts: int
    payload_type: int
    route_type: int
    limit: int
    offset: int
    def __init__(self, after_ts: _Optional[int] = ..., before_ts: _Optional[int] = ..., payload_type: _Optional[int] = ..., route_type: _Optional[int] = ..., limit: _Optional[int] = ..., offset: _Optional[int] = ...) -> None: ...

class PacketRecord(_message.Message):
    __slots__ = ("id", "ts", "kind", "snr", "rssi", "route_type", "payload_type", "path_len", "path", "payload_len", "payload_hex")
    ID_FIELD_NUMBER: _ClassVar[int]
    TS_FIELD_NUMBER: _ClassVar[int]
    KIND_FIELD_NUMBER: _ClassVar[int]
    SNR_FIELD_NUMBER: _ClassVar[int]
    RSSI_FIELD_NUMBER: _ClassVar[int]
    ROUTE_TYPE_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_TYPE_FIELD_NUMBER: _ClassVar[int]
    PATH_LEN_FIELD_NUMBER: _ClassVar[int]
    PATH_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_LEN_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_HEX_FIELD_NUMBER: _ClassVar[int]
    id: int
    ts: int
    kind: str
    snr: float
    rssi: int
    route_type: int
    payload_type: int
    path_len: int
    path: str
    payload_len: int
    payload_hex: str
    def __init__(self, id: _Optional[int] = ..., ts: _Optional[int] = ..., kind: _Optional[str] = ..., snr: _Optional[float] = ..., rssi: _Optional[int] = ..., route_type: _Optional[int] = ..., payload_type: _Optional[int] = ..., path_len: _Optional[int] = ..., path: _Optional[str] = ..., payload_len: _Optional[int] = ..., payload_hex: _Optional[str] = ...) -> None: ...

class QueryPacketsResponse(_message.Message):
    __slots__ = ("packets", "total")
    PACKETS_FIELD_NUMBER: _ClassVar[int]
    TOTAL_FIELD_NUMBER: _ClassVar[int]
    packets: _containers.RepeatedCompositeFieldContainer[PacketRecord]
    total: int
    def __init__(self, packets: _Optional[_Iterable[_Union[PacketRecord, _Mapping]]] = ..., total: _Optional[int] = ...) -> None: ...

class PacketStatsRequest(_message.Message):
    __slots__ = ("after_ts", "before_ts", "bucket")
    AFTER_TS_FIELD_NUMBER: _ClassVar[int]
    BEFORE_TS_FIELD_NUMBER: _ClassVar[int]
    BUCKET_FIELD_NUMBER: _ClassVar[int]
    after_ts: int
    before_ts: int
    bucket: str
    def __init__(self, after_ts: _Optional[int] = ..., before_ts: _Optional[int] = ..., bucket: _Optional[str] = ...) -> None: ...

class PacketStatsBucket(_message.Message):
    __slots__ = ("bucket_ts", "payload_type", "route_type", "count", "total_bytes", "avg_snr", "avg_rssi", "min_snr", "max_snr")
    BUCKET_TS_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_TYPE_FIELD_NUMBER: _ClassVar[int]
    ROUTE_TYPE_FIELD_NUMBER: _ClassVar[int]
    COUNT_FIELD_NUMBER: _ClassVar[int]
    TOTAL_BYTES_FIELD_NUMBER: _ClassVar[int]
    AVG_SNR_FIELD_NUMBER: _ClassVar[int]
    AVG_RSSI_FIELD_NUMBER: _ClassVar[int]
    MIN_SNR_FIELD_NUMBER: _ClassVar[int]
    MAX_SNR_FIELD_NUMBER: _ClassVar[int]
    bucket_ts: int
    payload_type: int
    route_type: int
    count: int
    total_bytes: int
    avg_snr: float
    avg_rssi: float
    min_snr: float
    max_snr: float
    def __init__(self, bucket_ts: _Optional[int] = ..., payload_type: _Optional[int] = ..., route_type: _Optional[int] = ..., count: _Optional[int] = ..., total_bytes: _Optional[int] = ..., avg_snr: _Optional[float] = ..., avg_rssi: _Optional[float] = ..., min_snr: _Optional[float] = ..., max_snr: _Optional[float] = ...) -> None: ...

class PacketStatsResponse(_message.Message):
    __slots__ = ("buckets",)
    BUCKETS_FIELD_NUMBER: _ClassVar[int]
    buckets: _containers.RepeatedCompositeFieldContainer[PacketStatsBucket]
    def __init__(self, buckets: _Optional[_Iterable[_Union[PacketStatsBucket, _Mapping]]] = ...) -> None: ...

class StreamEventsRequest(_message.Message):
    __slots__ = ("types",)
    TYPES_FIELD_NUMBER: _ClassVar[int]
    types: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, types: _Optional[_Iterable[str]] = ...) -> None: ...

class DaemonEvent(_message.Message):
    __slots__ = ("type", "ts", "payload_json")
    TYPE_FIELD_NUMBER: _ClassVar[int]
    TS_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_JSON_FIELD_NUMBER: _ClassVar[int]
    type: str
    ts: int
    payload_json: str
    def __init__(self, type: _Optional[str] = ..., ts: _Optional[int] = ..., payload_json: _Optional[str] = ...) -> None: ...

class RawCommandRequest(_message.Message):
    __slots__ = ("frame", "timeout")
    FRAME_FIELD_NUMBER: _ClassVar[int]
    TIMEOUT_FIELD_NUMBER: _ClassVar[int]
    frame: bytes
    timeout: float
    def __init__(self, frame: _Optional[bytes] = ..., timeout: _Optional[float] = ...) -> None: ...

class RawCommandResponse(_message.Message):
    __slots__ = ("frames",)
    FRAMES_FIELD_NUMBER: _ClassVar[int]
    frames: _containers.RepeatedScalarFieldContainer[bytes]
    def __init__(self, frames: _Optional[_Iterable[bytes]] = ...) -> None: ...

class ProxyStatusResponse(_message.Message):
    __slots__ = ("enabled", "clients", "client_addrs", "commands_proxied", "cache_hits")
    ENABLED_FIELD_NUMBER: _ClassVar[int]
    CLIENTS_FIELD_NUMBER: _ClassVar[int]
    CLIENT_ADDRS_FIELD_NUMBER: _ClassVar[int]
    COMMANDS_PROXIED_FIELD_NUMBER: _ClassVar[int]
    CACHE_HITS_FIELD_NUMBER: _ClassVar[int]
    enabled: bool
    clients: int
    client_addrs: _containers.RepeatedScalarFieldContainer[str]
    commands_proxied: int
    cache_hits: int
    def __init__(self, enabled: _Optional[bool] = ..., clients: _Optional[int] = ..., client_addrs: _Optional[_Iterable[str]] = ..., commands_proxied: _Optional[int] = ..., cache_hits: _Optional[int] = ...) -> None: ...

class ChannelInfo(_message.Message):
    __slots__ = ("index", "name", "flags", "frequency", "bandwidth", "sf", "cr")
    INDEX_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    FLAGS_FIELD_NUMBER: _ClassVar[int]
    FREQUENCY_FIELD_NUMBER: _ClassVar[int]
    BANDWIDTH_FIELD_NUMBER: _ClassVar[int]
    SF_FIELD_NUMBER: _ClassVar[int]
    CR_FIELD_NUMBER: _ClassVar[int]
    index: int
    name: str
    flags: int
    frequency: int
    bandwidth: int
    sf: int
    cr: int
    def __init__(self, index: _Optional[int] = ..., name: _Optional[str] = ..., flags: _Optional[int] = ..., frequency: _Optional[int] = ..., bandwidth: _Optional[int] = ..., sf: _Optional[int] = ..., cr: _Optional[int] = ...) -> None: ...

class ListChannelsResponse(_message.Message):
    __slots__ = ("channels",)
    CHANNELS_FIELD_NUMBER: _ClassVar[int]
    channels: _containers.RepeatedCompositeFieldContainer[ChannelInfo]
    def __init__(self, channels: _Optional[_Iterable[_Union[ChannelInfo, _Mapping]]] = ...) -> None: ...
