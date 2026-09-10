# meshcore-daemon

Man-in-the-middle daemon for [MeshCore](https://meshcore.co.uk) companion nodes.
Owns the single serial/TCP connection to a node and shares it with everything
else: a transparent TCP proxy for MeshCore apps, a gRPC control API, and an
SQLite archive of packets, messages and contacts that outgrows the device's
own limits.

## Features

- **Device gateway** — connects to a node over USB serial or TCP using the
  official [meshcore](https://github.com/meshcore-dev/meshcore_py) library,
  with automatic reconnection.
- **Transparent TCP proxy** — multiple MeshCore apps/tools can connect at
  once (default port `5000`); commands are serialized onto the single node
  connection, push notifications are broadcast to every client, and read-only
  responses (contacts, device info, battery) are cached briefly so low-power
  nodes aren't hammered.
- **Packet logging** — every RF log/raw packet is stored in SQLite with an
  always-on hourly aggregate table; raw rows are pruned after a configurable
  retention window while statistics survive forever.
- **Message + contact archive** — all messages (inbound, daemon-sent, and
  app-sent through the proxy) and every contact ever seen are persisted,
  beyond the ~350 on-device contact limit.
- **gRPC API** (default port `50051`) — full control: status, device config,
  contacts, messaging, packet queries/stats, a live event stream, and a raw
  command escape hatch. Standard gRPC health service included.

## Running

```bash
cp .env.example .env   # adjust device transport/port
docker compose up -d --build
```

For USB nodes uncomment the `devices:` mapping in `docker-compose.yml`.

### Without Docker

```bash
python3.11 -m venv .venv
.venv/bin/pip install -e .
.venv/bin/meshcore-daemon
```

Configuration is environment-only (or `.env`); see [.env.example](.env.example).

## Development

Regenerate gRPC stubs after editing [protos/meshcored.proto](protos/meshcored.proto):

```bash
.venv/bin/pip install -e '.[dev]'
./scripts/gen_protos.sh
```

## Ping RPC

`Ping` sends a MeshCore TRACE round trip to a stored repeater or room server.
`hash_size` is a trace route-hash width (1 by default; 2 and 4 supported), not
payload size. One five-second deadline covers directory lookup, command queueing,
send acknowledgement and the returned trace. Timeouts use `DEADLINE_EXCEEDED`.
The returned frame must match fresh tag/auth correlation values, flags and the
entire round-trip route; neither a command ACK nor a message ACK is success.
The trace auth field is a correlation value, not cryptographic authentication.

Known direct routes and forwarding-enabled servers are required. Widening stored
hashes requires uniquely resolvable relay keys; no route discovery, login or
contact mutation is performed. Return relays reverse the outbound route.
Multi-byte trace support requires compatible firmware (MeshCore v1.11+).
Trace semantics were verified against upstream `src/Mesh.cpp`, `src/Packet.cpp`
and `examples/companion_radio/MyMesh.cpp`, and installed meshcore 2.3.8.
Run `scripts/validate_grpc.py --offline` for hardware-free regression tests.

## Notes on message fetching

The companion protocol deletes a queued message once *someone* syncs it. With
`MESHCORED_MESSAGE_FETCH_MODE=auto` (default) the daemon only drains queued
messages while no proxy client is connected, so connected apps behave exactly
as if they were talking to the node directly — and the daemon still archives
every message it observes, no matter who fetched it.
