# meshwarden-daemon

Man-in-the-middle daemon for [MeshCore](https://meshcore.co.uk) companion nodes. Makes MeshCore more of a traditional 'service', upon which you can build smaller microservices out of.

This is part of a greater ecosystem of tools that lets you do more stuff with MeshCore. 

- [MeshWarden CLI](https://github.com/benbaptist/meshwarden-cli): This is the simplest way to interact with the daemon.
- [Meshwarden Dashboard](https://github.com/benbaptist/meshwarden-dashboard): More advanced way to access the daemon.

## Features

- **Device connection** — talks to a node over USB serial or TCP using the
  official [meshcore](https://github.com/meshcore-dev/meshcore_py) library and
  reconnects automatically.
- **TCP proxy** — several MeshCore apps can share the node at once (default
  port `5000`). Commands are queued onto the single connection and push
  notifications go to every client.
- **Message and contact archive** — all messages and every contact ever seen
  are saved, past the device's ~350 contact limit.
- **Packet logging** — packets are stored in SQLite. Raw rows are pruned after
  a configurable retention period; hourly statistics are kept indefinitely.
- **gRPC API** (default port `50051`) — status, device config, contacts,
  messaging, packet queries, a live event stream, and raw commands.

## Running

### With Docker

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

The gRPC definition lives in the shared `meshwarden-protos` repo, included as a
git submodule at `protos/`. Clone with `--recurse-submodules` (or run
`git submodule update --init`), then after changing the proto regenerate the stubs:

```bash
.venv/bin/pip install -e '.[dev]'
bash protos/gen_python.sh meshcore_daemon/grpc_api/pb
```

Run the tests with `.venv/bin/python -m unittest discover tests`.

## Vibecoded

This project was mostly written by AI coding agents as an experiment in
vibecoding. It works for me, but it hasn't had careful human review, so treat
it accordingly and read the code before trusting it with anything important.