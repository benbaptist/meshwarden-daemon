"""Entry point: ``python -m meshcore_daemon`` or the ``meshcore-daemon`` script."""

import asyncio
import signal

from .daemon import Daemon


async def _run() -> None:
    daemon = Daemon()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, daemon.request_stop)
    await daemon.run()


def main() -> None:
    asyncio.run(_run())


if __name__ == "__main__":
    main()
