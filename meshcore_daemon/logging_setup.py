"""Console + rotating file logging."""

import logging
import logging.handlers

from .config import Settings

_FORMAT = "%(asctime)s %(levelname)-8s %(name)s: %(message)s"


def setup_logging(settings: Settings) -> None:
    level = getattr(logging, settings.log_level.upper(), logging.INFO)

    root = logging.getLogger()
    root.setLevel(level)

    console = logging.StreamHandler()
    console.setFormatter(logging.Formatter(_FORMAT))
    root.addHandler(console)

    settings.log_dir.mkdir(parents=True, exist_ok=True)
    file_handler = logging.handlers.RotatingFileHandler(
        settings.log_dir / "meshcored.log",
        maxBytes=settings.log_file_max_bytes,
        backupCount=settings.log_file_backups,
        encoding="utf-8",
    )
    file_handler.setFormatter(logging.Formatter(_FORMAT))
    root.addHandler(file_handler)

    # The meshcore library manages its own logger level in MeshCore.__init__;
    # keep it aligned with the daemon unless the user asked for DEBUG.
    logging.getLogger("meshcore").setLevel(level)
