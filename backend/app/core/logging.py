import logging
from typing import Optional


def configure_logging(level: Optional[str] = None) -> None:
    logging.basicConfig(
        level=level or "INFO",
        format="[%(asctime)s] %(levelname)s %(name)s - %(message)s",
    )


logger = logging.getLogger("mlb_unicorn_engine")
