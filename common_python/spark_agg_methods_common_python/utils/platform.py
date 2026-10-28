
import logging
import sys


def setup_logging(
        include_debug: bool = False
):
    logging.basicConfig(
        stream=sys.stdout,
        level=logging.DEBUG if include_debug else logging.INFO,
    )
