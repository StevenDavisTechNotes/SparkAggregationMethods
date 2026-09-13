
import logging
import sys


def setup_logging():
    logging.basicConfig(
        stream=sys.stdout,
        level=logging.DEBUG if __debug__ else logging.INFO,
    )
