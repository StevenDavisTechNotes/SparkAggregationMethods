#!python
# usage: python -O -m src.perf_test_runner_for_py_only

import logging

from spark_agg_methods_common_python.utils.platform import setup_logging

from src.challenges.vanilla import vanilla_runner_py_stream

logger = logging.getLogger(__name__)


def main():
    logger.info(f"Running {__file__}")
    try:
        vanilla_runner_py_stream.main()
    except KeyboardInterrupt:
        logger.warning("Interrupted!")
        return
    logger.info("Done!")


if __name__ == "__main__":
    setup_logging()
    main()
