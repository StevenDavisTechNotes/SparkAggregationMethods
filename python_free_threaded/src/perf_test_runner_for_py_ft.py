#!python
# usage: .\.venv\Scripts\activate.ps1; python -O -m src.perf_test_runner_for_py_ft

import logging

from spark_agg_methods_common_python.utils.platform import setup_logging

from src.challenges.vanilla import vanilla_runner_py_ft

logger = logging.getLogger(__name__)


def main() -> None:
    logger.info(f"Running {__file__}")
    vanilla_runner_py_ft.main()
    logger.info("Done!")


if __name__ == "__main__":
    setup_logging()
    try:
        main()
    except KeyboardInterrupt:
        logger.warning("Interrupted!")
