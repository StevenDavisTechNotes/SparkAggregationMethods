#!python
# usage: .\venv\Scripts\activate.ps1; python -O -m src.perf_test_runner_for_dask

import logging

from spark_agg_methods_common_python.utils.platform import setup_logging

from src.challenges.bi_level import bi_level_runner_dask
from src.challenges.conditional import conditional_runner_dask
from src.challenges.vanilla import vanilla_runner_dask

logger = logging.getLogger(__name__)


def main() -> None:
    logger.info(f"Running {__file__}")
    bi_level_runner_dask.main()
    conditional_runner_dask.main()
    vanilla_runner_dask.main()
    # dedupe_runner_dask.main()  TODO: Implement this
    # sectional_runner_dask.main() TODO: Implement this
    logger.info("Done!")


if __name__ == "__main__":
    setup_logging()
    try:
        main()
    except KeyboardInterrupt:
        logger.warning("Interrupted!")
