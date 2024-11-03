#! python
# usage: .\venv\Scripts\activate.ps1; python -O -m src.perf_test_runner_for_dask

import logging
import sys

from src.challenges.bi_level import bi_level_runner_dask
from src.challenges.conditional import conditional_runner_dask
from src.challenges.vanilla import vanilla_runner_dask

logger = logging.getLogger(__name__)


def main():
    logger.info(f"Running {__file__}")
    try:
        # with DaskClient(
        #         processes=True,
        #         n_workers=LOCAL_NUM_EXECUTORS,
        #         threads_per_worker=1,
        # ) as dask_client:
        bi_level_runner_dask.do_with_client()
        conditional_runner_dask.do_with_client()
        vanilla_runner_dask.do_with_local_client()
        # dedupe_runner_dask.do_with_client()
        # sectional_runner_dask.do_with_client()
    except KeyboardInterrupt:
        logger.warning("Interrupted!")
        return
    logger.info("Done!")


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
    main()
