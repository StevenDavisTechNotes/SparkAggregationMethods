#!python
# usage: .\venv\Scripts\activate.ps1; python -O -m src.perf_test_runner_for_dask

import logging

from spark_agg_methods_common_python.utils.platform import setup_logging

from src.challenges.bi_level import bi_level_runner_dask
from src.challenges.conditional import conditional_runner_dask
from src.challenges.vanilla import vanilla_runner_dask

logger = logging.getLogger(__name__)


def main():
    logger.info(f"Running {__file__}")
    try:
        # with DaskClient(  TODO: Need to test cluster setup
        #         processes=True,
        #         n_workers=LOCAL_NUM_EXECUTORS,
        #         threads_per_worker=1,
        # ) as dask_client:
        bi_level_runner_dask.do_with_client()
        conditional_runner_dask.do_with_client()
        vanilla_runner_dask.do_with_local_client()
        # dedupe_runner_dask.do_with_client()  TODO: Implement this
        # sectional_runner_dask.do_with_client() TODO: Implement this
    except KeyboardInterrupt:
        logger.warning("Interrupted!")
        return
    logger.info("Done!")


if __name__ == "__main__":
    setup_logging()
    main()
