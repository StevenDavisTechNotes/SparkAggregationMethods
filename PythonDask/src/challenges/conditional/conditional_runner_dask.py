#!python
# usage: .\venv\Scripts\activate.ps1 ; python -O -m src.challenges.conditional.conditional_runner_dask
import logging
import os

from spark_agg_methods_common_python.challenges.conditional.conditional_record_runs import (
    ConditionalPythonRunResultFileWriter,
)
from spark_agg_methods_common_python.perf_test_common import CalcEngine
from spark_agg_methods_common_python.utils.platform import setup_logging

logger = logging.getLogger(__name__)

ENGINE = CalcEngine.DASK


class ConditionalLevelDaskRunResultFileWriter(ConditionalPythonRunResultFileWriter):
    RUN_LOG_FILE_PATH: str = os.path.abspath('results/conditional_dask_runs.csv')

    def __init__(self):
        super().__init__(
            engine=ENGINE,
            rel_log_file_path=__class__.RUN_LOG_FILE_PATH,
        )

# TODO


def main() -> None:
    logger.info(f"Running {__file__}")
    # args = parse_args()
    # update_challenge_registration()
    # with DaskClient(  TODO: Add cluster testing
    #         processes=True,
    #         n_workers=LOCAL_NUM_EXECUTORS,
    #         threads_per_worker=1,
    # ) as dask_client:
    # do_test_runs(args)
    logger.info("Done!")


if __name__ == "__main__":
    setup_logging()
    try:
        main()
    except KeyboardInterrupt:
        logger.warning("Interrupted!")
