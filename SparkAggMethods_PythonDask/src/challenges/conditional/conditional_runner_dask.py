import logging
import os

from spark_agg_methods_common_python.challenges.conditional.conditional_record_runs import (
    ConditionalPythonRunResultFileWriter,
)
from spark_agg_methods_common_python.perf_test_common import CalcEngine

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


def do_with_client():
    # args = parse_args()
    # update_challenge_registration()
    # return do_test_runs(args)
    pass


def main():
    logger.info(f"Running {__file__}")
    try:
        # with DaskClient(
        #         processes=True,
        #         n_workers=LOCAL_NUM_EXECUTORS,
        #         threads_per_worker=1,
        # ) as dask_client:
        do_with_client()
    except KeyboardInterrupt:
        logger.warning("Interrupted!")
        return
    logger.info("Done!")
