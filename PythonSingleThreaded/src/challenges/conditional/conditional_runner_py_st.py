import logging
import os

from spark_agg_methods_common_python.challenges.conditional.conditional_record_runs import (
    ConditionalPythonRunResultFileWriter,
)
from spark_agg_methods_common_python.perf_test_common import CalcEngine

logger = logging.getLogger(__name__)

ENGINE = CalcEngine.SINGLE_THREADED


class ConditionalPythonSTRunResultFileWriter(ConditionalPythonRunResultFileWriter):
    RUN_LOG_FILE_PATH: str = os.path.abspath('results/conditional_python_single_threaded_runs.csv')

    def __init__(self):
        super().__init__(
            engine=ENGINE,
            rel_log_file_path=__class__.RUN_LOG_FILE_PATH,
        )

# TODO: Implement the main function


def main() -> None:
    logger.info(f"Running {__file__}")
    try:
        #     args = parse_args()
        #     update_challenge_registration()
        #     do_test_runs(args)
        pass
    except KeyboardInterrupt:
        logger.warning("Interrupted!")
        return
    logger.info("Done!")
