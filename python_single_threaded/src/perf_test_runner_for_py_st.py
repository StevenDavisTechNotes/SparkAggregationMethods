#!python
# usage: uv run python -O -m src.perf_test_runner_for_py_st [--runs N | --target-num-samples N]
#            [--shuffle | --no-shuffle] [--random-seed S]
#
# --runs N runs every (strategy, data size) of every challenge exactly N times (default 1).
# --target-num-samples N instead runs only what is missing so that every feasible one has at least N timings in
# results/<year>/<environment>/<challenge>_py_st_runs.csv, so an interrupted run can be repeated.

import logging
import sys

from spark_agg_methods_common_python.perf_test_runner_common import (
    ChallengeRunner, parse_request, run_challenges_in_process,
)
from spark_agg_methods_common_python.utils.platform import setup_logging
from src.challenges.bi_level import bi_level_runner_py_st
from src.challenges.conditional import conditional_runner_py_st
# from src.challenges.deduplication import dedupe_runner_py_st
from src.challenges.sectional import section_runner_py_st
from src.challenges.vanilla import vanilla_runner_py_st

logger = logging.getLogger(__name__)

CHALLENGE_RUNNERS = [
    ChallengeRunner(
        module=bi_level_runner_py_st,
        size_list_name="DATA_SIZE_LIST_BI_LEVEL_MAXIMAL",
        writer_class_name="BiLevelPythonSTRunResultFileWriter",
    ),
    ChallengeRunner(
        module=conditional_runner_py_st,
        size_list_name="DATA_SIZE_LIST_CONDITIONAL_MAXIMAL",
        writer_class_name="ConditionalPythonSTRunResultFileWriter",
    ),
    # dedupe_runner_py_st  TODO: Implement this
    ChallengeRunner(
        module=section_runner_py_st,
        size_list_name="DATA_SIZE_LIST_SECTIONAL_MAXIMAL",
        writer_class_name="SectionPythonSTRunResultFileWriter",
    ),
    ChallengeRunner(
        module=vanilla_runner_py_st,
        size_list_name="DATA_SIZE_LIST_VANILLA_MAXIMAL",
        writer_class_name="VanillaPythonSTRunResultFileWriter",
    ),
]


def main() -> None:
    logger.info(f"Running {__file__}")
    run_challenges_in_process(
        request=parse_request(sys.argv[1:]),
        challenges=CHALLENGE_RUNNERS,
    )
    logger.info("Done!")


if __name__ == "__main__":
    setup_logging()
    try:
        main()
    except KeyboardInterrupt:
        logger.warning("Interrupted!")
