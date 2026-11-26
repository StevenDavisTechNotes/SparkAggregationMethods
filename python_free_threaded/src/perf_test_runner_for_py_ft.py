#!python
# usage: .\.venv\Scripts\activate.ps1; python -O -m src.perf_test_runner_for_py_ft [--runs N | --target-num-samples N]
#            [--shuffle | --no-shuffle] [--random-seed S]
#
# --runs N runs every (strategy, data size) of every challenge exactly N times (default 1).
# --target-num-samples N instead runs only what is missing so that every feasible one has at least N timings in
# results/<year>/<challenge>_py_ft_runs.csv, so an interrupted run can be repeated.

import logging
import sys

from spark_agg_methods_common_python.perf_test_runner_common import (
    ChallengeRunner, parse_request, run_challenges_in_process,
)
from spark_agg_methods_common_python.utils.platform import setup_logging
from src.challenges.vanilla import vanilla_runner_py_ft

logger = logging.getLogger(__name__)

CHALLENGE_RUNNERS = [
    ChallengeRunner(
        module=vanilla_runner_py_ft,
        size_list_name="DATA_SIZE_LIST_VANILLA_MAXIMAL",
        writer_class_name="VanillaPythonFreeThreadedRunResultFileWriter",
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
