#!python
# usage: uv run python -O -m src.perf_test_runner_for_py_ft [--runs N | --target-num-samples N | --quick]
#            [--shuffle | --no-shuffle] [--random-seed S]
#
# --runs N runs every (strategy, data size) of every challenge exactly N times (default 1); --runs 0 only refreshes
# results/<year>/challenge_strategy_registration.json.
# --quick runs each challenge's smallest sizes once, as a smoke test.
# --target-num-samples N instead runs only what is missing so that every feasible one has at least N timings in
# results/<year>/<environment>/<challenge>_py_ft_runs.csv, so an interrupted run can be repeated.

import logging
import sys

from spark_agg_methods_common_python.perf_test_runner_common import (
    discover_challenge_runners, parse_request, run_challenges_in_process,
)
from spark_agg_methods_common_python.utils.platform import setup_logging
from src import challenges

logger = logging.getLogger(__name__)

CHALLENGE_RUNNERS = discover_challenge_runners(challenges, "py_ft")


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
