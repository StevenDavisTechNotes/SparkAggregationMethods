#!python
# usage: uv run python -O -m src.perf_test_runner_for_pyspark [--runs N] [--shuffle | --no-shuffle] [--random-seed S]
#
# Runs every challenge's itinerary, each challenge in a child process of its own so its JVM starts fresh.
#
# On Windows Spark leaks one listening socket per Python worker for the life of the JVM, so a single JVM runs out of
# ports after roughly 50k tasks. Only the conditional challenge has been seen to get there (its shuffles use
# 1000-partition stages); it therefore runs one benchmark step per child process, and warms its strategy up first
# (see conditional_runner_pyspark.warm_up_strategy) because that fresh JVM is otherwise cold.
import logging
import subprocess
import sys
from types import ModuleType

from spark_agg_methods_common_python.perf_test_common import assemble_itinerary
from spark_agg_methods_common_python.utils.platform import setup_logging
from src.challenges.bi_level import bi_level_runner_pyspark
from src.challenges.conditional import conditional_runner_pyspark
from src.challenges.deduplication import dedupe_runner_pyspark
from src.challenges.sectional import section_runner_pyspark
from src.challenges.vanilla import vanilla_runner_pyspark

logger = logging.getLogger(__name__)

RUNNERS = (
    bi_level_runner_pyspark,
    conditional_runner_pyspark,
    dedupe_runner_pyspark,
    section_runner_pyspark,
    vanilla_runner_pyspark,
)
RUNNERS_WITH_PROCESS_PER_STEP = frozenset({conditional_runner_pyspark})


def run_child(runner: ModuleType, runner_args: list[str]) -> bool:
    command = [
        sys.executable,
        *(["-" + "O" * sys.flags.optimize] if sys.flags.optimize else []),
        "-m", runner.__name__,
        *runner_args,
    ]
    return subprocess.run(command, check=False).returncode == 0


def run_challenge(runner: ModuleType) -> list[str]:
    """Run one challenge; returns a description of each failed child process."""
    if runner not in RUNNERS_WITH_PROCESS_PER_STEP:
        return run_challenge_shared_py_spark_session(runner)
    else:
        return run_challenge_new_py_spark_session_each(runner)


def run_challenge_shared_py_spark_session(runner: ModuleType) -> list[str]:
    challenge = runner.__name__.rsplit(".", 1)[-1]
    logger.info(f"Running {challenge} in one process")
    return [] if run_child(runner, sys.argv[1:]) else [challenge]


def run_challenge_new_py_spark_session_each(runner: ModuleType) -> list[str]:
    challenge = runner.__name__.rsplit(".", 1)[-1]
    args = runner.parse_args()
    runner.update_challenge_registration()
    itinerary = assemble_itinerary(args)
    failed: list[str] = []
    for index, (strategy_name, size_code) in enumerate(itinerary):
        description = f"{strategy_name} for {size_code}"
        logger.info(f"Step {index}/{len(itinerary)} of {challenge}: {description}")
        if not run_child(runner, [
            "--runs", "1",
            "--no-shuffle",
            "--size", size_code,
            "--strategy", strategy_name,
        ]):
            logger.error(f"Step {index}/{len(itinerary)} of {challenge} failed: {description}")
            failed.append(description)
    return failed


def main() -> None:
    logger.info(f"Running {__file__}")
    failures = [failure for runner in RUNNERS for failure in run_challenge(runner)]
    if failures:
        logger.error(f"{len(failures)} failed: {failures}")
        sys.exit(1)
    logger.info("Done!")


if __name__ == "__main__":
    setup_logging()
    try:
        main()
    except KeyboardInterrupt:
        logger.warning("Interrupted!")
