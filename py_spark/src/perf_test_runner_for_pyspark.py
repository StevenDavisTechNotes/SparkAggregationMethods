#!python
# usage: uv run python -O -m src.perf_test_runner_for_pyspark [--runs N | --target-num-samples N]
#            [--shuffle | --no-shuffle] [--random-seed S]
#
# --runs N runs every (strategy, data size) of every challenge exactly N times (default 1).
# --target-num-samples N instead runs only what is missing so that every feasible one has at least N timings in
# results/<year>/<environment>/<challenge>_pyspark_runs.csv, so an interrupted run can be repeated. Cells that return
# "infeasible" never get rows, so they are always reported as short.
#
# The work runs in child processes so that each starts with a fresh JVM.
# On Windows Spark starts a Python worker process per task and leaks one listening socket for each for the life of
# the JVM. RDD strategies run the most tasks (measured: 130-270 leaked sockets per step, against 4-40 for DataFrame and
# SQL strategies), and enough of them in one JVM exhaust the ephemeral ports ("No buffer space available"). So on
# Windows every RDD step gets a child process of its own, and each challenge's remaining steps share one child.
# Elsewhere (Linux, WSL) Spark forks workers from a daemon and does not leak, so each challenge runs in one child.
import logging
import subprocess
import sys
from collections import Counter
from dataclasses import dataclass

from spark_agg_methods_common_python.perf_test_common import (
    RunnerArgumentsBase, SolutionInterfacePySpark, assemble_itinerary,
)
from spark_agg_methods_common_python.perf_test_runner_common import Cell, ChallengeRunner, missing_runs, parse_request
from spark_agg_methods_common_python.utils.platform import setup_logging
from src.challenges.bilevel import bilevel_runner_pyspark
from src.challenges.conditional import conditional_runner_pyspark
from src.challenges.dedupe import dedupe_runner_pyspark
from src.challenges.sectional import section_runner_pyspark
from src.challenges.vanilla import vanilla_runner_pyspark

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PysparkChallengeRunner(ChallengeRunner):
    strategy_registry_name: str  # module attribute holding what the runner registers

    def rdd_strategy_names(self) -> set[str]:
        return {
            x.strategy_name
            for x in getattr(self.module, self.strategy_registry_name)
            if x.interface == SolutionInterfacePySpark.PYSPARK_RDD
        }


CHALLENGE_RUNNERS = (
    PysparkChallengeRunner(
        module=bilevel_runner_pyspark,
        size_list_name="DATA_SIZE_LIST_BILEVEL_MAXIMAL",
        writer_class_name="BilevelPysparkRunResultFileWriter",
        strategy_registry_name="BILEVEL_STRATEGY_REGISTRY_PYSPARK",
    ),
    PysparkChallengeRunner(
        module=conditional_runner_pyspark,
        size_list_name="DATA_SIZE_LIST_CONDITIONAL_MAXIMAL",
        writer_class_name="ConditionalPysparkRunResultFileWriter",
        strategy_registry_name="CONDITIONAL_STRATEGY_REGISTRY_PYSPARK",
    ),
    PysparkChallengeRunner(
        module=dedupe_runner_pyspark,
        size_list_name="DATA_SIZE_LIST_DEDUPE_MAXIMAL",
        writer_class_name="DedupePysparkRunResultFileWriter",
        strategy_registry_name="DEDUPE_STRATEGY_REGISTRY_PYSPARK",
    ),
    PysparkChallengeRunner(
        module=section_runner_pyspark,
        size_list_name="DATA_SIZE_LIST_SECTIONAL_MAXIMAL",
        writer_class_name="SectionPysparkRunResultFileWriter",
        strategy_registry_name="SECTIONAL_STRATEGY_REGISTRY_PYSPARK",
    ),
    PysparkChallengeRunner(
        module=vanilla_runner_pyspark,
        size_list_name="DATA_SIZE_LIST_VANILLA_MAXIMAL",
        writer_class_name="VanillaPysparkRunResultFileWriter",
        strategy_registry_name="VANILLA_STRATEGY_REGISTRY_PYSPARK",
    ),
)


def leaks_sockets_per_python_worker() -> bool:
    return sys.platform == "win32"  # WSL reports "linux"


def plan_children(
        args: RunnerArgumentsBase,
        rdd_strategy_names: set[str],
        timed_size_codes: set[str],
        logged: Counter[Cell],
) -> list[list[str]]:
    """The command line arguments of each child process needed to reach ``args.num_runs`` timings per cell.

    Cells that share a shortfall run together in one child, so that child also gives their cross product that many
    runs; a cell that already had more than its share simply ends up with extra timings.
    """
    if not leaks_sockets_per_python_worker():
        rdd_strategy_names = set()
    remaining = missing_runs(
        args=args,
        timed_size_codes=timed_size_codes,
        logged=logged,
    )
    children: list[list[str]] = []
    for shortfall in sorted(set(remaining.values()), reverse=True):
        cells = [
            cell for cell, n in remaining.items()
            if n == shortfall and cell[0] not in rdd_strategy_names
        ]
        if not cells:
            continue
        children.append([
            "--runs", str(shortfall),
            "--shuffle" if args.shuffle else "--no-shuffle",
            *(["--random-seed", str(args.random_seed)] if args.random_seed is not None else []),
            "--size", *[x for x in args.sizes if any(x == size_code for _, size_code in cells)],
            "--strategy", *[x for x in args.strategy_names if any(x == strategy for strategy, _ in cells)],
        ])
    for strategy_name, size_code in assemble_itinerary(args):
        if strategy_name in rdd_strategy_names and remaining.get((strategy_name, size_code), 0) > 0:
            remaining[(strategy_name, size_code)] -= 1
            children.append(["--runs", "1", "--no-shuffle", "--size", size_code, "--strategy", strategy_name])
    return children


def run_child(challenge: ChallengeRunner, child_args: list[str]) -> bool:
    command = [
        sys.executable,
        *(["-" + "O" * sys.flags.optimize] if sys.flags.optimize else []),
        "-m", challenge.module.__name__,
        *child_args,
    ]
    return subprocess.run(command, check=False).returncode == 0


def main() -> None:
    logger.info(f"Running {__file__}")
    request = parse_request(sys.argv[1:])
    sys.argv[1:] = request.runner_argv
    failures: list[str] = []
    for challenge in CHALLENGE_RUNNERS:
        args = challenge.module.parse_args()
        # an exact count ignores the run log; debugging-only sizes are never logged, so only top-up skips them
        timed_size_codes = challenge.timed_size_codes() if request.top_up else set(args.sizes)
        logged = challenge.count_logged_runs() if request.top_up else Counter()
        children = plan_children(
            args=args,
            rdd_strategy_names=challenge.rdd_strategy_names(),
            timed_size_codes=timed_size_codes,
            logged=logged,
        )
        logger.info(f"{challenge.name}: {len(children)} child processes to run")
        for index, child_args in enumerate(children):
            description = f"{challenge.name} {' '.join(child_args)}"
            logger.info(f"Child {index + 1}/{len(children)} of {challenge.name}: {' '.join(child_args)}")
            if not run_child(
                challenge=challenge,
                child_args=child_args,
            ):
                logger.error(f"Child failed: {description}")
                failures.append(description)
        if request.top_up and children:
            short = missing_runs(
                args=args,
                timed_size_codes=timed_size_codes,
                logged=challenge.count_logged_runs(),
            )
            logger.info(f"{challenge.name}: {len(short)} cells still below {args.num_runs} timings: {sorted(short)}")
    if failures:
        logger.error(f"{len(failures)} child processes failed:\n" + "\n".join(failures))
        sys.exit(1)
    logger.info("Done!")


if __name__ == "__main__":
    setup_logging()
    try:
        main()
    except KeyboardInterrupt:
        logger.warning("Interrupted!")
