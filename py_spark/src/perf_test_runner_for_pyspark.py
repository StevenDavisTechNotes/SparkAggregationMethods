#!python
# usage: uv run python -O -m src.perf_test_runner_for_pyspark [--runs N | --target-num-samples N]
#            [--shuffle | --no-shuffle] [--random-seed S]
#
# --runs N runs every (strategy, data size) of every challenge exactly N times (default 1).
# --target-num-samples N instead runs only what is missing so that every feasible one has at least N timings in
# results/<year>/<challenge>_pyspark_runs.csv, so an interrupted run can be repeated. Cells that return "infeasible"
# never get rows, so they are always reported as short.
#
# The work runs in child processes so that each starts with a fresh JVM.
# On Windows Spark starts a Python worker process per task and leaks one listening socket for each for the life of
# the JVM. RDD strategies run the most tasks (measured: 130-270 leaked sockets per step, against 4-40 for DataFrame and
# SQL strategies), and enough of them in one JVM exhaust the ephemeral ports ("No buffer space available"). So on
# Windows every RDD step gets a child process of its own, and each challenge's remaining steps share one child.
# Elsewhere (Linux, WSL) Spark forks workers from a daemon and does not leak, so each challenge runs in one child.
import argparse
import logging
import subprocess
import sys
from collections import Counter
from dataclasses import dataclass
from types import ModuleType

from spark_agg_methods_common_python.perf_test_common import (
    RunnerArgumentsBase, SolutionInterfacePySpark, assemble_itinerary, construct_result_file_path,
)
from spark_agg_methods_common_python.utils.platform import setup_logging
from src.challenges.bi_level import bi_level_runner_pyspark
from src.challenges.conditional import conditional_runner_pyspark
from src.challenges.deduplication import dedupe_runner_pyspark
from src.challenges.sectional import section_runner_pyspark
from src.challenges.vanilla import vanilla_runner_pyspark

logger = logging.getLogger(__name__)

type Cell = tuple[str, str]  # (strategy name, size code)


@dataclass(frozen=True)
class ChallengeRunner:
    module: ModuleType
    strategy_registry_name: str  # module attributes holding what the runner registers and logs
    size_list_name: str
    writer_class_name: str

    @property
    def name(self) -> str:
        return self.module.__name__.rsplit(".", 1)[-1]

    def rdd_strategy_names(self) -> set[str]:
        return {
            x.strategy_name
            for x in getattr(self.module, self.strategy_registry_name)
            if x.interface == SolutionInterfacePySpark.PYSPARK_RDD
        }

    def timed_size_codes(self) -> set[str]:
        """The data sizes whose runs are logged; debugging-only sizes are run but never recorded."""
        return {x.size_code for x in getattr(self.module, self.size_list_name) if not x.debugging_only}

    def count_logged_runs(self) -> Counter[Cell]:
        """How many timings each (strategy, size code) already has in this challenge's run log."""
        sizes = [x for x in getattr(self.module, self.size_list_name) if not x.debugging_only]
        regressor_column = sizes[0].regressor_field_name()
        size_code_by_regressor = {str(x.regressor_value): x.size_code for x in sizes}
        log_path = construct_result_file_path(getattr(self.module, self.writer_class_name).RUN_LOG_FILE_NAME)
        counts: Counter[Cell] = Counter()
        if not log_path.exists():
            return counts
        columns: list[str] = []
        for line in log_path.read_text().splitlines():
            if line.startswith(" "):  # a header; result lines never start with a blank
                columns = line.strip().split(",")
            elif columns and "," in line and not line.startswith("#"):
                row = dict(zip(columns, line.split(",")))
                size_code = size_code_by_regressor.get(row.get(regressor_column, ""))
                if size_code is not None:
                    counts[(row["strategy_name"], size_code)] += 1
        return counts


CHALLENGE_RUNNERS = (
    ChallengeRunner(
        bi_level_runner_pyspark, "BI_LEVEL_STRATEGY_REGISTRY_PYSPARK", "DATA_SIZE_LIST_BI_LEVEL_MAXIMAL",
        "BiLevelPysparkRunResultFileWriter"),
    ChallengeRunner(
        conditional_runner_pyspark, "CONDITIONAL_STRATEGY_REGISTRY_PYSPARK", "DATA_SIZE_LIST_CONDITIONAL_MAXIMAL",
        "ConditionalPysparkRunResultFileWriter"),
    ChallengeRunner(
        dedupe_runner_pyspark, "DEDUPE_STRATEGY_REGISTRY_PYSPARK", "DATA_SIZE_LIST_DEDUPE_MAXIMAL",
        "DedupePysparkRunResultFileWriter"),
    ChallengeRunner(
        section_runner_pyspark, "SECTIONAL_STRATEGY_REGISTRY_PYSPARK", "DATA_SIZE_LIST_SECTIONAL_MAXIMAL",
        "SectionPysparkRunResultFileWriter"),
    ChallengeRunner(
        vanilla_runner_pyspark, "VANILLA_STRATEGY_REGISTRY_PYSPARK", "DATA_SIZE_LIST_VANILLA_MAXIMAL",
        "VanillaPysparkRunResultFileWriter"),
)


def leaks_sockets_per_python_worker() -> bool:
    return sys.platform == "win32"  # WSL reports "linux"


def missing_runs(args: RunnerArgumentsBase, timed_size_codes: set[str], logged: Counter[Cell]) -> dict[Cell, int]:
    shortfalls = {
        (strategy_name, size_code): args.num_runs - logged[(strategy_name, size_code)]
        for strategy_name in args.strategy_names
        for size_code in args.sizes
        if size_code in timed_size_codes
    }
    return {cell: n for cell, n in shortfalls.items() if n > 0}


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
    remaining = missing_runs(args, timed_size_codes, logged)
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


@dataclass(frozen=True)
class PerfRunnerRequest:
    num_runs: int  # the exact number of runs per cell, or with ``top_up`` the number of timings to end up with
    top_up: bool
    runner_argv: list[str]  # what the challenge runners' own parse_args should see


def parse_request(argv: list[str]) -> PerfRunnerRequest:
    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument("--runs", type=int, help="run every strategy and size exactly this many times (default 1)")
    parser.add_argument(
        "--target-num-samples", type=int,
        help="run only what is missing so that every feasible strategy and size has at least this many logged timings")
    args, runner_argv = parser.parse_known_args(argv)
    if args.runs is not None and args.target_num_samples is not None:
        parser.error("--runs and --target-num-samples cannot be specified together")
    top_up = args.target_num_samples is not None
    num_runs = args.target_num_samples if top_up else 1 if args.runs is None else args.runs
    if num_runs < 0:
        parser.error("the number of runs or samples cannot be negative")
    return PerfRunnerRequest(num_runs=num_runs, top_up=top_up, runner_argv=[*runner_argv, "--runs", str(num_runs)])


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
        children = plan_children(args, challenge.rdd_strategy_names(), timed_size_codes, logged)
        logger.info(f"{challenge.name}: {len(children)} child processes to run")
        for index, child_args in enumerate(children):
            description = f"{challenge.name} {' '.join(child_args)}"
            logger.info(f"Child {index + 1}/{len(children)} of {challenge.name}: {' '.join(child_args)}")
            if not run_child(challenge, child_args):
                logger.error(f"Child failed: {description}")
                failures.append(description)
        if request.top_up and children:
            short = missing_runs(args, timed_size_codes, challenge.count_logged_runs())
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
