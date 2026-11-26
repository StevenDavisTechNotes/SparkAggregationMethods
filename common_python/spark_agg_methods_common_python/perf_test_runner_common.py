"""Shared machinery of the per-engine ``perf_test_runner_for_*`` scripts.

Each script accepts either ``--runs N`` (every strategy and size exactly N more times) or
``--target-num-samples N`` (only what is missing so that every feasible strategy and size has at least N timings in
the challenge's run log, so an interrupted run can be repeated). Cells that return "infeasible" never get rows, so
they are always reported as short.
"""
import argparse
import logging
import sys
from collections import Counter
from dataclasses import dataclass
from types import ModuleType

from spark_agg_methods_common_python.perf_test_common import RunnerArgumentsBase, construct_result_file_path

logger = logging.getLogger(__name__)

type Cell = tuple[str, str]  # (strategy name, size code)


@dataclass(frozen=True)
class ChallengeRunner:
    module: ModuleType  # a challenge runner module, exposing parse_args() and main()
    size_list_name: str  # module attributes holding what the runner logs
    writer_class_name: str

    @property
    def name(self) -> str:
        return self.module.__name__.rsplit(".", 1)[-1]

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


def missing_runs(args: RunnerArgumentsBase, timed_size_codes: set[str], logged: Counter[Cell]) -> dict[Cell, int]:
    shortfalls = {
        (strategy_name, size_code): args.num_runs - logged[(strategy_name, size_code)]
        for strategy_name in args.strategy_names
        for size_code in args.sizes
        if size_code in timed_size_codes
    }
    return {cell: n for cell, n in shortfalls.items() if n > 0}


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
    num_runs = (
        args.target_num_samples if args.target_num_samples is not None else
        args.runs if args.runs is not None else
        1
    )
    if num_runs < 0:
        parser.error("the number of runs or samples cannot be negative")
    return PerfRunnerRequest(
        num_runs=num_runs,
        top_up=top_up,
        runner_argv=[*runner_argv, "--runs", str(num_runs)],
    )


def run_challenges_in_process(
        *,
        request: PerfRunnerRequest,
        challenges: list[ChallengeRunner],
) -> None:
    """Run each challenge's ``main()`` in this process, once per distinct shortfall when topping up."""
    program = sys.argv[0]
    for challenge in challenges:
        sys.argv = [program, *request.runner_argv]
        if not request.top_up:
            challenge.module.main()
            continue
        args = challenge.module.parse_args()
        timed_size_codes = challenge.timed_size_codes()
        remaining = missing_runs(args=args, timed_size_codes=timed_size_codes, logged=challenge.count_logged_runs())
        for shortfall in sorted(set(remaining.values()), reverse=True):
            cells = [cell for cell, n in remaining.items() if n == shortfall]
            sys.argv = [
                program, *request.runner_argv, "--runs", str(shortfall),
                "--size", *[x for x in args.sizes if any(x == size_code for _, size_code in cells)],
                "--strategy", *[x for x in args.strategy_names if any(x == strategy for strategy, _ in cells)],
            ]
            challenge.module.main()
        sys.argv = [program, *request.runner_argv]
        short = missing_runs(args=args, timed_size_codes=timed_size_codes, logged=challenge.count_logged_runs())
        if remaining:
            logger.info(f"{challenge.name}: {len(short)} cells still below {args.num_runs} timings: {sorted(short)}")
