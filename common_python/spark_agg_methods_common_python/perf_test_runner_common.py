"""Shared machinery of the per-engine ``perf_test_runner_for_*`` scripts.

Each script accepts ``--quick`` (a smoke test of each challenge's smallest sizes) and either ``--runs N`` (every
strategy and size exactly N more times; ``--runs 0`` only refreshes the strategy registrations) or
``--target-num-samples N`` (only what is missing so that every feasible strategy and size has at least N timings in
the challenge's run log, so an interrupted run can be repeated). Cells that return "infeasible" never get rows, so
they are always reported as short.
"""
import argparse
import importlib
import logging
import sys
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType

from spark_agg_methods_common_python.perf_test_common import Challenge, RunnerArgumentsBase, construct_result_file_path

logger = logging.getLogger(__name__)

QUICK_NUM_SIZES = 2

type Cell = tuple[str, str]  # (strategy name, size code)


@dataclass(frozen=True)
class ChallengeRunner:
    """A challenge runner module, exposing parse_args(), main() and a CHALLENGE constant.

    By convention the module also has ``DATA_SIZE_LIST_<CHALLENGE>_MAXIMAL`` and defines exactly one run log
    writer class (the one that declares ``RUN_LOG_FILE_NAME``); nothing else needs to be named.
    """
    module: ModuleType

    @property
    def name(self) -> str:
        return self.module.__name__.rsplit(".", 1)[-1]

    @property
    def challenge(self) -> Challenge:
        return self.module.CHALLENGE

    @property
    def size_list_name(self) -> str:
        """Module attribute holding what the runner logs."""
        return f"DATA_SIZE_LIST_{self.challenge.name}_MAXIMAL"

    @property
    def writer_class(self) -> type:
        writers = [
            x for x in vars(self.module).values()
            if isinstance(x, type) and x.__module__ == self.module.__name__ and hasattr(x, "RUN_LOG_FILE_NAME")
        ]
        if len(writers) != 1:
            raise TypeError(f"{self.module.__name__} must define exactly one run log writer, found {writers}")
        return writers[0]

    @property
    def quick_size_codes(self) -> list[str]:
        """The smallest sizes, for a quick smoke test: the first of the challenge's ``_MINIMAL`` list."""
        minimal = getattr(self.module, self.size_list_name.removesuffix("_MAXIMAL") + "_MINIMAL")
        return [x.size_code for x in minimal][:QUICK_NUM_SIZES]

    def timed_size_codes(self) -> set[str]:
        """The data sizes whose runs are logged; debugging-only sizes are run but never recorded."""
        return {x.size_code for x in getattr(self.module, self.size_list_name) if not x.debugging_only}

    def count_logged_runs(self) -> Counter[Cell]:
        """How many timings each (strategy, size code) already has in this challenge's run log."""
        sizes = [x for x in getattr(self.module, self.size_list_name) if not x.debugging_only]
        regressor_column = sizes[0].regressor_field_name()
        size_code_by_regressor = {str(x.regressor_value): x.size_code for x in sizes}
        log_path = construct_result_file_path(self.writer_class.RUN_LOG_FILE_NAME)  # ty: ignore[unresolved-attribute]
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


def discover_challenge_runners[T: ChallengeRunner](
        challenges_package: ModuleType,
        engine_suffix: str,
        runner_class: type[T] = ChallengeRunner,  # ty: ignore[invalid-parameter-default]
) -> list[T]:
    """Every ``<challenges_package>/<challenge>/<challenge>_runner_<engine_suffix>.py``, in challenge order."""
    root = Path(next(iter(challenges_package.__path__)))
    return [
        runner_class(module=importlib.import_module(f"{challenges_package.__name__}.{path.parent.name}.{path.stem}"))
        for path in sorted(root.glob(f"*/*_runner_{engine_suffix}.py"))
    ]


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
    quick: bool = False  # a smoke test: one run of each challenge's smallest sizes, unshuffled

    def argv_for(self, challenge: "ChallengeRunner") -> list[str]:
        """The command line arguments for one challenge's runner."""
        return [*self.runner_argv, *(["--size", *challenge.quick_size_codes] if self.quick else [])]


def parse_request(argv: list[str]) -> PerfRunnerRequest:
    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument("--runs", type=int, help="run every strategy and size exactly this many times (default 1)")
    parser.add_argument(
        "--target-num-samples", type=int,
        help="run only what is missing so that every feasible strategy and size has at least this many logged timings")
    parser.add_argument(
        "--quick", action="store_true",
        help="smoke test: one run (unless --runs says otherwise) of each challenge's smallest sizes, unshuffled")
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
        runner_argv=[*(["--no-shuffle"] if args.quick else []), *runner_argv, "--runs", str(num_runs)],
        quick=args.quick,
    )


def run_challenges_in_process(
        *,
        request: PerfRunnerRequest,
        challenges: list[ChallengeRunner],
) -> None:
    """Run each challenge's ``main()`` in this process, once per distinct shortfall when topping up."""
    program = sys.argv[0]
    for challenge in challenges:
        sys.argv = [program, *request.argv_for(challenge)]
        if not request.top_up:
            challenge.module.main()
            continue
        args = challenge.module.parse_args()
        timed_size_codes = challenge.timed_size_codes()
        remaining = missing_runs(args=args, timed_size_codes=timed_size_codes, logged=challenge.count_logged_runs())
        for shortfall in sorted(set(remaining.values()), reverse=True):
            cells = [cell for cell, n in remaining.items() if n == shortfall]
            sys.argv = [
                program, *request.argv_for(challenge), "--runs", str(shortfall),
                "--size", *[x for x in args.sizes if any(x == size_code for _, size_code in cells)],
                "--strategy", *[x for x in args.strategy_names if any(x == strategy for strategy, _ in cells)],
            ]
            challenge.module.main()
        sys.argv = [program, *request.argv_for(challenge)]
        short = missing_runs(args=args, timed_size_codes=timed_size_codes, logged=challenge.count_logged_runs())
        if remaining:
            logger.info(f"{challenge.name}: {len(short)} cells still below {args.num_runs} timings: {sorted(short)}")
