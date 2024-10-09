#! python
# usage: python -m src.challenges.vanilla.vanilla_python_only_runner
import argparse
import gc
import logging
import random
import time
from dataclasses import dataclass

from challenges.vanilla.vanilla_test_data_types import DATA_SIZES_LIST_VANILLA
from src.challenges.vanilla.vanilla_record_runs import VanillaPythonRunResultFileWriter, VanillaRunResult
from src.challenges.vanilla.vanilla_strategy_directory import STRATEGIES_USING_PYTHON_ONLY_REGISTRY
from src.perf_test_common import CalcEngine
from src.six_field_test_data.six_generate_test_data import (
    ChallengeMethodPythonOnlyRegistration, DataSetPythonOnlyWithAnswer, populate_data_set_python_only,
)
from src.six_field_test_data.six_runner_base import test_one_step_in_python_only_itinerary
from src.six_field_test_data.six_test_data_types import (
    SHARED_LOCAL_TEST_DATA_FILE_LOCATION, Challenge, ExecutionParameters,
)
from src.utils.utils import always_true, set_random_seed

CHALLENGE = Challenge.VANILLA


logger = logging.getLogger(__name__)
ENGINE = CalcEngine.PYTHON_ONLY

DEBUG_ARGS = None if True else (
    []
    # + '--size 3_3_10'.split()
    + '--runs 10'.split()
    # + '--random-seed 1234'.split()
    + ['--no-shuffle']
    + ['--strategy',
       'vanilla_py_only_pd_grp_numpy',
       'vanilla_py_only_pd_grp_numba',
       ]
)


@dataclass(frozen=True)
class Arguments:
    num_runs: int
    random_seed: int | None
    shuffle: bool
    sizes: list[str]
    strategy_names: list[str]
    exec_params: ExecutionParameters


def parse_args() -> Arguments:
    sizes = [x.size_code for x in DATA_SIZES_LIST_VANILLA]
    strategy_names = [x.strategy_name for x in STRATEGIES_USING_PYTHON_ONLY_REGISTRY]

    parser = argparse.ArgumentParser()
    parser.add_argument('--random-seed', type=int)
    parser.add_argument('--runs', type=int, default=30)
    parser.add_argument(
        '--size',
        choices=sizes,
        default=sizes,
        nargs="+")
    parser.add_argument(
        '--shuffle', default=True,
        action=argparse.BooleanOptionalAction)
    parser.add_argument(
        '--strategy',
        choices=strategy_names,
        default=strategy_names,
        nargs="+")
    if DEBUG_ARGS is None:
        args = parser.parse_args()
    else:
        args = parser.parse_args(DEBUG_ARGS)
    return Arguments(
        num_runs=args.runs,
        random_seed=args.random_seed,
        shuffle=args.shuffle,
        sizes=args.size,
        strategy_names=args.strategy,
        exec_params=ExecutionParameters(
            DefaultParallelism=1,
            TestDataFolderLocation=SHARED_LOCAL_TEST_DATA_FILE_LOCATION,
        ),
    )


def do_test_runs(
        args: Arguments,
) -> None:
    data_sets = populate_data_sets(args)
    keyed_implementation_list = {
        x.strategy_name: x for x in STRATEGIES_USING_PYTHON_ONLY_REGISTRY}
    itinerary: list[tuple[ChallengeMethodPythonOnlyRegistration, DataSetPythonOnlyWithAnswer]] = [
        (challenge_method_registration, data_set)
        for strategy in args.strategy_names
        if always_true(challenge_method_registration := keyed_implementation_list[strategy])
        for data_set in data_sets
        for _ in range(0, args.num_runs)
    ]
    if args.random_seed is not None:
        set_random_seed(args.random_seed)
    if args.shuffle:
        random.shuffle(itinerary)
    with VanillaPythonRunResultFileWriter(ENGINE) as file:
        for index, (challenge_method_registration, data_set) in enumerate(itinerary):
            logger.info(
                "Working on %d of %d" % (index, len(itinerary)))
            print(f"Working on {challenge_method_registration.strategy_name} for {data_set.description.size_code}")
            base_run_result = test_one_step_in_python_only_itinerary(
                challenge=CHALLENGE,
                exec_params=args.exec_params,
                challenge_method_registration=challenge_method_registration,
                numerical_tolerance=challenge_method_registration.numerical_tolerance,
                data_set=data_set,
                correct_answer=data_set.answer,
            )
            if base_run_result is None:
                continue
            file.write_run_result(challenge_method_registration, VanillaRunResult(
                engine=ENGINE,
                num_data_points=data_set.description.num_data_points,
                elapsed_time=base_run_result.elapsed_time,
                record_count=base_run_result.record_count,
            ))
            gc.collect()
            time.sleep(0.1)


def populate_data_sets(
        args: Arguments,
) -> list[DataSetPythonOnlyWithAnswer]:
    data_sets = [
        populate_data_set_python_only(
            exec_params=args.exec_params,
            data_size=size,
        )
        for size in DATA_SIZES_LIST_VANILLA
        if size.size_code in args.sizes
    ]
    return data_sets


def main():
    args = parse_args()
    do_test_runs(args)


if __name__ == "__main__":
    main()
    print("Done!")
