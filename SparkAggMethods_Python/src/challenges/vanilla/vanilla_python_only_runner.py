#! python
# usage: cd src; python -m challenges.vanilla.vanilla_python_only_runner ; cd ..
import argparse
import gc
import logging
import os
import random
import time
from dataclasses import dataclass
from typing import Optional

from challenges.vanilla.vanilla_record_runs import derive_run_log_file_path
from challenges.vanilla.vanilla_strategy_directory import (
    SOLUTIONS_USING_PYTHON_ONLY_REGISTRY, STRATEGY_NAME_LIST_PYTHON_ONLY)
from challenges.vanilla.vanilla_test_data_types import SIZES_LIST_VANILLA
from perf_test_common import CalcEngine
from six_field_test_data.six_generate_test_data import (
    ChallengeMethodPythonOnlyRegistration, DataSetPythonOnlyWithAnswer,
    populate_data_set_python_only)
from six_field_test_data.six_run_result_types import write_header
from six_field_test_data.six_runner_base import \
    test_one_step_in_python_only_itinerary
from six_field_test_data.six_test_data_types import (
    SHARED_LOCAL_TEST_DATA_FILE_LOCATION, Challenge, ExecutionParameters)
from utils.tidy_spark_session import get_python_code_root_path
from utils.utils import always_true, set_random_seed

logger = logging.getLogger(__name__)
ENGINE = CalcEngine.PYTHON_ONLY
CHALLENGE = Challenge.VANILLA
DEBUG_ARGS = None if False else (
    []
    # + '--size 3_3_10'.split()
    + '--runs 1'.split()
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
    random_seed: Optional[int]
    shuffle: bool
    sizes: list[str]
    strategy_names: list[str]
    exec_params: ExecutionParameters


def parse_args() -> Arguments:
    sizes = [x.size_code for x in SIZES_LIST_VANILLA]

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
        choices=STRATEGY_NAME_LIST_PYTHON_ONLY,
        default=STRATEGY_NAME_LIST_PYTHON_ONLY,
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
        x.strategy_name: x for x in SOLUTIONS_USING_PYTHON_ONLY_REGISTRY}
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
    result_log_path_name = os.path.join(
        get_python_code_root_path(),
        derive_run_log_file_path(ENGINE))
    with open(result_log_path_name, 'at+') as file:
        write_header(file)
        for index, (challenge_method_registration, data_set) in enumerate(itinerary):
            logger.info(
                "Working on %d of %d" % (index, len(itinerary)))
            print(f"Working on {challenge_method_registration.strategy_name} for {data_set.data_size.size_code}")
            test_one_step_in_python_only_itinerary(
                challenge=CHALLENGE,
                exec_params=args.exec_params,
                challenge_method_registration=challenge_method_registration,
                numerical_tolerance=challenge_method_registration.numerical_tolerance,
                file=file,
                data_set=data_set,
                correct_answer=data_set.answer,
            )
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
        for size in SIZES_LIST_VANILLA
        if size.size_code in args.sizes
    ]
    return data_sets


def main():
    args = parse_args()
    do_test_runs(args)


if __name__ == "__main__":
    main()
    print("Done!")
