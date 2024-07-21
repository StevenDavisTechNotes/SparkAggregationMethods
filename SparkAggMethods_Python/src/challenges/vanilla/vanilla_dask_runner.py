#! python
# usage: python -m src.challenges.vanilla.vanilla_dask_runner

import argparse
import gc
import random
import time
from typing import NamedTuple

from src.challenges.vanilla.vanilla_record_runs import derive_run_log_file_path
from src.challenges.vanilla.vanilla_strategy_directory import (
    SOLUTIONS_USING_DASK_REGISTRY, STRATEGY_NAME_LIST_DASK)
from src.challenges.vanilla.vanilla_test_data_types import SIZES_LIST_VANILLA
from src.perf_test_common import CalcEngine
from src.six_field_test_data.six_generate_test_data import (
    ChallengeMethodPythonDaskRegistration, DataSetDaskWithAnswer,
    populate_data_set_dask)
from src.six_field_test_data.six_run_result_types import write_header
from src.six_field_test_data.six_runner_base import \
    test_one_step_in_dask_itinerary
from src.six_field_test_data.six_test_data_types import (
    SHARED_LOCAL_TEST_DATA_FILE_LOCATION, Challenge, ExecutionParameters)
from src.utils.tidy_spark_session import LOCAL_NUM_EXECUTORS
from src.utils.utils import always_true, set_random_seed

ENGINE = CalcEngine.DASK
CHALLENGE = Challenge.VANILLA

DEBUG_ARGS = None if False else (
    []
    + '--size 3_3_10'.split()
    + '--runs 1'.split()
    # + '--random-seed 1234'.split()
    + ['--no-shuffle']
    # + ['--strategy',
    #    'vanilla_dask_ddf_grp_apply',
    #    'vanilla_dask_ddf_grp_udaf',
    #    'vanilla_dask_sql_no_gpu',
    #    ]
)


class Arguments(NamedTuple):
    have_gpu: bool
    num_runs: int
    random_seed: int | None
    shuffle: bool
    sizes: list[str]
    strategies: list[str]
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
    parser.add_argument('--shuffle', default=True, action=argparse.BooleanOptionalAction)
    parser.add_argument('--have-gpu', default=True, action=argparse.BooleanOptionalAction)
    parser.add_argument(
        '--strategy',
        choices=STRATEGY_NAME_LIST_DASK,
        default=STRATEGY_NAME_LIST_DASK,
        nargs="+")
    if DEBUG_ARGS is None:
        args = parser.parse_args()
    else:
        args = parser.parse_args(DEBUG_ARGS)
    return Arguments(
        have_gpu=args.have_gpu,
        num_runs=args.runs,
        random_seed=args.random_seed,
        shuffle=args.shuffle,
        sizes=args.size,
        strategies=args.strategy,
        exec_params=ExecutionParameters(
            DefaultParallelism=2 * LOCAL_NUM_EXECUTORS,
            TestDataFolderLocation=SHARED_LOCAL_TEST_DATA_FILE_LOCATION,
        ),
    )


def do_test_runs(
        args: Arguments,
) -> None:
    data_sets = populate_data_sets(args)
    keyed_implementation_list = {
        x.strategy_name: x for x in SOLUTIONS_USING_DASK_REGISTRY}
    itinerary: list[tuple[ChallengeMethodPythonDaskRegistration, DataSetDaskWithAnswer]] = [
        (challenge_method_registration, data_set)
        for strategy in args.strategies
        if always_true(challenge_method_registration := keyed_implementation_list[strategy])
        if (args.have_gpu or not challenge_method_registration.requires_gpu)
        for data_set in data_sets
        for _ in range(0, args.num_runs)
    ]
    if args.random_seed is not None:
        set_random_seed(args.random_seed)
    if args.shuffle:
        random.shuffle(itinerary)
    with open(derive_run_log_file_path(ENGINE), 'at+') as file:
        write_header(file)
        for index, (challenge_method_registration, data_set) in enumerate(itinerary):
            print("Working on %d of %d" % (index, len(itinerary)))
            print(f"Working on {challenge_method_registration.strategy_name} for {data_set.data_size.size_code}")
            test_one_step_in_dask_itinerary(
                challenge=CHALLENGE,
                exec_params=args.exec_params,
                challenge_method_registration=challenge_method_registration,
                file=file,
                data_set=data_set,
            )
            gc.collect()
            time.sleep(0.1)


def populate_data_sets(
        args: Arguments,
) -> list[DataSetDaskWithAnswer]:
    data_sets = [
        populate_data_set_dask(
            args.exec_params,
            data_size=data_size,
        )
        for data_size in SIZES_LIST_VANILLA
        if data_size.size_code in args.sizes
    ]
    return data_sets


def do_with_client():
    args = parse_args()
    return do_test_runs(args)


def main():
    # with DaskClient(
    #         processes=True,
    #         n_workers=LOCAL_NUM_EXECUTORS,
    #         threads_per_worker=1,
    # ) as dask_client:
    do_with_client()


if __name__ == "__main__":
    main()
    print("Done!")
