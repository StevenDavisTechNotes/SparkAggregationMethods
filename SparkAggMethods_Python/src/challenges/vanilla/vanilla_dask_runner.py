#! python
# usage: python -m challenges.vanilla.vanilla_dask_runner

import argparse
import gc
import random
import time
from typing import NamedTuple

from dask.distributed import Client as DaskClient

from challenges.vanilla.vanilla_record_runs import derive_run_log_file_path
from challenges.vanilla.vanilla_strategy_directory import (
    DASK_STRATEGY_NAME_LIST, solutions_using_dask)
from perf_test_common import CalcEngine
from six_field_test_data.six_generate_test_data import (
    ChallengeMethodPythonDaskRegistration, DaskDataSetWithAnswer,
    populate_data_set_dask)
from six_field_test_data.six_run_result_types import write_header
from six_field_test_data.six_runner_base import test_one_step_in_dask_itinerary
from six_field_test_data.six_test_data_types import (
    SHARED_LOCAL_TEST_DATA_FILE_LOCATION, Challenge, ExecutionParameters)
from utils.tidy_spark_session import LOCAL_NUM_EXECUTORS
from utils.utils import always_true, set_random_seed

ENGINE = CalcEngine.DASK
CHALLENGE = Challenge.VANILLA
DEBUG_ARGS = None if False else (
    []
    # + '--size 10'.split()
    + '--runs 1'.split()
    # + '--random-seed 1234'.split()
    + ['--no-shuffle']
    #     + ['--strategy',
    #           'vanilla_dask_ddf_grp_apply',
    #           'vanilla_dask_ddf_grp_udaf',
    #        ]
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
    parser = argparse.ArgumentParser()
    parser.add_argument('--random-seed', type=int)
    parser.add_argument('--runs', type=int, default=30)
    parser.add_argument(
        '--size',
        choices=['1', '10', '100', '1k', '10k', '100k'],
        default=['1', '10', '100', '1k', '10k', '100k'],
        nargs="+")
    parser.add_argument(
        '--shuffle', default=True,
        action=argparse.BooleanOptionalAction)
    parser.add_argument(
        '--strategy',
        choices=DASK_STRATEGY_NAME_LIST,
        default=DASK_STRATEGY_NAME_LIST,
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
        dask_client: DaskClient,
) -> None:
    data_sets = populate_data_sets(args)
    keyed_implementation_list = {
        x.strategy_name: x for x in solutions_using_dask}
    itinerary: list[tuple[ChallengeMethodPythonDaskRegistration, DaskDataSetWithAnswer]] = [
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
            print(f"Working on {challenge_method_registration.strategy_name} for {data_set.description.SizeCode}")
            test_one_step_in_dask_itinerary(
                challenge=CHALLENGE,
                dask_client=dask_client,
                exec_params=args.exec_params,
                challenge_method_registration=challenge_method_registration,
                file=file,
                data_set=data_set,
            )
            gc.collect()
            time.sleep(0.1)


def populate_data_sets(
        args: Arguments,
) -> list[DaskDataSetWithAnswer]:

    def generate_single_test_data_set_simple(
            code: str,
            num_grp_1:
            int, num_grp_2: int,
            num_data_points: int
    ) -> DaskDataSetWithAnswer:
        return populate_data_set_dask(
            args.exec_params,
            code, num_grp_1, num_grp_2, num_data_points)

    data_sets = [x for x in [
        generate_single_test_data_set_simple('1', 3, 3, 10 ** 0,
                                             ) if '1' in args.sizes else None,
        generate_single_test_data_set_simple('10', 3, 3, 10 ** 1,
                                             ) if '10' in args.sizes else None,
        generate_single_test_data_set_simple('100', 3, 3, 10 ** 2,
                                             ) if '100' in args.sizes else None,
        generate_single_test_data_set_simple('1k', 3, 3, 10 ** 3,
                                             ) if '1k' in args.sizes else None,
        generate_single_test_data_set_simple('10k', 3, 3, 10 ** 4,
                                             ) if '10k' in args.sizes else None,
        generate_single_test_data_set_simple('100k', 3, 3, 10 ** 5,
                                             ) if '100k' in args.sizes else None,
        generate_single_test_data_set_simple('1m', 3, 3, 10 ** 6,
                                             ) if '1m' in args.sizes else None,
    ] if x is not None]

    return data_sets


def do_with_client(dask_client: DaskClient):
    args = parse_args()
    return do_test_runs(args, dask_client)


def main():
    with DaskClient(
            processes=True,
            n_workers=LOCAL_NUM_EXECUTORS,
            threads_per_worker=1,
    ) as dask_client:
        do_with_client(dask_client)
    print("Done!")


if __name__ == "__main__":
    main()
