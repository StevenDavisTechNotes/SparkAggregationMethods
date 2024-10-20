#! python
# usage: python -m src.challenges.vanilla.vanilla_dask_runner
import argparse
import gc
import os
import random
import time
from typing import NamedTuple

from spark_agg_methods_common_python.challenge_strategy_registry import (
    ChallengeResultLogFileRegistration, ChallengeStrategyRegistration, update_challenge_strategy_registration,
)
from spark_agg_methods_common_python.challenges.six_field_test_data.six_test_data_types import (
    SHARED_LOCAL_TEST_DATA_FILE_LOCATION, SixTestExecutionParameters,
)
from spark_agg_methods_common_python.perf_test_common import (
    ELAPSED_TIME_COLUMN_NAME, CalcEngine, Challenge, SolutionLanguage,
)
from spark_agg_methods_common_python.utils.utils import always_true, set_random_seed

from src.challenges.six_field_test_data.six_runner_dask_base import test_one_step_in_dask_itinerary
from src.challenges.six_field_test_data.six_test_data_for_dask import (
    ChallengeMethodPythonDaskRegistration, DataSetDaskWithAnswer, populate_data_set_dask,
)
from src.challenges.vanilla.vanilla_record_runs import (
    PYTHON_DASK_RUN_LOG_FILE_PATH, VanillaPythonRunResultFileWriter, VanillaRunResult,
)
from src.challenges.vanilla.vanilla_strategy_directory import STRATEGIES_USING_DASK_REGISTRY
from src.challenges.vanilla.vanilla_test_data_types import DATA_SIZES_LIST_VANILLA, VanillaDataSetDescription
from src.utils.tidy_spark_session import LOCAL_NUM_EXECUTORS

LANGUAGE = SolutionLanguage.PYTHON
ENGINE = CalcEngine.DASK
CHALLENGE = Challenge.VANILLA


DEBUG_ARGS = None if False else (
    []
    + '--size 3_3_10'.split()
    + '--runs 1'.split()
    # + '--random-seed 1234'.split()
    + ['--no-shuffle']
    + ['--strategy',
       'vanilla_dask_bag_accumulate',
       'vanilla_dask_bag_fold',
       'vanilla_dask_bag_foldby',
       'vanilla_dask_bag_reduction',
       'vanilla_dask_bag_map_partitions',
       #    'vanilla_dask_ddf_grp_apply',
       #    'vanilla_dask_ddf_grp_udaf',
       'vanilla_dask_sql_no_gpu',
       ]
)


class Arguments(NamedTuple):
    have_gpu: bool
    num_runs: int
    random_seed: int | None
    shuffle: bool
    sizes: list[str]
    strategy_names: list[str]
    exec_params: SixTestExecutionParameters


def parse_args() -> Arguments:
    sizes = [x.size_code for x in DATA_SIZES_LIST_VANILLA]
    strategy_names = [x.strategy_name for x in STRATEGIES_USING_DASK_REGISTRY]

    parser = argparse.ArgumentParser()
    parser.add_argument('--random-seed', type=int)
    parser.add_argument('--runs', type=int, default=1)
    parser.add_argument('--size', choices=sizes, default=sizes, nargs="*")
    parser.add_argument('--shuffle', default=True, action=argparse.BooleanOptionalAction)
    parser.add_argument('--have-gpu', default=True, action=argparse.BooleanOptionalAction)
    parser.add_argument('--strategy', choices=strategy_names, default=strategy_names, nargs="*")
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
        strategy_names=args.strategy,
        exec_params=SixTestExecutionParameters(
            default_parallelism=2 * LOCAL_NUM_EXECUTORS,
            test_data_folder_location=SHARED_LOCAL_TEST_DATA_FILE_LOCATION,
        ),
    )


def do_test_runs(
        args: Arguments,
) -> None:
    data_sets = populate_data_sets(args)
    keyed_implementation_list = {
        x.strategy_name: x for x in STRATEGIES_USING_DASK_REGISTRY}
    itinerary: list[tuple[ChallengeMethodPythonDaskRegistration, DataSetDaskWithAnswer]] = [
        (challenge_method_registration, data_set)
        for strategy_name in args.strategy_names
        if always_true(challenge_method_registration := keyed_implementation_list[strategy_name])
        if (args.have_gpu or not challenge_method_registration.requires_gpu)
        for data_set in data_sets
        for _ in range(0, args.num_runs)
    ]
    if len(itinerary) == 0:
        print("No runs to execute.")
        return
    if args.random_seed is not None:
        set_random_seed(args.random_seed)
    if args.shuffle:
        random.shuffle(itinerary)
    with VanillaPythonRunResultFileWriter(ENGINE) as file:
        for index, (challenge_method_registration, data_set) in enumerate(itinerary):
            print("Working on %d of %d" % (index, len(itinerary)))
            print(f"Working on {challenge_method_registration.strategy_name} for {data_set.data_description.size_code}")
            base_run_result = test_one_step_in_dask_itinerary(
                challenge=CHALLENGE,
                exec_params=args.exec_params,
                challenge_method_registration=challenge_method_registration,
                data_set=data_set,
            )
            if base_run_result is None:
                continue
            if data_set.data_description.debugging_only:
                continue
            file.write_run_result(
                challenge_method_registration,
                VanillaRunResult(
                    num_source_rows=base_run_result.num_source_rows,
                    elapsed_time=base_run_result.elapsed_time,
                    num_output_rows=base_run_result.num_output_rows,
                    finished_at=base_run_result.finished_at,
                )
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
        for data_size in DATA_SIZES_LIST_VANILLA
        if data_size.size_code in args.sizes
    ]
    return data_sets


def update_challenge_registration():
    update_challenge_strategy_registration(
        language=LANGUAGE,
        engine=ENGINE,
        challenge=CHALLENGE,
        registration=ChallengeResultLogFileRegistration(
            result_file_path=os.path.abspath(PYTHON_DASK_RUN_LOG_FILE_PATH),
            regressor_column_name=VanillaDataSetDescription.regressor_field_name(),
            elapsed_time_column_name=ELAPSED_TIME_COLUMN_NAME,
            expected_regressor_values=[
                x.regressor_value
                for x in DATA_SIZES_LIST_VANILLA
                if not x.debugging_only
            ],
            strategies=[
                ChallengeStrategyRegistration(
                    language=LANGUAGE,
                    engine=ENGINE,
                    challenge=CHALLENGE,
                    interface=x.interface,
                    strategy_name=x.strategy_name,
                    numerical_tolerance=x.numerical_tolerance.value,
                    requires_gpu=x.requires_gpu,
                )
                for x in STRATEGIES_USING_DASK_REGISTRY
            ]
        ),
    )


def do_with_local_client():
    args = parse_args()
    update_challenge_registration()
    return do_test_runs(args)


def main():
    # with DaskClient(
    #         processes=True,
    #         n_workers=LOCAL_NUM_EXECUTORS,
    #         threads_per_worker=1,
    # ) as dask_client:
    do_with_local_client()


if __name__ == "__main__":
    print(f"Running {__file__}")
    main()
    print("Done!")
