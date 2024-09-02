#! python
# usage: python -m src.challenges.vanilla.vanilla_pyspark_runner
import argparse
import gc
import os
import random
import time
from dataclasses import dataclass

from src.challenges.vanilla.vanilla_record_runs import \
    derive_run_log_file_path_for_recording
from src.challenges.vanilla.vanilla_strategy_directory import \
    STRATEGIES_USING_PYSPARK_REGISTRY
from src.challenges.vanilla.vanilla_test_data_types import (
    DATA_SIZES_LIST_VANILLA, RESULT_COLUMNS)
from src.perf_test_common import CalcEngine
from src.six_field_test_data.six_generate_test_data import (
    ChallengeMethodPythonPysparkRegistration, DataSetPysparkWithAnswer,
    populate_data_set_pyspark)
from src.six_field_test_data.six_run_result_types import write_header
from src.six_field_test_data.six_runner_base import \
    test_one_step_in_pyspark_itinerary
from src.six_field_test_data.six_test_data_types import (
    SHARED_LOCAL_TEST_DATA_FILE_LOCATION, Challenge, ExecutionParameters)
from src.utils.tidy_spark_session import (LOCAL_NUM_EXECUTORS,
                                          TidySparkSession,
                                          get_python_code_root_path)
from src.utils.utils import always_true, set_random_seed

ENGINE = CalcEngine.PYSPARK
CHALLENGE = Challenge.VANILLA

DEBUG_ARGS = None if False else (
    []
    + '--size 3_3_10'.split()
    + '--runs 1'.split()
    # + '--random-seed 1234'.split()
    + ['--no-shuffle']
    # + ['--strategy',
    #    'vanilla_pyspark_rdd_grp_map',
    #    'vanilla_pyspark_rdd_mappart',
    #    'vanilla_pyspark_rdd_reduce',
    #    ]
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
    strategy_names = [x.strategy_name for x in STRATEGIES_USING_PYSPARK_REGISTRY]

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
            DefaultParallelism=2 * LOCAL_NUM_EXECUTORS,
            TestDataFolderLocation=SHARED_LOCAL_TEST_DATA_FILE_LOCATION,
        ),
    )


def do_test_runs(
        args: Arguments,
        spark_session: TidySparkSession,
) -> None:
    data_sets = populate_data_sets(args, spark_session)
    keyed_implementation_list = {
        x.strategy_name: x for x in STRATEGIES_USING_PYSPARK_REGISTRY}
    itinerary: list[tuple[ChallengeMethodPythonPysparkRegistration, DataSetPysparkWithAnswer]] = [
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
        derive_run_log_file_path_for_recording(ENGINE))
    with open(result_log_path_name, 'at+') as file:
        write_header(file)
        for index, (challenge_method_registration, data_set) in enumerate(itinerary):
            spark_session.log.info(
                "Working on %d of %d" % (index, len(itinerary)))
            print(f"Working on {challenge_method_registration.strategy_name} for {data_set.description.size_code}")
            test_one_step_in_pyspark_itinerary(
                challenge=CHALLENGE,
                spark_session=spark_session,
                exec_params=args.exec_params,
                challenge_method_registration=challenge_method_registration,
                result_columns=RESULT_COLUMNS,
                file=file,
                data_set=data_set,
            )
            gc.collect()
            time.sleep(0.1)


def populate_data_sets(
        args: Arguments,
        spark_session: TidySparkSession,
) -> list[DataSetPysparkWithAnswer]:
    data_sets = [
        populate_data_set_pyspark(
            spark_session, args.exec_params,
            data_size=data_size,
        )
        for data_size in DATA_SIZES_LIST_VANILLA
        if data_size.size_code in args.sizes
    ]
    return data_sets


def main():
    args = parse_args()
    config = {
        "spark.sql.shuffle.partitions": args.exec_params.DefaultParallelism,
        "spark.default.parallelism": args.exec_params.DefaultParallelism,
        "spark.driver.memory": "2g",
        "spark.executor.memory": "3g",
        "spark.executor.memoryOverhead": "1g",
    }
    with TidySparkSession(
        config,
        enable_hive_support=False
    ) as spark_session:
        do_test_runs(args, spark_session)


if __name__ == "__main__":
    main()
    print("Done!")
