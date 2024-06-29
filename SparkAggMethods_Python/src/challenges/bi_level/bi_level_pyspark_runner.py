#! python
# usage: (cd src; python -m challenges.bi_level.bi_level_pyspark_runner)
import argparse
import gc
import random
import time
from dataclasses import dataclass
from typing import Optional

from challenges.bi_level.bi_level_record_runs import derive_run_log_file_path
from challenges.bi_level.bi_level_strategy_directory import (
    STRATEGY_NAME_LIST, solutions_using_pyspark)
from challenges.bi_level.bi_level_test_data_types import result_columns
from perf_test_common import CalcEngine
from six_field_test_data.six_generate_test_data import (
    ChallengeMethodPythonPysparkRegistration, DataSetPysparkWithAnswer,
    populate_data_set_pyspark)
from six_field_test_data.six_run_result_types import write_header
from six_field_test_data.six_runner_base import \
    test_one_step_in_pyspark_itinerary
from six_field_test_data.six_test_data_types import (
    SHARED_LOCAL_TEST_DATA_FILE_LOCATION, Challenge, ExecutionParameters)
from utils.tidy_spark_session import LOCAL_NUM_EXECUTORS, TidySparkSession
from utils.utils import always_true, set_random_seed

DEBUG_ARGS = None if False else (
    []
    + '--size 3_3_10'.split()
    + '--runs 1'.split()
    # + '--random-seed 1234'.split()
    + ['--no-shuffle']
    + ['--have-gps']
    # + ['--strategy',
    #    #    'bi_sql_join',
    #    #    'bi_fluent_join',
    #    #    'bi_pandas',
    #    #    'bi_pandas_numba',
    #    #    'bi_sql_nested',
    #    #    'bi_fluent_nested',
    #    #    'bi_fluent_window',
    #    #    'bi_rdd_grpmap',
    #    #    'bi_rdd_reduce1',
    #    #    'bi_rdd_reduce2',
    #    #    'bi_rdd_mappart'
    #    ]
)
ENGINE = CalcEngine.PYSPARK
CHALLENGE = Challenge.BI_LEVEL


@dataclass(frozen=True)
class Arguments:
    num_runs: int
    random_seed: Optional[int]
    shuffle: bool
    sizes: list[str]
    strategy_names: list[str]
    exec_params: ExecutionParameters


def parse_args() -> Arguments:
    parser = argparse.ArgumentParser()
    parser.add_argument('--random-seed', type=int)
    parser.add_argument('--runs', type=int, default=30)
    parser.add_argument(
        '--size',
        choices=['3_3_10', '3_3_100k', '3_30_10k', '3_300_1k', '3_3k_100'],
        default=['3_3_100k', '3_30_10k', '3_300_1k', '3_3k_100'],
        nargs="+")
    parser.add_argument('--shuffle', default=True, action=argparse.BooleanOptionalAction)
    parser.add_argument(
        '--strategy',
        choices=STRATEGY_NAME_LIST,
        default=[
            x.strategy_name
            for x in solutions_using_pyspark
        ],
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


def do_pyspark_test_runs(
        args: Arguments,
        spark_session: TidySparkSession
) -> None:
    data_sets = populate_data_sets(args, spark_session)
    keyed_implementation_list = {
        x.strategy_name: x for x in solutions_using_pyspark}
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
    with open(file=derive_run_log_file_path(ENGINE), mode='at+') as file:
        write_header(file)
        for index, (challenge_method_registration, data_set) in enumerate(itinerary):
            spark_session.log.info("Working on %d of %d" %
                                   (index, len(itinerary)))
            print(f"Working on {challenge_method_registration.strategy_name} for {data_set.description.SizeCode}")
            test_one_step_in_pyspark_itinerary(
                challenge=CHALLENGE,
                spark_session=spark_session,
                exec_params=args.exec_params,
                challenge_method_registration=challenge_method_registration,
                result_columns=result_columns,
                file=file,
                data_set=data_set,
            )
            gc.collect()
            time.sleep(0.1)


def populate_data_sets(
        args: Arguments,
        spark_session: TidySparkSession,
) -> list[DataSetPysparkWithAnswer]:

    def generate_single_test_data_set_simple(
            code: str, num_grp_1: int, num_grp_2: int, num_data_points: int
    ) -> DataSetPysparkWithAnswer:
        return populate_data_set_pyspark(
            spark_session, args.exec_params,
            code, num_grp_1, num_grp_2, num_data_points)

    data_sets = [x for x in [
        generate_single_test_data_set_simple('3_3_10',
                                             3, 3, 10 ** 1,
                                             ) if '3_3_10' in args.sizes else None,
        generate_single_test_data_set_simple('3_3_100k',
                                             3, 3, 10 ** 5,
                                             ) if '3_3_100k' in args.sizes else None,
        generate_single_test_data_set_simple('3_30_10k',
                                             3, 30, 10 ** 4,
                                             ) if '3_30_10k' in args.sizes else None,
        generate_single_test_data_set_simple('3_300_1k',
                                             3, 300, 10 ** 3,
                                             ) if '3_300_1k' in args.sizes else None,
        generate_single_test_data_set_simple('3_3k_100',
                                             3, 3000, 10 ** 2,
                                             ) if '3_3k_100' in args.sizes else None,
    ] if x is not None]
    return data_sets


def main():
    args = parse_args()
    config = {
        "spark.sql.shuffle.partitions": args.exec_params.DefaultParallelism,
        "spark.default.parallelism": args.exec_params.DefaultParallelism,
        "spark.driver.memory": "2g",
        "spark.executor.memory": "3g",
        "spark.executor.memoryOverhead": "1g",
        "spark.sql.execution.arrow.pyspark.enabled": "true",
    }
    with TidySparkSession(
        config,
        enable_hive_support=False
    ) as spark_session:
        do_pyspark_test_runs(args, spark_session)


if __name__ == "__main__":
    main()
