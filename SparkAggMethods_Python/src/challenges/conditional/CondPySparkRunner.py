#! python
# usage: (cd src; python -m challenges.conditional.CondPySparkRunner)
import argparse
import gc
import random
import time
from dataclasses import dataclass
from typing import List, Optional, Tuple

from challenges.conditional.CondDirectory import (pyspark_implementation_list,
                                                  strategy_name_list)
from challenges.conditional.CondRunResult import (derive_run_log_file_path,
                                                  pyspark_infeasible)
from perf_test_common import CalcEngine
from six_field_test_data.six_generate_test_data_using_pyspark import (
    GrpTotal, PySparkDataSetWithAnswer, PysparkPythonTestMethod,
    populate_data_set_pyspark)
from six_field_test_data.six_run_result_types import write_header
from six_field_test_data.six_runner_base import test_one_step_in_itinerary
from six_field_test_data.six_test_data_types import (
    SHARED_LOCAL_TEST_DATA_FILE_LOCATION, ExecutionParameters)
from utils.TidySparkSession import LOCAL_NUM_EXECUTORS, TidySparkSession
from utils.Utils import always_true, set_random_seed

ENGINE = CalcEngine.PYSPARK
DEBUG_ARGS = None if False else (
    []
    + '--size 3_3_10'.split()
    + '--runs 1'.split()
    # + '--random-seed 1234'.split()
    + ['--no-shuffle']
    # + ['--strategy']
    #   + [
    #     # 'cond_sql_join',
    #     # 'cond_fluent_join',
    #     # 'cond_sql_null',
    #     # 'cond_fluent_null',
    #     # 'cond_fluent_zero',
    #     # 'cond_pandas',
    #     # 'cond_pandas_numba',
    #     # 'cond_sql_nested',
    #     # 'cond_fluent_nested',
    #     # 'cond_fluent_window',
    #     'cond_rdd_grpmap',
    #     'cond_rdd_reduce',
    #     'cond_rdd_mappart',
    # ]
)


@ dataclass(frozen=True)
class Arguments:
    num_runs: int
    random_seed: Optional[int]
    shuffle: bool
    sizes: List[str]
    strategies: List[str]
    exec_params: ExecutionParameters


def parse_args() -> Arguments:
    parser = argparse.ArgumentParser()
    parser.add_argument('--random-seed', type=int)
    parser.add_argument('--runs', type=int, default=30)
    parser.add_argument(
        '--size',
        choices=[
            '3_3_10',
            '3_3_100',
            '3_3_1k',
            '3_3_10k',
            '3_3_100k',
        ],
        default=[
            '3_3_10',
            '3_3_100',
            '3_3_1k',
            '3_3_10k',
            '3_3_100k',
        ],
        nargs="+")
    parser.add_argument(
        '--shuffle', default=True,
        action=argparse.BooleanOptionalAction)
    parser.add_argument(
        '--strategy',
        choices=strategy_name_list,
        default=strategy_name_list,
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
        strategies=args.strategy,
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
        x.strategy_name: x for x in pyspark_implementation_list}
    itinerary: List[Tuple[PysparkPythonTestMethod, PySparkDataSetWithAnswer]] = [
        (test_method, data_set)
        for strategy in args.strategies
        if always_true(test_method := keyed_implementation_list[strategy])
        for data_set in data_sets
        if not pyspark_infeasible(strategy, data_set)
        for _ in range(0, args.num_runs)
    ]
    if args.random_seed is not None:
        set_random_seed(args.random_seed)
    if args.shuffle:
        random.shuffle(itinerary)
    with open(derive_run_log_file_path(ENGINE), 'at+') as file:
        write_header(file)
        for index, (test_method, data_set) in enumerate(itinerary):
            spark_session.log.info(
                "Working on %s %d of %d" %
                (test_method.strategy_name, index, len(itinerary)))
            print(f"Working on {test_method.strategy_name} for {data_set.description.SizeCode}")
            test_one_step_in_itinerary(
                engine=ENGINE,
                spark_session=spark_session,
                exec_params=args.exec_params,
                test_method=test_method,
                result_columns=list(GrpTotal._fields),
                file=file,
                data_set=data_set,
                correct_answer=data_set.answer.conditional_answer,
            )
            gc.collect()
            time.sleep(0.1)


def populate_data_sets(
        args: Arguments,
        spark_session: TidySparkSession,
) -> List[PySparkDataSetWithAnswer]:

    def generate_single_test_data_set_simple(
            code: str,
            num_grp_1: int,
            num_grp_2: int,
            num_data_points: int,
    ) -> PySparkDataSetWithAnswer:
        return populate_data_set_pyspark(
            spark_session, args.exec_params,
            code, num_grp_1, num_grp_2, num_data_points)

    data_sets = [x for x in [
        generate_single_test_data_set_simple(
            '3_3_10',
            3, 3, 10**1,
        ) if '3_3_10' in args.sizes else None,
        generate_single_test_data_set_simple(
            '3_3_100',
            3, 3, 10**2,
        ) if '3_3_100' in args.sizes else None,
        generate_single_test_data_set_simple(
            '3_3_1k',
            3, 3, 10**3,
        ) if '3_3_1k' in args.sizes else None,
        generate_single_test_data_set_simple(
            '3_3_10k',
            3, 3, 10**4,
        ) if '3_3_10k' in args.sizes else None,
        generate_single_test_data_set_simple(
            '3_3_100k',
            3, 3, 10**5,
        ) if '3_3_100k' in args.sizes else None,
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
        do_test_runs(args, spark_session)


if __name__ == "__main__":
    main()
