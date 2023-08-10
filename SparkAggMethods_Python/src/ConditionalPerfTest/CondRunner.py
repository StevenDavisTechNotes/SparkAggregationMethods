import argparse
import gc
import random
import time
from dataclasses import dataclass
from typing import List, Optional, TextIO, Tuple

import pandas as pd

from ConditionalPerfTest.CondDataTypes import GrpTotal
from SixFieldCommon.SixFieldTestData import (
    SHARED_LOCAL_TEST_DATA_FILE_LOCATION, DataSetWithAnswer, ExecutionParameters,
    PythonTestMethod, populate_data_set)
from Utils.TidySparkSession import LOCAL_NUM_EXECUTORS, TidySparkSession
from Utils.Utils import always_true

from ConditionalPerfTest.CondDirectory import implementation_list, strategy_name_list
from ConditionalPerfTest.CondRunResult import (RunResult, infeasible, write_header,
                                               write_run_result)

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
PYTHON_RESULT_FILE_PATH = 'Results/conditional_runs.csv'


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
        x.strategy_name: x for x in implementation_list}
    itinerary: List[Tuple[PythonTestMethod, DataSetWithAnswer]] = [
        (test_method, data_set)
        for strategy in args.strategies
        if always_true(test_method := keyed_implementation_list[strategy])
        for data_set in data_sets
        if not infeasible(strategy, data_set)
        for _ in range(0, args.num_runs)
    ]
    if args.random_seed is not None:
        random.seed(args.random_seed)
    if args.shuffle:
        random.shuffle(itinerary)
    with open(PYTHON_RESULT_FILE_PATH, 'at+') as file:
        write_header(file)
        for index, (test_method, data_set) in enumerate(itinerary):
            spark_session.log.info(
                "Working on %s %d of %d" %
                (test_method.strategy_name, index, len(itinerary)))
            print(f"Working on {test_method.strategy_name} for {data_set.description.SizeCode}")
            test_one_step_in_itinerary(
                spark_session,
                args.exec_params,
                test_method,
                file,
                data_set)
            gc.collect()
            time.sleep(0.1)


def populate_data_sets(
        args: Arguments,
        spark_session: TidySparkSession,
) -> List[DataSetWithAnswer]:

    def generate_single_test_data_set_simple(
            code: str,
            num_grp_1: int,
            num_grp_2: int,
            num_data_points: int,
    ) -> DataSetWithAnswer:
        return populate_data_set(
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


def test_one_step_in_itinerary(
        spark_session: TidySparkSession,
        exec_params: ExecutionParameters,
        test_method: PythonTestMethod,
        file: TextIO,
        data_set: DataSetWithAnswer,
):
    startedTime = time.time()
    rdd, df = test_method.delegate(
        spark_session, exec_params, data_set)
    if df is not None:
        rdd = df.rdd
    assert rdd is not None
    if rdd.getNumPartitions() > max(data_set.data.AggTgtNumPartitions, exec_params.DefaultParallelism):
        print(
            f"{test_method.strategy_name} output rdd has {rdd.getNumPartitions()} partitions")
        findings = rdd.collect()
        print(f"size={len(findings)}, ", findings)
        exit(1)
    if df is not None:
        df_answer = df.toPandas()
        finishedTime = time.time()
    elif rdd is not None:
        answer = rdd.collect()
        finishedTime = time.time()
        if len(answer) > 0:
            assert isinstance(answer[0], GrpTotal)
            df_answer = pd.DataFrame.from_records(
                answer, columns=GrpTotal._fields)
        else:
            df_answer = pd.DataFrame(columns=GrpTotal._fields)
        if 'cond_var_of_E2' not in df_answer:
            df_answer['cond_var_of_E2'] = df_answer['cond_var_of_E']
    else:
        raise ValueError("Both df and rdd are None")
    abs_diff = float(
        (data_set.answer.conditional_answer - df_answer)
        .abs().max().max())
    status = abs_diff < 1e-12
    assert (status is True)
    recordCount = len(df_answer)
    result = RunResult(
        dataSize=data_set.description.NumDataPoints,
        elapsedTime=finishedTime - startedTime,
        recordCount=recordCount)
    write_run_result(test_method, result, file)


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
