#!python
# python -m Runner_Conditional
import argparse
import gc
import random
import time
from dataclasses import dataclass
from typing import List, Optional, Tuple

from SixFieldTestData import DataSet, ExecutionParameters, generateData
from ConditionalPerfTest.CondDirectory import (
    PythonTestMethod, implementation_list, strategy_name_list)
from ConditionalPerfTest.CondRunResult import (
    RunResult, write_header, write_run_result)
from PerfTestCommon import count_iter
from Utils.SparkUtils import NUM_EXECUTORS, TidySparkSession
from Utils.Utils import always_true

DEBUG_ARGS = None if False else (
    []
    + '--size 3_3_10'.split()
    + '--runs 1'.split()
    # + '--random-seed 1234'.split()
    + ['--no-shuffle']
    # + ['--strategy']
    #   + [
    #     #     'cond_sql_join',
    #     # 'cond_fluent_join',
    #     # 'cond_sql_null',
    #     # 'cond_fluent_null',
    #     # 'cond_fluent_zero',
    #     # 'cond_pandas',
    #     # 'cond_pandas_numba',
    #     # 'cond_sql_nested',
    #     # 'cond_fluent_nested',
    #     # 'cond_fluent_window',
    #     # 'cond_rdd_grpmap',
    #     # 'cond_rdd_reduce',
    #     'cond_rdd_mappart',
    # ]
)
PYTHON_RESULT_FILE_PATH = 'Results/conditional_runs.csv'


@dataclass(frozen=True)
class Arguments:
    num_runs: int
    random_seed: Optional[int]
    shuffle: bool
    sizes: List[str]
    strategies: List[str]


available_data_sizes = [
    '3_3_10',
    '3_3_100k',
    '3_30_10k',
    '3_300_1k',
    '3_3k_100',
]


def parse_args() -> Arguments:
    parser = argparse.ArgumentParser()
    parser.add_argument('--random-seed', type=int)
    parser.add_argument('--runs', type=int, default=30)
    parser.add_argument(
        '--size',
        choices=available_data_sizes,
        default=available_data_sizes,
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
    )


def do_test_runs(args: Arguments, spark_session: TidySparkSession):
    data_sets = [x for x in [
        generateData(
            '3_3_10', spark_session,
            3, 3, 10**1) if '3_3_10' in args.sizes else None,
        generateData(
            '3_3_100', spark_session,
            3, 3, 10**2) if '3_3_100' in args.sizes else None,
        generateData(
            '3_3_1k', spark_session,
            3, 3, 10**3) if '3_3_1k' in args.sizes else None,
        generateData(
            '3_3_10k', spark_session,
            3, 3, 10**4) if '3_3_10k' in args.sizes else None,
        generateData(
            '3_3_100k', spark_session,
            3, 3, 10**5) if '3_3_100k' in args.sizes else None,
    ] if x is not None]
    keyed_implementation_list = {
        x.strategy_name: x for x in implementation_list}
    cond_run_itinerary: List[
        Tuple[PythonTestMethod, DataSet]
    ] = [
        (cond_method, data)
        for strategy in args.strategies
        if always_true(cond_method := keyed_implementation_list[strategy])
        for data in data_sets
        for _ in range(0, args.num_runs)
    ]
    if args.random_seed is not None:
        random.seed(args.random_seed)
    if args.shuffle:
        random.shuffle(cond_run_itinerary)
    exec_params = ExecutionParameters(
        NumExecutors=NUM_EXECUTORS,
    )
    with open(PYTHON_RESULT_FILE_PATH, 'at+') as file:
        write_header(file)
        for index, (cond_method, data_set) in enumerate(cond_run_itinerary):
            print(f"Working on {cond_method.strategy_name} for {data_set.SizeCode}")
            spark_session.log.info(
                "Working on %s %d of %d" %
                (cond_method.strategy_name, index, len(cond_run_itinerary)))
            startedTime = time.time()
            rdd, df = cond_method.delegate(
                spark_session, exec_params, data_set)
            if df is not None:
                rdd = df.rdd
            assert rdd is not None
            if rdd.getNumPartitions() > max(data_set.AggTgtNumPartitions, NUM_EXECUTORS * 2):
                print(
                    f"{cond_method.strategy_name} output rdd has {rdd.getNumPartitions()} partitions")
                findings = rdd.collect()
                print(f"size={len(findings)}, ", findings)
                exit(1)
            recordCount = count_iter(rdd.toLocalIterator())
            finishedTime = time.time()
            result = RunResult(
                dataSize=data_set.NumDataPoints,
                elapsedTime=finishedTime - startedTime,
                recordCount=recordCount)
            write_run_result(cond_method, result, file)
            del df
            del rdd
            gc.collect()
            time.sleep(0.1)


if __name__ == "__main__":
    args = parse_args()
    config = {
        "spark.sql.shuffle.partitions": NUM_EXECUTORS * 2,
        "spark.rdd.compress": "false",
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
