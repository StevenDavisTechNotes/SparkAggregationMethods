#!python
# python -m VanillaPerfTest.VanillaRunner
import argparse
import gc
import random
import time
from dataclasses import dataclass
from typing import List, Optional, Tuple

import pandas as pd

from PerfTestCommon import count_iter
from SixFieldCommon.SixFieldTestData import (
    DataSet, ExecutionParameters, PythonTestMethod, RunResult, generateData)
from Utils.SparkUtils import NUM_EXECUTORS, TidySparkSession
from Utils.Utils import always_true

from .Strategy.VanillaPandasCuda import vanilla_panda_cupy
from .VanillaDirectory import implementation_list, strategy_name_list
from .VanillaRunResult import (
    PYTHON_RESULT_FILE_PATH, infeasible, write_header, write_run_result)

DEBUG_ARGS = None if False else (
    []
    # + '--size 10'.split()
    + '--runs 1'.split()
    # + '--random-seed 1234'.split()
    + ['--no-shuffle']
    # + ['--strategy',
    #    'vanilla_sql',
    #    'vanilla_fluent',
    # 'vanilla_pandas',
    # 'vanilla_pandas_numpy',
    #    'vanilla_rdd_grpmap',
    # 'vanilla_rdd_reduce',
    # 'vanilla_rdd_mappart'
    #    ]
)


@dataclass(frozen=True)
class Arguments:
    num_runs: int
    random_seed: Optional[int]
    shuffle: bool
    sizes: List[str]
    strategies: List[str]


def parse_args() -> Arguments:
    parser = argparse.ArgumentParser()
    parser.add_argument('--random-seed', type=int)
    parser.add_argument('--runs', type=int, default=30)
    parser.add_argument(
        '--size',
        choices=['1', '10', '100', '1k', '10k', '100k'],
        default=['10', '100', '1k', '10k', '100k'],
        nargs="+")
    parser.add_argument(
        '--shuffle', default=True,
        action=argparse.BooleanOptionalAction)
    parser.add_argument(
        '--strategy',
        choices=strategy_name_list,
        default=[
            'vanilla_sql', 'vanilla_fluent',
            'vanilla_pandas', 'vanilla_pandas_numpy',
            'vanilla_rdd_grpmap', 'vanilla_rdd_reduce', 'vanilla_rdd_mappart'],
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
        generateData('1', spark_session,
                     3, 3, 10 ** 0,
                     ) if '1' in args.sizes else None,
        generateData('10', spark_session,
                     3, 3, 10 ** 1,
                     ) if '10' in args.sizes else None,
        generateData('100', spark_session,
                     3, 3, 10 ** 2,
                     ) if '100' in args.sizes else None,
        generateData('1k', spark_session,
                     3, 3, 10 ** 3,
                     ) if '1k' in args.sizes else None,
        generateData('10k', spark_session,
                     3, 3, 10 ** 4,
                     ) if '10k' in args.sizes else None,
        generateData('100k', spark_session,
                     3, 3, 10 ** 5,
                     ) if '100k' in args.sizes else None,
        generateData('1m', spark_session,
                     3, 3, 10 ** 6,
                     ) if '1m' in args.sizes else None,
    ] if x is not None]
    keyed_implementation_list = {
        x.strategy_name: x for x in implementation_list}
    itinerary: List[
        Tuple[PythonTestMethod, DataSet]
    ] = [
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
    exec_params = ExecutionParameters(
        NumExecutors=NUM_EXECUTORS,
    )
    if 'vanilla_panda_cupy' in args.strategies:
        # for code generation warming
        vanilla_panda_cupy(
            spark_session, exec_params, generateData(
                '1', spark_session, 3, 3, 10**0,
            ))
    with open(PYTHON_RESULT_FILE_PATH, 'at+') as file:
        write_header(file)
        for index, (test_method, data_set) in enumerate(itinerary):
            test_one_step_in_itinerary(spark_session, itinerary, test_method, exec_params, file, index, data_set)
            gc.collect()
            time.sleep(0.1)


def test_one_step_in_itinerary(spark_session, itinerary, test_method, exec_params, file, index, data_set):
    print(f"Working on {test_method.strategy_name} for {data_set.SizeCode}")
    spark_session.log.info(
        "Working on %d of %d" % (index, len(itinerary)))
    startedTime = time.time()
    rdd, df = test_method.delegate(
        spark_session, exec_params, data_set)
    if df is not None:
        rdd = df.rdd
    assert rdd is not None
    if rdd.getNumPartitions() > max(data_set.AggTgtNumPartitions, NUM_EXECUTORS * 2):
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
        df_answer = pd.DataFrame.from_records([x.asDict() for x in answer])
        if 'var_of_E2' not in df_answer:
            df_answer['var_of_E2'] = df_answer['var_of_E']
    else:
        raise ValueError("Both df and rdd are None")
    abs_diff = float(
        (data_set.vanilla_answer - df_answer)
        .abs().max().max())
    status = abs_diff < 1e-12
    assert (status is True)
    recordCount = len(df_answer)
    result = RunResult(
        dataSize=data_set.NumDataPoints,
        elapsedTime=finishedTime - startedTime,
        recordCount=recordCount)
    write_run_result(test_method, result, file)


def main():
    args = parse_args()
    config = {
        "spark.sql.shuffle.partitions": NUM_EXECUTORS * 2,
        "spark.rdd.compress": "false",
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