#!python
# python -m Runner_Vanilla
import argparse
import gc
import random
import time
from dataclasses import dataclass
from typing import List, Optional, Tuple

from SixFieldTestData import DataSet, ExecutionParameters, PythonTestMethod, RunResult, generateData
from PerfTestCommon import count_iter
from Utils.SparkUtils import NUM_EXECUTORS, TidySparkSession
from Utils.Utils import always_true
from VanillaPerfTest.Strategy.VanillaPandasCuda import vanilla_panda_cupy
from VanillaPerfTest.VanillaDirectory import (
    implementation_list, strategy_name_list)
from VanillaPerfTest.VanillaRunResult import (
    PYTHON_RESULT_FILE_PATH, write_header, write_run_result)

DEBUG_ARGS = None if False else (
    []
    + '--size 1 10'.split()
    + '--runs 1'.split()
    # + '--random-seed 1234'.split()
    + ['--no-shuffle']
    # + ['--strategy', 'vanilla_rdd_grpmap']
    # + ' vanilla_rdd_reduce vanilla_rdd_mappart'.split()
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
        choices=['1', '10', '100', '1k', '10k', '100k', '1m'],
        default=['10', '100', '1k', '10k', '100k'],
        nargs="+")
    parser.add_argument(
        '--shuffle', default=True,
        action=argparse.BooleanOptionalAction)
    parser.add_argument(
        '--strategy',
        choices=strategy_name_list,
        default='vanilla_sql vanilla_fluent vanilla_pandas vanilla_pandas_numpy'.split()
        + 'vanilla_rdd_grpmap vanilla_rdd_reduce vanilla_rdd_mappart'.split(),
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
                     3, 3, 10 ** 0) if '1' in args.sizes else None,
        generateData('10', spark_session,
                     3, 3, 10 ** 1) if '10' in args.sizes else None,
        generateData('100', spark_session,
                     3, 3, 10 ** 2) if '100' in args.sizes else None,
        generateData('1k', spark_session,
                     3, 3, 10 ** 3) if '1k' in args.sizes else None,
        generateData('10k', spark_session,
                     3, 3, 10 ** 4) if '10k' in args.sizes else None,
        generateData('100k', spark_session,
                     3, 3, 10 ** 5) if '100k' in args.sizes else None,
        generateData('1m', spark_session,
                     3, 3, 10 ** 6) if '1m' in args.sizes else None,
    ] if x is not None]
    keyed_implementation_list = {
        x.strategy_name: x for x in implementation_list}
    cond_run_itinerary: List[
        Tuple[PythonTestMethod, DataSet]
    ] = [
        (cond_method, data_set)
        for strategy in args.strategies
        if always_true(cond_method := keyed_implementation_list[strategy])
        for data_set in data_sets
        for _ in range(0, args.num_runs)
    ]
    if args.random_seed is not None:
        random.seed(args.random_seed)
    if args.shuffle:
        random.shuffle(cond_run_itinerary)
    exec_params = ExecutionParameters(
        NumExecutors=NUM_EXECUTORS,
    )
    if 'vanilla_panda_cupy' in args.strategies:
        # for code generation warming
        vanilla_panda_cupy(
            spark_session, exec_params, generateData(
                '1', spark_session, 3, 3, 10**0))
    with open(PYTHON_RESULT_FILE_PATH, 'at+') as file:
        write_header(file)
        for index, (cond_method, data_set) in enumerate(cond_run_itinerary):
            print(f"Working on {cond_method.strategy_name} for {data_set.SizeCode}")
            spark_session.log.info(
                "Working on %d of %d" % (index, len(cond_run_itinerary)))
            startedTime = time.time()
            rdd, df = cond_method.delegate(
                spark_session, exec_params, data_set)
            if df is not None:
                rdd = df.rdd
            assert rdd is not None
            if rdd.getNumPartitions() != data_set.AggTgtNumPartitions:
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
    }
    with TidySparkSession(
        config,
        enable_hive_support=False
    ) as spark_session:
        do_test_runs(args, spark_session)
