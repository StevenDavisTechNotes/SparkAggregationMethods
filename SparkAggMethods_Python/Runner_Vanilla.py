#!python
# python -m Runner_Vanilla
from typing import List, Tuple, Optional

import argparse
from dataclasses import dataclass
import gc
import random
import time

from PerfTestCommon import count_iter
from Utils.SparkUtils import TidySparkSession
from Utils.Utils import always_true

from VanillaPerfTest.VanillaDirectory import implementation_list, PythonTestMethod
from VanillaPerfTest.Strategy.VanillaPandasCuda import vanilla_panda_cupy
from VanillaPerfTest.VanillaRunResult import RunResult, write_run_result
from VanillaPerfTest.VanillaTestData import DataPoint, generateData

DEBUG_ARGS = None if False else (
    []
    + '--size 1 10'.split()
    + '--runs 1'.split()
    + '--random-seed 1234'.split()
    + ['--no-shuffle']
    # +'--strategy vanilla_pandas'.split()
)
RESULT_FILE_PATH = 'Results/vanilla_runs.csv'


@dataclass
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
        choices='vanilla_sql vanilla_fluent vanilla_pandas vanilla_pandas_numpy vanilla_panda_cupy vanilla_pandas_numba'.split()
        + 'vanilla_rdd_grpmap vanilla_rdd_reduce vanilla_rdd_mappart'.split(),
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


def DoTesting(args: Arguments, spark_session: TidySparkSession):
    data_sets = [x for x in [
        generateData(3, 3, 10**0) if '1' in args.sizes else None,
        generateData(3, 3, 10**1) if '10' in args.sizes else None,
        generateData(3, 3, 10**2) if '100' in args.sizes else None,
        generateData(3, 3, 10**3) if '1k' in args.sizes else None,
        generateData(3, 3, 10**4) if '10k' in args.sizes else None,
        generateData(3, 3, 10**5) if '100k' in args.sizes else None,
        generateData(3, 3, 10**6) if '1m' in args.sizes else None,
    ] if x is not None]
    keyed_implementation_list = {x.name: x for x in implementation_list}
    cond_run_itinerary: List[
        Tuple[PythonTestMethod, List[DataPoint]]
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
    if 'vanilla_panda_cupy' in args.strategies:
        # for code generation warming
        vanilla_panda_cupy(spark_session, generateData(3, 3, 10**0))
    with open(RESULT_FILE_PATH, 'a') as file:
        for index, (cond_method, data) in enumerate(cond_run_itinerary):
            spark_session.log.info(
                "Working on %d of %d" % (index, len(cond_run_itinerary)))
            startedTime = time.time()
            rdd, df = cond_method.delegate(spark_session, data)
            if df is not None:
                rdd = df.rdd
            assert rdd is not None
            recordCount = count_iter(rdd.toLocalIterator())
            finishedTime = time.time()
            result = RunResult(
                dataSize=len(data),
                elapsedTime=finishedTime-startedTime,
                recordCount=recordCount)
            write_run_result(cond_method, result, file)
            del df
            del rdd
            gc.collect()
            time.sleep(1)


if __name__ == "__main__":
    args = parse_args()
    config = {
        "spark.sql.shuffle.partitions": 7,
        "spark.rdd.compress": "false",
        "spark.driver.memory": "2g",
        "spark.executor.memory": "3g",
        "spark.executor.memoryOverhead": "1g",
    }
    with TidySparkSession(
        config,
        enable_hive_support=False
    ) as spark_session:
        DoTesting(args, spark_session)
