#!python
# python -m VanillaPerfTest.VanillaRunner
from typing import List, Tuple

import argparse
from dataclasses import dataclass
import gc
import random
import time

from pyspark import SparkContext
from pyspark.sql import SparkSession

from PerfTestCommon import PythonTestMethod, count_iter
from Utils.SparkUtils import createSparkContext, setupSparkContext
from Utils.Utils import always_true

from .Strategy.Directory import implementation_list
from .Strategy.VanillaPandasCuda import vanilla_panda_cupy
from .RunResult import RunResult, write_run_result
from .VanillaTestData import DataPointAsTuple, generateData

DEBUG_ARGS = None if False else (
    '--size 10 100'.split()
)
RESULT_FILE_PATH = 'Results/vanilla_runs.csv'


@dataclass
class Arguments:
    num_runs: int
    scramble: bool
    sizes: List[str]
    strategies: List[str]


def parse_args() -> Arguments:
    parser = argparse.ArgumentParser()
    parser.add_argument('--runs', type=int, default=30)
    parser.add_argument(
        '--size',
        choices=['10', '100', '1k', '10k', '100k', '1m'],
        nargs="+")
    parser.add_argument(
        '--scramble', default=True,
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
        scramble=args.scramble,
        sizes=args.size,
        strategies=args.strategy
    )


def DoTesting(args: Arguments, spark: SparkSession, sc: SparkContext, log):
    data_sets = [x for x in [
        generateData(3, 3, 10**1) if '10' in args.sizes else None,
        generateData(3, 3, 10**2) if '100' in args.sizes else None,
        generateData(3, 3, 10**3) if '1k' in args.sizes else None,
        generateData(3, 3, 10**4) if '10k' in args.sizes else None,
        generateData(3, 3, 10**5) if '100k' in args.sizes else None,
        generateData(3, 3, 10**6) if '1m' in args.sizes else None,
    ] if x is not None]
    keyed_implementation_list = {x.name: x for x in implementation_list}
    cond_run_itinerary: List[
        Tuple[PythonTestMethod, List[DataPointAsTuple]]
    ] = [
        (cond_method, data)
        for strategy in args.strategies
        if always_true(cond_method := keyed_implementation_list[strategy])        
        for data in data_sets
        for _ in range(0, args.num_runs)
    ]
    if args.scramble:
        random.shuffle(cond_run_itinerary)
    if 'vanilla_panda_cupy' in args.strategies:
        # for code generation warming
        vanilla_panda_cupy(spark, generateData(3, 3, 10**0))
    with open(RESULT_FILE_PATH, 'a') as file:
        for index, (cond_method, data) in enumerate(cond_run_itinerary):
            log.info("Working on %d of %d" % (index, len(cond_run_itinerary)))
            startedTime = time.time()
            rdd, df = cond_method.delegate(spark, data)
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


if __name__ == "__main__":
    args = parse_args()
    spark = createSparkContext({
        "spark.sql.shuffle.partitions": 7,
        "spark.ui.enabled": "false",
        "spark.rdd.compress": "false",
        "spark.driver.memory": "2g",
        "spark.executor.memory": "3g",
        "spark.executor.memoryOverhead": "1g",
    })
    sc, log = setupSparkContext(spark)
    DoTesting(args, spark, sc, log)
