#!python
# python -m Runner_DeDupe
from typing import List, Optional

import argparse
import gc
import math
import random
import time
from dataclasses import dataclass

from DedupePerfTest.DedupeDirectory import (
    implementation_list,
    strategy_name_list,
)
from DedupePerfTest.DedupeExpected import (
    ItineraryItem, verifyCorrectnessDf,
    verifyCorrectnessRdd,
)
from DedupePerfTest.DedupeRunResult import (
    RESULT_FILE_PATH, RunResult,
    write_failed_run, write_header,
    write_run_result,
)
from DedupePerfTest.DedupeTestData import (
    DedupeDataParameters, DoGenData,
    GenDataSets,
)
from PerfTestCommon import count_iter
from Utils.SparkUtils import TidySparkSession
from Utils.Utils import always_true

DEBUG_ARGS = None if False else (
    []
    + '--size 1 10'.split()
    + '--runs 1'.split()
    + '--random-seed 1234'.split()
    + ['--no-shuffle']
    # + '--strategy method_rdd_reduce'.split()
)
LOCAL_TEST_DATA_FILE_LOCATION = "d:/temp/SparkPerfTesting"
REMOTE_TEST_DATA_LOCATION = "wasb:///sparkperftesting"
MaximumProcessableSegment = pow(10, 5)


@dataclass
class Arguments:
    num_runs: int
    random_seed: Optional[int]
    shuffle: bool
    sizes: List[str]
    strategies: List[str]
    ideosyncracies: DedupeDataParameters


def parse_args() -> Arguments:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--assume-no-dupes-per-partition', default=True,
        action=argparse.BooleanOptionalAction)
    parser.add_argument(
        '--cloud-mode', default=False,
        action=argparse.BooleanOptionalAction)
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
        default=strategy_name_list,
        nargs="+")
    if DEBUG_ARGS is None:
        args = parser.parse_args()
    else:
        args = parser.parse_args(DEBUG_ARGS)

    IsCloudMode = args.cloud_mode
    if IsCloudMode:
        NumExecutors = 40
        CanAssumeNoDupesPerPartition = False
        DefaultParallelism = 2 * NumExecutors
        SufflePartitions = DefaultParallelism
        test_data_file_location = REMOTE_TEST_DATA_LOCATION
    else:
        NumExecutors = 7
        CanAssumeNoDupesPerPartition = args.assume_no_dupes_per_partition
        DefaultParallelism = 16
        SufflePartitions = 14
        test_data_file_location = LOCAL_TEST_DATA_FILE_LOCATION

    return Arguments(
        num_runs=args.runs,
        random_seed=args.random_seed,
        shuffle=args.shuffle,
        sizes=args.size,
        strategies=args.strategy,
        ideosyncracies=DedupeDataParameters(
            in_cloud_mode=IsCloudMode,
            NumExecutors=NumExecutors,
            CanAssumeNoDupesPerPartition=CanAssumeNoDupesPerPartition,
            DefaultParallelism=DefaultParallelism,
            SufflePartitions=SufflePartitions,
            test_data_file_location=test_data_file_location,
        ))


def runtests(srcDfListList: List[GenDataSets],
             args: Arguments, spark_session: TidySparkSession):
    data_params = args.ideosyncracies

    keyed_implementation_list = {
        x.strategy_name: x for x in implementation_list}
    test_run_itinerary: List[ItineraryItem] = [
        ItineraryItem(
            testMethod=test_method,
            NumSources=sizeObj.NumSources,
            numPeople=srcDfList.NumPeople,
            dataSize=sizeObj.DataSize,
            df=sizeObj.dfSrc,
        )
        for strategy in args.strategies
        if always_true(test_method := keyed_implementation_list[strategy])
        for srcDfList in srcDfListList
        for sizeObj in srcDfList.DataSets
        for _ in range(args.num_runs)
    ]
    if args.random_seed is not None:
        random.seed(args.random_seed)
    if args.shuffle is True:
        random.shuffle(test_run_itinerary)

    test_runs = {}
    with open(RESULT_FILE_PATH, 'a+') as result_log_file:
        log = spark_session.log
        write_header(result_log_file)
        for index, itinerary_item in enumerate(test_run_itinerary):
            log.info("Working on %d of %d" % (index, len(test_run_itinerary)))
            startedTime = time.time()
            print("Working on %s %d %d" %
                  (test_method.strategy_name, itinerary_item.numPeople, itinerary_item.dataSize))
            try:
                rddout, dfout = test_method.delegate(
                    spark_session, data_params, itinerary_item.dataSize, itinerary_item.df)
                if rddout is not None:
                    print(f"NumPartitions={rddout.getNumPartitions()}")
                    foundNumPeople = count_iter(rddout.toLocalIterator())
                elif dfout is not None:
                    print(f"NumPartitions={dfout.rdd.getNumPartitions()}")
                    foundNumPeople = count_iter(dfout.toLocalIterator())
                else:
                    raise Exception("not returning anything")
            except Exception as exception:
                rddout = dfout = None
                log.exception(exception)
                foundNumPeople = -1
            elapsedTime = time.time() - startedTime
            result = RunResult(
                numSources=itinerary_item.NumSources,
                actualNumPeople=itinerary_item.numPeople,
                dataSize=itinerary_item.dataSize,
                dataSizeExp=round(math.log10(itinerary_item.dataSize)),
                elapsedTime=elapsedTime,
                foundNumPeople=foundNumPeople,
                IsCloudMode=data_params.in_cloud_mode,
                CanAssumeNoDupesPerPartition=data_params.CanAssumeNoDupesPerPartition)
            success = True
            if foundNumPeople != itinerary_item.numPeople:
                write_failed_run(test_method, result, result_log_file)
                continue
            if rddout is not None:
                success = verifyCorrectnessRdd(
                    spark_session, itinerary_item, rddout)
            elif dfout is not None:
                success = verifyCorrectnessDf(
                    spark_session, itinerary_item, dfout)
            else:
                success = False
            if test_method.strategy_name not in test_runs:
                test_runs[test_method.strategy_name] = []
            test_runs[test_method.strategy_name].append(result)
            write_run_result(success, test_method, result, result_log_file)
            print("Took %f secs" % elapsedTime)
            del rddout
            del dfout
            gc.collect()
            time.sleep(1)
            print("")


def do_test_runs(args: Arguments, spark_session: TidySparkSession):
    testDataSizes = [x for x in [
        10**0 if '1' in args.sizes else None,
        10**1 if '10' in args.sizes else None,
        10**2 if '100' in args.sizes else None,
        10**3 if '1k' in args.sizes else None,
        10**4 if '10k' in args.sizes else None,
    ] if x is not None]
    srcDfListList = DoGenData(
        testDataSizes, spark_session.spark, args.ideosyncracies)
    runtests(srcDfListList, args, spark_session)


if __name__ == "__main__":
    args = parse_args()
    config = {
        "spark.sql.shuffle.partitions": args.ideosyncracies.SufflePartitions,
        "spark.default.parallelism": args.ideosyncracies.DefaultParallelism,
        "spark.rdd.compress": "false",
        "spark.python.worker.reuse": "false",
        "spark.port.maxRetries": "1",
        "spark.rpc.retry.wait": "10s",
        "spark.reducer.maxReqsInFlight": "1",
        "spark.storage.blockManagerHeartbeatTimeoutMs": "300s",
        "spark.network.timeout": "30s",
        "spark.shuffle.io.maxRetries": "10",
        "spark.shuffle.io.retryWait": "60s",
        "spark.sql.execution.arrow.pyspark.enabled": "true",
    }
    enable_hive_support = args.ideosyncracies.in_cloud_mode
    with TidySparkSession(config, enable_hive_support) as spark_session:
        do_test_runs(args, spark_session)
