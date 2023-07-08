#!python
# python -m Runner_Section
from typing import List, Tuple, Optional

import argparse
from dataclasses import dataclass
import gc
import random
import time

from PerfTestCommon import count_iter
from SectionPerfTest.SectionTypeDefs import DataSetDescription, PythonTestMethod, RunResult
from Utils.SparkUtils import NUM_EXECUTORS, TidySparkSession
from Utils.Utils import always_true

from SectionPerfTest.SectionDirectory import implementation_list, strategy_name_list
from SectionPerfTest.SectionRunResult import write_header, write_run_result, PYTHON_RESULT_FILE_PATH
from SectionPerfTest.SectionTestData import populateDatasets, available_data_sizes

DEBUG_ARGS = None if False else (
    []
    + '--size 1'.split()
    + '--runs 1'.split()
    # + '--random-seed 1234'.split()
    + ['--no-shuffle']
    # +'--strategy method_prepcsv_groupby'.split()
)


@dataclass(frozen=True)
class Arguments:
    make_new_data_files: bool
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
        '--new-files',
        default=False,
        action=argparse.BooleanOptionalAction)
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
        make_new_data_files=args.new_files,
        num_runs=args.runs,
        random_seed=args.random_seed,
        shuffle=args.shuffle,
        sizes=args.size,
        strategies=args.strategy,
    )


def infeasible(strategy: str, data_set: DataSetDescription) -> bool:
    dataNumStudents = data_set.dataSize // data_set.sectionMaximum
    match strategy:
        case 'method_nospark_single_threaded':
            return False
        case 'method_mappart_single_threaded':
            return dataNumStudents >= pow(10, 5)  # unrealiable
        case 'method_join_groupby':
            return dataNumStudents >= pow(10, 5)  # too slow
        case 'method_mappart_partials':
            return dataNumStudents >= pow(10, 7)  # unrealiable
        case 'method_asymreduce_partials':
            return dataNumStudents >= pow(10, 7)  # unrealiable
        case 'method_mappart_odd_even':
            return dataNumStudents >= pow(10, 7)  # unrealiable
        case 'method_join_groupby':
            return dataNumStudents >= pow(10, 7)  # times out
        case 'method_join_mappart':
            return dataNumStudents >= pow(10, 7)  # times out
        case 'method_prep_mappart':
            return dataNumStudents >= pow(10, 8)  # takes too long
        case 'method_prepcsv_groupby':
            return dataNumStudents >= pow(10, 8)  # times out
        case 'method_prep_groupby':
            return dataNumStudents >= pow(10, 8)  # times out
        case 'method_prepcsv_groupby':
            return False
        case _:
            raise ValueError(f"Unknown strategy {strategy}")


def do_test_runs(args: Arguments, spark_session: TidySparkSession):
    data_sets = {str(x.NumStudents): x for x in populateDatasets(
        args.make_new_data_files)}
    data_sets = {k: v for k, v in data_sets.items() if k in args.sizes}
    keyed_implementation_list = {
        x.strategy_name: x for x in implementation_list}
    test_run_itinerary: List[
        Tuple[PythonTestMethod, DataSetDescription]
    ] = [
        (test_method, data)
        for strategy in args.strategies
        if always_true(test_method := keyed_implementation_list[strategy])
        for data in data_sets.values()
        if not infeasible(strategy, data)
        for _ in range(0, args.num_runs)
    ]
    if args.random_seed is not None:
        random.seed(args.random_seed)
    if args.shuffle:
        random.shuffle(test_run_itinerary)
    with open(PYTHON_RESULT_FILE_PATH, 'at+') as file:
        write_header(file)
        for index, (test_method, data) in enumerate(test_run_itinerary):
            spark_session.log.info(
                "Working on %d of %d" %
                (index, len(test_run_itinerary)))
            startedTime = time.time()
            lst, rdd, df = test_method.delegate(spark_session, data)
            if lst is not None:
                foundNumStudents = len(lst)
            elif rdd is not None:
                print(f"output rdd has {rdd.getNumPartitions()} partitions")
                foundNumStudents = count_iter(rdd.toLocalIterator())
            elif df is not None:
                print(f"output rdd has {df.rdd.getNumPartitions()} partitions")
                foundNumStudents = count_iter(df.rdd.toLocalIterator())
                rdd = df.rdd
            else:
                raise ValueError("Not data returned")
            finishedTime = time.time()
            if rdd is not None and rdd.getNumPartitions() > NUM_EXECUTORS * 2:
                print(
                    f"{test_method.strategy_name} output rdd has {rdd.getNumPartitions()} partitions")
                findings = rdd.collect()
                print(f"size={len(findings)}!", findings)
                exit(1)
            actualNumStudents = data.dataSize // data.sectionMaximum
            result = RunResult(
                success=foundNumStudents == actualNumStudents,
                data=data,
                elapsed_time=finishedTime - startedTime,
                record_count=foundNumStudents)
            write_run_result(test_method, result, file)
            print(
                "%s Took %f secs" %
                (test_method.strategy_name,
                 finishedTime -
                 startedTime))
            del lst
            del df
            del rdd
            gc.collect()
            time.sleep(0.1)


if __name__ == "__main__":
    args = parse_args()
    config = {
        "spark.sql.shuffle.partitions": NUM_EXECUTORS * 2,
        "spark.rdd.compress": "false",
        "spark.worker.cleanup.enabled": "true",
        "spark.default.parallelism": 7,
        "spark.driver.memory": "2g",
        "spark.executor.memory": "3g",
        "spark.executor.memoryOverhead": "1g",
        "spark.port.maxRetries": "1",
        "spark.rpc.retry.wait": "10s",
        "spark.reducer.maxReqsInFlight": "1",
        "spark.executor.heartbeatInterval": "10s",
        "spark.network.timeout": "120s",
        # "spark.storage.blockManagerHeartbeatTimeoutMs": "300s",
        "spark.shuffle.io.maxRetries": "10",
        "spark.shuffle.io.retryWait": "60s",
        "spark.sql.execution.arrow.pyspark.enabled": "true",
    }
    with TidySparkSession(
        config,
        enable_hive_support=False
    ) as spark_session:
        do_test_runs(args, spark_session)
