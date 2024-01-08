#! python
# usage: (cd src; python -m challenges.deduplication.dedupe_pyspark_runner)
import argparse
import gc
import math
import random
import time
from dataclasses import dataclass
from typing import Literal, Optional

from pyspark import RDD
from pyspark.sql import Row

from challenges.deduplication.dedupe_generate_test_data import (
    DATA_SIZE_CODE_TO_DATA_SIZE, generate_test_data)
from challenges.deduplication.dedupe_record_runs import (
    RunResult, derive_run_log_file_path, write_header, write_run_result)
from challenges.deduplication.dedupe_strategy_directory import (
    STRATEGY_NAME_LIST, pyspark_implementation_list)
from challenges.deduplication.dedupe_test_data_types import (
    DataSet, ExecutionParameters)
from challenges.deduplication.domain_logic.dedupe_expected_results import (
    ItineraryItem, verify_correctness)
from perf_test_common import CalcEngine
from utils.tidy_spark_session import TidySparkSession
from utils.utils import always_true, set_random_seed

ENGINE = CalcEngine.PYSPARK
DEBUG_ARGS = None if False else (
    []
    + '--size 2'.split()
    + '--runs 1'.split()
    # + '--random-seed 1234'.split()
    + ['--no-shuffle']
    # + '--strategy  dedupe_fluent_nested_python'.split()
)
LOCAL_TEST_DATA_FILE_LOCATION = "d:/temp/SparkPerfTesting"
REMOTE_TEST_DATA_LOCATION = "wasb:///sparkperftesting"
MaximumProcessableSegment = pow(10, 5)


@dataclass(frozen=True)
class Arguments:
    num_runs: int
    random_seed: Optional[int]
    shuffle: bool
    sizes: list[str]
    strategies: list[str]
    exec_params: ExecutionParameters


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
        choices=sorted(DATA_SIZE_CODE_TO_DATA_SIZE.keys()),
        default=sorted([k for k, v in DATA_SIZE_CODE_TO_DATA_SIZE.items() if v.num_people > 1]),
        nargs="+")
    parser.add_argument(
        '--shuffle', default=True,
        action=argparse.BooleanOptionalAction)
    parser.add_argument(
        '--strategy',
        choices=STRATEGY_NAME_LIST,
        default=STRATEGY_NAME_LIST,
        nargs="+")
    if DEBUG_ARGS is None:
        args = parser.parse_args()
    else:
        args = parser.parse_args(DEBUG_ARGS)

    in_cloud_mode = args.cloud_mode
    if in_cloud_mode:
        num_executors = 40
        can_assume_no_dupes_per_partition = False
        default_parallelism = 2 * num_executors
        test_data_folder_location = REMOTE_TEST_DATA_LOCATION
    else:
        num_executors = 7
        can_assume_no_dupes_per_partition = args.assume_no_dupes_per_partition
        default_parallelism = 16
        test_data_folder_location = LOCAL_TEST_DATA_FILE_LOCATION

    return Arguments(
        num_runs=args.runs,
        random_seed=args.random_seed,
        shuffle=args.shuffle,
        sizes=args.size,
        strategies=args.strategy,
        exec_params=ExecutionParameters(
            InCloudMode=in_cloud_mode,
            NumExecutors=num_executors,
            CanAssumeNoDupesPerPartition=can_assume_no_dupes_per_partition,
            DefaultParallelism=default_parallelism,
            TestDataFolderLocation=test_data_folder_location,
        ))


def run_one_itinerary_step(
        index: int, num_iterinary_stops: int, itinerary_item: ItineraryItem,
        args: Arguments, spark_session: TidySparkSession
) -> tuple[bool, RunResult] | Literal["infeasible"]:
    exec_params = args.exec_params
    log = spark_session.log
    log.info("Working on %d of %d" % (index, num_iterinary_stops))
    test_method = itinerary_item.test_method
    startedTime = time.time()
    print("Working on %s %d %d" %
          (test_method.strategy_name, itinerary_item.data_set.num_people, itinerary_item.data_set.data_size))
    success = True
    try:
        answer_set = test_method.delegate(
            spark_session, exec_params, itinerary_item.data_set)
        if answer_set.feasible is False:
            return "infeasible"
        rddout: RDD[Row]
        if answer_set.rdd_row is not None:
            rddout = answer_set.rdd_row
        elif answer_set.spark_df is not None:
            rddout = answer_set.spark_df.rdd
        else:
            raise Exception(f"{itinerary_item.test_method.strategy_name} dit not returning anything")
        print("NumPartitions: in vs out ",
              itinerary_item.data_set.df.rdd.getNumPartitions(),
              rddout.getNumPartitions())
        if rddout.getNumPartitions() \
                > max(args.exec_params.DefaultParallelism,
                      itinerary_item.data_set.grouped_num_partitions):
            print(
                f"{test_method.strategy_name} output rdd has {rddout.getNumPartitions()} partitions")
            findings = rddout.collect()
            print(f"size={len(findings)}!")
            exit(1)
        foundPeople: list[Row] = rddout.collect()
    except Exception as exception:
        foundPeople = []
        exit(1)
        log.exception(exception)
        success = False
    elapsedTime = time.time() - startedTime
    foundNumPeople = len(foundPeople)
    success = verify_correctness(itinerary_item, foundPeople)
    assert success is True
    result = RunResult(
        numSources=itinerary_item.data_set.num_sources,
        actualNumPeople=itinerary_item.data_set.num_people,
        dataSize=itinerary_item.data_set.data_size,
        dataSizeExp=round(
            math.log10(
                itinerary_item.data_set.data_size)),
        elapsedTime=elapsedTime,
        foundNumPeople=foundNumPeople,
        IsCloudMode=exec_params.InCloudMode,
        CanAssumeNoDupesPerPartition=exec_params.CanAssumeNoDupesPerPartition)
    return success, result


def run_tests(
        data_sets: list[DataSet],
        args: Arguments,
        spark_session: TidySparkSession,
):
    keyed_implementation_list = {
        x.strategy_name: x for x in pyspark_implementation_list}
    itinerary: list[ItineraryItem] = [
        ItineraryItem(
            test_method=test_method,
            data_set=data_set,
        )
        for strategy in args.strategies
        if always_true(test_method := keyed_implementation_list[strategy])
        for data_set in data_sets
        for _ in range(args.num_runs)
    ]
    if args.random_seed is not None:
        set_random_seed(args.random_seed)
    if args.shuffle is True:
        random.shuffle(itinerary)
    with open(derive_run_log_file_path(ENGINE), 'at+') as result_log_file:
        write_header(result_log_file)
        for index, itinerary_item in enumerate(itinerary):
            match run_one_itinerary_step(
                    index=index,
                    num_iterinary_stops=len(itinerary),
                    itinerary_item=itinerary_item,
                    args=args,
                    spark_session=spark_session):
                case "infeasible":
                    pass
                case (success, result):
                    write_run_result(success, itinerary_item.test_method, result, result_log_file)
                    gc.collect()
                    time.sleep(0.1)
            print("")


def do_test_runs(
        args: Arguments,
        spark_session: TidySparkSession,
):
    data_sets = generate_test_data(
        args.sizes, spark_session.spark, args.exec_params)
    run_tests(data_sets, args, spark_session)


def main():
    args = parse_args()
    config = {
        "spark.sql.shuffle.partitions": args.exec_params.DefaultParallelism,
        "spark.default.parallelism": args.exec_params.DefaultParallelism,
        "spark.python.worker.reuse": "false",
        "spark.port.maxRetries": "1",
        "spark.rpc.retry.wait": "10s",
        "spark.reducer.maxReqsInFlight": "1",
        "spark.storage.blockManagerHeartbeatTimeoutMs": "7200s",
        "spark.executor.heartbeatInterval": "3600s",
        "spark.network.timeout": "36000s",
        "spark.shuffle.io.maxRetries": "10",
        "spark.shuffle.io.retryWait": "60s",
        "spark.sql.execution.arrow.pyspark.enabled": "true",
    }
    enable_hive_support = args.exec_params.InCloudMode
    with TidySparkSession(config, enable_hive_support) as spark_session:
        do_test_runs(args, spark_session)


if __name__ == "__main__":
    main()
