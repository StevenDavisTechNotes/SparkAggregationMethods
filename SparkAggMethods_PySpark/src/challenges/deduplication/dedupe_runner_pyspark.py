#! python
# usage: python -m src.challenges.deduplication.dedupe_pyspark_runner
# cSpell: ignore wasb, sparkperftesting, Reqs
import argparse
import datetime as dt
import gc
import random
import time
from dataclasses import dataclass
from typing import Literal

from pyspark import RDD
from pyspark.sql import DataFrame as PySparkDataFrame
from pyspark.sql import Row
from spark_agg_methods_common_python.challenge_strategy_registry import (
    ChallengeResultLogFileRegistration, ChallengeStrategyRegistration, update_challenge_strategy_registration,
)
from spark_agg_methods_common_python.challenges.deduplication.dedupe_record_runs import DedupeRunResult
from spark_agg_methods_common_python.challenges.deduplication.dedupe_test_data_types import (
    DedupeDataSetDescription, ExecutionParameters,
)
from spark_agg_methods_common_python.perf_test_common import (
    ELAPSED_TIME_COLUMN_NAME, CalcEngine, Challenge, NumericalToleranceExpectations, SolutionLanguage,
)
from spark_agg_methods_common_python.utils.utils import always_true, set_random_seed

from src.challenges.deduplication.dedupe_generate_test_data_pyspark import DATA_SIZE_LIST_DEDUPE, generate_test_data
from src.challenges.deduplication.dedupe_record_runs_pyspark import (
    DedupePysparkPersistedRunResultLog, DedupePysparkRunResultFileWriter,
)
from src.challenges.deduplication.dedupe_strategy_directory_pyspark import STRATEGIES_USING_PYSPARK_REGISTRY
from src.challenges.deduplication.dedupe_test_data_types_pyspark import DedupePySparkDataSet
from src.challenges.deduplication.domain_logic.dedupe_expected_results_pyspark import (
    DedupeItineraryItem, verify_correctness,
)
from src.utils.tidy_session_pyspark import TidySparkSession

LANGUAGE = SolutionLanguage.PYTHON
ENGINE = CalcEngine.PYSPARK
CHALLENGE = Challenge.DEDUPLICATION


DEBUG_ARGS = None if True else (
    []
    + '--size 2'.split()
    + '--runs 0'.split()
    # + '--random-seed 1234'.split()
    + ['--no-shuffle']
    # + ['--strategy',
    #    'dedupe_pyspark_df_grp_pandas',
    #    ]
)
LOCAL_TEST_DATA_FILE_LOCATION = "d:/temp/SparkPerfTesting"
REMOTE_TEST_DATA_LOCATION = "wasb:///sparkperftesting"
MaximumProcessableSegment = pow(10, 5)


@dataclass(frozen=True)
class Arguments:
    num_runs: int
    random_seed: int | None
    shuffle: bool
    sizes: list[str]
    strategy_names: list[str]
    exec_params: ExecutionParameters


def parse_args() -> Arguments:
    strategy_names = [x.strategy_name for x in STRATEGIES_USING_PYSPARK_REGISTRY]
    sizes = [x.size_code for x in DATA_SIZE_LIST_DEDUPE]
    default_sizes = [x.size_code for x in DATA_SIZE_LIST_DEDUPE if not x.debugging_only]

    parser = argparse.ArgumentParser()
    parser.add_argument('--assume-no-dupes-per-partition', default=True, action=argparse.BooleanOptionalAction)
    parser.add_argument('--cloud-mode', default=False, action=argparse.BooleanOptionalAction)
    parser.add_argument('--random-seed', type=int)
    parser.add_argument('--runs', type=int, default=1)
    parser.add_argument('--size', choices=sizes, default=default_sizes, nargs="*")
    parser.add_argument('--shuffle', default=True, action=argparse.BooleanOptionalAction)
    parser.add_argument('--strategy', choices=strategy_names, default=strategy_names, nargs="*")
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
        strategy_names=args.strategy,
        exec_params=ExecutionParameters(
            in_cloud_mode=in_cloud_mode,
            num_executors=num_executors,
            can_assume_no_dupes_per_partition=can_assume_no_dupes_per_partition,
            default_parallelism=default_parallelism,
            test_data_folder_location=test_data_folder_location,
        ))


def run_one_itinerary_step(
        index: int, num_itinerary_stops: int, itinerary_item: DedupeItineraryItem,
        args: Arguments, spark_session: TidySparkSession
) -> DedupeRunResult | Literal["infeasible"]:
    exec_params = args.exec_params
    log = spark_session.log
    log.info("Working on %d of %d" % (index, num_itinerary_stops))
    challenge_method_registration = itinerary_item.challenge_method_registration
    startedTime = time.time()
    print("Working on %s %d %d" %
          (challenge_method_registration.strategy_name,
           itinerary_item.data_set.data_description.num_people,
           itinerary_item.data_set.data_description.num_source_rows))
    success = True
    try:
        rdd_out: RDD[Row]
        match challenge_method_registration.delegate(
                spark_session=spark_session,
                exec_params=exec_params,
                data_set=itinerary_item.data_set,
        ):
            case RDD() as rdd_row:
                rdd_out = rdd_row
            case PySparkDataFrame() as spark_df:
                rdd_out = spark_df.rdd
            case "infeasible":
                return "infeasible"
            case _:
                raise Exception(
                    f"{itinerary_item.challenge_method_registration.strategy_name} dit not returning anything")
        print("NumPartitions: in vs out ",
              itinerary_item.data_set.df.rdd.getNumPartitions(),
              rdd_out.getNumPartitions())
        if rdd_out.getNumPartitions() \
                > max(args.exec_params.default_parallelism,
                      itinerary_item.data_set.grouped_num_partitions):
            print(
                f"{challenge_method_registration.strategy_name} output rdd has {rdd_out.getNumPartitions()} partitions")
            findings = rdd_out.collect()
            print(f"size={len(findings)}!")
            exit(1)
        found_people: list[Row] = rdd_out.collect()
    except Exception as exception:
        found_people = []
        exit(1)
        log.exception(exception)
        success = False
    finished_at = time.time()
    elapsed_time = finished_at - startedTime
    num_people_found = len(found_people)
    success = verify_correctness(itinerary_item, found_people)
    assert success is True
    data_description = itinerary_item.data_set.data_description
    result = DedupeRunResult(
        status="success" if success else "failure",
        num_sources=data_description.num_sources,
        num_people_actual=data_description.num_people,
        num_source_rows=data_description.num_source_rows,
        data_size_exponent=data_description.data_size_exponent,
        elapsed_time=elapsed_time,
        num_output_rows=num_people_found,
        num_people_found=num_people_found,
        in_cloud_mode=exec_params.in_cloud_mode,
        can_assume_no_duplicates_per_partition=exec_params.can_assume_no_dupes_per_partition,
        finished_at=dt.datetime.now().isoformat(),
    )
    return result


def run_tests(
        data_sets: list[DedupePySparkDataSet],
        args: Arguments,
        spark_session: TidySparkSession,
):
    keyed_implementation_list = {
        x.strategy_name: x for x in STRATEGIES_USING_PYSPARK_REGISTRY}
    itinerary = [
        DedupeItineraryItem(
            challenge_method_registration=challenge_method_registration,
            data_set=data_set,
        )
        for strategy_name in args.strategy_names
        if always_true(challenge_method_registration := keyed_implementation_list[strategy_name])
        for data_set in data_sets
        for _ in range(args.num_runs)
    ]
    if len(itinerary) == 0:
        print("No runs to execute.")
        return
    if args.random_seed is not None:
        set_random_seed(args.random_seed)
    if args.shuffle is True:
        random.shuffle(itinerary)
    with DedupePysparkRunResultFileWriter() as file:
        for index, itinerary_item in enumerate(itinerary):
            match run_one_itinerary_step(
                    index=index,
                    num_itinerary_stops=len(itinerary),
                    itinerary_item=itinerary_item,
                    args=args,
                    spark_session=spark_session):
                case (_, result):
                    if not itinerary_item.data_set.data_description.debugging_only:
                        file.write_run_result(itinerary_item.challenge_method_registration, result)
                    gc.collect()
                    time.sleep(0.1)
                case "infeasible":
                    pass
            print("")


def do_test_runs(
        args: Arguments,
        spark_session: TidySparkSession,
):
    data_sets = generate_test_data(
        args.sizes, spark_session.spark, args.exec_params)
    run_tests(data_sets, args, spark_session)


def update_challenge_registration():
    update_challenge_strategy_registration(
        language=LANGUAGE,
        engine=ENGINE,
        challenge=CHALLENGE,
        registration=ChallengeResultLogFileRegistration(
            result_file_path=DedupePysparkPersistedRunResultLog().log_file_path,
            regressor_column_name=DedupeDataSetDescription.regressor_field_name(),
            elapsed_time_column_name=ELAPSED_TIME_COLUMN_NAME,
            expected_regressor_values=[
                x.regressor_value
                for x in DATA_SIZE_LIST_DEDUPE
                if not x.debugging_only
            ],
            strategies=[
                ChallengeStrategyRegistration(
                    language=LANGUAGE,
                    engine=ENGINE,
                    challenge=CHALLENGE,
                    interface=x.interface,
                    strategy_name=x.strategy_name,
                    numerical_tolerance=NumericalToleranceExpectations.NOT_APPLICABLE.value,
                    requires_gpu=x.requires_gpu,
                )
                for x in STRATEGIES_USING_PYSPARK_REGISTRY
            ]
        ),
    )


def main():
    args = parse_args()
    update_challenge_registration()
    config = {
        "spark.sql.shuffle.partitions": args.exec_params.default_parallelism,
        "spark.default.parallelism": args.exec_params.default_parallelism,
        "spark.python.worker.reuse": "false",
        "spark.port.maxRetries": "1",
        "spark.reducer.maxReqsInFlight": "1",
        "spark.storage.blockManagerHeartbeatTimeoutMs": "7200s",
        "spark.executor.heartbeatInterval": "3600s",
        "spark.network.timeout": "36000s",
        "spark.shuffle.io.maxRetries": "10",
        "spark.shuffle.io.retryWait": "60s",
        "spark.sql.execution.arrow.pyspark.enabled": "true",
    }
    enable_hive_support = args.exec_params.in_cloud_mode
    with TidySparkSession(config, enable_hive_support) as spark_session:
        do_test_runs(args, spark_session)


if __name__ == "__main__":
    print(f"Running {__file__}")
    main()
    print("Done!")
