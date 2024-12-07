#!python
# usage: .\venv\Scripts\activate.ps1; python -O -m src.challenges.deduplication.dedupe_pyspark_runner
# cSpell: ignore wasb, sparkperftesting, Reqs
import argparse
import datetime as dt
import gc
import logging
import os
import time
from dataclasses import dataclass
from functools import reduce
from typing import Literal

import pyspark.sql.functions as func
from pyspark import RDD
from pyspark.sql import DataFrame as PySparkDataFrame
from pyspark.sql import Row
from spark_agg_methods_common_python.challenge_strategy_registry import (
    ChallengeResultLogFileRegistration, ChallengeStrategyRegistration,
    update_challenge_strategy_registration,
)
from spark_agg_methods_common_python.challenges.deduplication.dedupe_record_runs import (
    DedupePythonRunResultFileWriter, DedupeRunResult,
)
from spark_agg_methods_common_python.challenges.deduplication.dedupe_test_data_types import (
    DATA_SIZE_LIST_DEDUPE, DEDUPE_SOURCE_CODES, DedupeDataSetDescription,
    dedupe_derive_source_test_data_file_paths,
)
from spark_agg_methods_common_python.perf_test_common import (
    ELAPSED_TIME_COLUMN_NAME, CalcEngine, Challenge,
    NumericalToleranceExpectations, RunnerArgumentsBase, RunResultBase,
    SolutionLanguage, assemble_itinerary,
)
from spark_agg_methods_common_python.utils.platform import setup_logging
from spark_agg_methods_common_python.utils.utils import int_divide_round_up

from src.challenges.deduplication.dedupe_strategy_directory_pyspark import (
    DEDUPE_STRATEGY_REGISTRY_PYSPARK,
)
from src.challenges.deduplication.dedupe_test_data_types_pyspark import (
    DedupeChallengeMethodPythonPysparkRegistration, DedupeDataSetPySpark,
    DedupeExecutionParametersPyspark, RecordSparseStruct,
)
from src.challenges.deduplication.domain_logic.dedupe_expected_results_pyspark import (
    verify_correctness,
)
from src.utils.tidy_session_pyspark import TidySparkSession

logger = logging.getLogger(__name__)

LANGUAGE = SolutionLanguage.PYTHON
ENGINE = CalcEngine.PYSPARK
CHALLENGE = Challenge.DEDUPLICATION
MAX_DATA_POINTS_PER_PARTITION: int = 10000


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
MaximumProcessableSegment = pow(10, 5)


@dataclass(frozen=True)
class Arguments(RunnerArgumentsBase):
    exec_params: DedupeExecutionParametersPyspark


def parse_args() -> Arguments:
    sizes = [x.size_code for x in DATA_SIZE_LIST_DEDUPE]
    default_sizes = [x.size_code for x in DATA_SIZE_LIST_DEDUPE if not x.debugging_only]
    strategy_names = sorted(x.strategy_name for x in DEDUPE_STRATEGY_REGISTRY_PYSPARK)

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
    else:
        num_executors = 7
        can_assume_no_dupes_per_partition = args.assume_no_dupes_per_partition
        default_parallelism = 16

    return Arguments(
        num_runs=args.runs,
        random_seed=args.random_seed,
        shuffle=args.shuffle,
        sizes=args.size,
        strategy_names=args.strategy,
        exec_params=DedupeExecutionParametersPyspark(
            in_cloud_mode=in_cloud_mode,
            num_executors=num_executors,
            can_assume_no_dupes_per_partition=can_assume_no_dupes_per_partition,
            default_parallelism=default_parallelism,
        ))


def prepare_data_sets(
    data_size_code_list: list[str],
    spark_session: TidySparkSession,
    exec_params: DedupeExecutionParametersPyspark
) -> list[DedupeDataSetPySpark]:
    logger = spark_session.logger
    spark = spark_session.spark

    all_data_sets: list[DedupeDataSetPySpark] = []
    target_data_size_list = [x for x in DATA_SIZE_LIST_DEDUPE if x.size_code in data_size_code_list]
    for target_data_size in sorted(target_data_size_list, key=lambda x: x.num_people):
        source_data_file_names = dedupe_derive_source_test_data_file_paths(target_data_size)
        single_source_data_frames: list[PySparkDataFrame]
        if exec_params.can_assume_no_dupes_per_partition:
            single_source_data_frames = [
                (spark.read
                 .csv(
                     source_data_file_names[source_code],
                     schema=RecordSparseStruct)
                 .coalesce(1)
                 .withColumn("SourceId", func.lit(i_source)))
                for i_source, source_code in enumerate(DEDUPE_SOURCE_CODES)
            ]
        else:
            single_source_data_frames = [
                (spark.read
                 .csv(
                     source_data_file_names[source_code],
                     schema=RecordSparseStruct)
                 .withColumn("SourceId", func.lit(i_source)))
                for i_source, source_code in enumerate(DEDUPE_SOURCE_CODES)
            ]

        def combine_sources(num: int) -> PySparkDataFrame:
            return reduce(
                lambda dfA, dfB: dfA.unionAll(dfB),
                [single_source_data_frames[i] for i in range(num)]
            )
        quantized_data_sets = {
            2: combine_sources(2),
            3: combine_sources(3),
            6: combine_sources(6),
        }
        if exec_params.can_assume_no_dupes_per_partition is False:
            # then scramble since we can't assume no duplicates
            quantized_data_sets = {
                k: df.repartition(exec_params.num_executors)
                for k, df in quantized_data_sets.items()
            }
        for df in quantized_data_sets.values():
            df.persist()
        df = quantized_data_sets[target_data_size.num_sources]
        actual_data_size = df.count()
        if actual_data_size != target_data_size.num_source_rows:
            logger.error(
                f"Bad result for {target_data_size.num_people}, "
                f"{target_data_size.num_sources} sources, expected "
                f"{target_data_size.num_source_rows}, got {actual_data_size}!")
            exit(11)
        num_partitions = max(
            exec_params.default_parallelism,
            int_divide_round_up(
                actual_data_size,
                MAX_DATA_POINTS_PER_PARTITION))
        all_data_sets.append(DedupeDataSetPySpark(
            data_description=target_data_size,
            grouped_num_partitions=num_partitions,
            df_source=df,
        ))
    return all_data_sets


class DedupePysparkRunResultFileWriter(DedupePythonRunResultFileWriter):
    RUN_LOG_FILE_PATH: str = os.path.abspath('results/dedupe_pyspark_runs.csv')

    def __init__(self):
        super().__init__(
            engine=ENGINE,
            rel_log_file_path=__class__.RUN_LOG_FILE_PATH,
        )


def do_test_runs(
        args: Arguments,
        spark_session: TidySparkSession,
):
    logger = spark_session.logger
    itinerary = assemble_itinerary(args)
    if len(itinerary) == 0:
        logger.info("No runs to execute.")
        return
    keyed_data_sets = {x.data_description.size_code: x for x in prepare_data_sets(
        args.sizes, spark_session, args.exec_params)}
    keyed_implementation_list = {
        x.strategy_name: x for x in DEDUPE_STRATEGY_REGISTRY_PYSPARK}
    with DedupePysparkRunResultFileWriter() as file:
        for index, (strategy_name, size_code) in enumerate(itinerary):
            challenge_method_registration = keyed_implementation_list[strategy_name]
            data_set = keyed_data_sets[size_code]
            match run_one_itinerary_step(
                    index=index,
                    num_itinerary_stops=len(itinerary),
                    challenge_method_registration=challenge_method_registration,
                    data_set=data_set,
                    args=args,
                    spark_session=spark_session):
                case RunResultBase() as result:
                    if not data_set.data_description.debugging_only:
                        file.write_run_result(challenge_method_registration, result)
                case ("infeasible", _):
                    pass
                case "interrupted":
                    break
                case base_run_result:
                    raise ValueError(f"Unexpected result type {type(base_run_result)}")
            logger.info("")
            gc.collect()
            time.sleep(0.01)


def run_one_itinerary_step(
        index: int,
        num_itinerary_stops: int,
        challenge_method_registration: DedupeChallengeMethodPythonPysparkRegistration,
        data_set: DedupeDataSetPySpark,
        args: Arguments,
        spark_session: TidySparkSession
) -> DedupeRunResult | tuple[Literal["infeasible"], str] | Literal["interrupted"]:
    exec_params = args.exec_params
    logger = spark_session.logger
    logger.info("Working on %d of %d" % (index, num_itinerary_stops))
    logger.info(f"Working on {challenge_method_registration.strategy_name} "
                f"for {data_set.data_description.size_code}")
    startedTime = time.time()
    success = True
    try:
        rdd_out: RDD[Row]
        match challenge_method_registration.delegate(
                spark_session=spark_session,
                exec_params=exec_params,
                data_set=data_set,
        ):
            case RDD() as rdd_row:
                rdd_out = rdd_row
            case PySparkDataFrame() as spark_df:
                rdd_out = spark_df.rdd
            case ("infeasible", msg):
                return "infeasible", msg
            case base_run_result:
                raise ValueError(
                    f"{challenge_method_registration.strategy_name} returned a {type(base_run_result)}")
        logger.info("NumPartitions: in vs out ",
                    data_set.df_source.rdd.getNumPartitions(),
                    rdd_out.getNumPartitions())
        if rdd_out.getNumPartitions() \
                > max(args.exec_params.default_parallelism,
                      data_set.grouped_num_partitions):
            logger.info(f"{challenge_method_registration.strategy_name} output rdd "
                        f"has {rdd_out.getNumPartitions()} partitions")
            findings = rdd_out.collect()
            logger.info(f"size={len(findings)}!")
            exit(1)
        found_people: list[Row] = rdd_out.collect()
    except KeyboardInterrupt:
        return "interrupted"
    except Exception as exception:
        found_people = []
        logger.error(exception)
        exit(1)
    finished_at = time.time()
    elapsed_time = finished_at - startedTime
    num_people_found = len(found_people)
    success = verify_correctness(data_set.data_description, found_people, spark_session)
    assert success is True
    data_description = data_set.data_description
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


def update_challenge_registration():
    update_challenge_strategy_registration(
        language=LANGUAGE,
        engine=ENGINE,
        challenge=CHALLENGE,
        registration=ChallengeResultLogFileRegistration(
            result_file_path=DedupePysparkRunResultFileWriter.RUN_LOG_FILE_PATH,
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
                for x in DEDUPE_STRATEGY_REGISTRY_PYSPARK
            ]
        ),
    )


def main():
    logger.info(f"Running {__file__}")
    try:
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
    except KeyboardInterrupt:
        logger.warning("Interrupted!")
        return
    logger.info("Done!")


if __name__ == "__main__":
    setup_logging()
    main()
