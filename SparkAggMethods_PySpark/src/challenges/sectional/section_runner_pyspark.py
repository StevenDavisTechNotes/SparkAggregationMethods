#! python
# usage: python -O -m src.challenges.sectional.section_pyspark_runner
# cSpell: ignore wasb, sparkperftesting, Reqs
import argparse
import datetime as dt
import gc
import logging
import os
import time
from dataclasses import dataclass
from typing import Iterable, Literal

import pandas as pd
from pyspark import RDD
from pyspark.sql import DataFrame as PySparkDataFrame
from spark_agg_methods_common_python.challenge_strategy_registry import (
    ChallengeResultLogFileRegistration, ChallengeStrategyRegistration, update_challenge_strategy_registration,
)
from spark_agg_methods_common_python.challenges.sectional.section_persist_test_data import AnswerFileSectional
from spark_agg_methods_common_python.challenges.sectional.section_record_runs import (
    SectionPythonRunResultFileWriter, SectionRunResult,
)
from spark_agg_methods_common_python.challenges.sectional.section_test_data_types import (
    DATA_SIZE_LIST_SECTIONAL, SECTION_SIZE_MAXIMUM, SectionDataSetDescription, StudentSummary,
    section_derive_source_test_data_file_path, section_verify_correctness,
)
from spark_agg_methods_common_python.perf_test_common import (
    ELAPSED_TIME_COLUMN_NAME, LOCAL_NUM_EXECUTORS, CalcEngine, Challenge, NumericalToleranceExpectations,
    RunnerArgumentsBase, SolutionLanguage, assemble_itinerary,
)
from spark_agg_methods_common_python.utils.pandas_helpers import make_pd_dataframe_from_list_of_named_tuples
from spark_agg_methods_common_python.utils.platform import setup_logging
from spark_agg_methods_common_python.utils.utils import int_divide_round_up

from src.challenges.sectional.section_strategy_directory_pyspark import SECTIONAL_STRATEGIES_USING_PYSPARK_REGISTRY
from src.challenges.sectional.section_test_data_types_pyspark import (
    MAXIMUM_PROCESSABLE_SEGMENT, SectionChallengeMethodPysparkRegistration, SectionDataSetPyspark,
    SectionExecutionParametersPyspark,
)
from src.utils.tidy_session_pyspark import TidySparkSession

logger = logging.getLogger(__name__)

DEBUG_ARGS = None if True else (
    []
    + '--size 1'.split()
    # + ['--no-check']
    + '--runs 0'.split()
    # + '--random-seed 1234'.split()
    + ['--no-shuffle']
    # + ['--strategy',
    #    ]
)

LANGUAGE = SolutionLanguage.PYTHON
ENGINE = CalcEngine.PYSPARK
CHALLENGE = Challenge.SECTIONAL


@dataclass(frozen=True)
class Arguments(RunnerArgumentsBase):
    exec_params: SectionExecutionParametersPyspark


def parse_args() -> Arguments:
    sizes = [x.size_code for x in DATA_SIZE_LIST_SECTIONAL]
    strategy_names = sorted(x.strategy_name for x in SECTIONAL_STRATEGIES_USING_PYSPARK_REGISTRY)

    parser = argparse.ArgumentParser()
    parser.add_argument('--random-seed', type=int)
    parser.add_argument('--runs', type=int, default=1)
    parser.add_argument('--size', choices=sizes, default=sizes, nargs="*")
    parser.add_argument('--shuffle', default=True, action=argparse.BooleanOptionalAction)
    parser.add_argument('--strategy', choices=strategy_names, default=strategy_names, nargs="*")
    if DEBUG_ARGS is None:
        args = parser.parse_args()
    else:
        args = parser.parse_args(DEBUG_ARGS)
    return Arguments(
        num_runs=args.runs,
        random_seed=args.random_seed,
        shuffle=args.shuffle,
        sizes=args.size,
        strategy_names=args.strategy,
        exec_params=SectionExecutionParametersPyspark(
            # for ExecutionParametersBase
            default_parallelism=2 * LOCAL_NUM_EXECUTORS,
            num_executors=LOCAL_NUM_EXECUTORS,
            # for SectionExecutionParametersBase
            # section_maximum=SECTION_SIZE_MAXIMUM,
            # for SectionExecutionArgumentsPySpark
            maximum_processable_segment=MAXIMUM_PROCESSABLE_SEGMENT,
        )
    )


def prepare_data_sets(
        args: Arguments,
) -> list[SectionDataSetPyspark]:
    exec_params = args.exec_params
    datasets: list[SectionDataSetPyspark] = []
    num_students = 1
    for data_description in DATA_SIZE_LIST_SECTIONAL:
        if data_description.size_code not in args.sizes:
            continue
        num_students = data_description.num_students
        file_path = section_derive_source_test_data_file_path(data_description)
        data_size = num_students * SECTION_SIZE_MAXIMUM
        assert os.path.exists(file_path)
        src_num_partitions = max(
            exec_params.default_parallelism,
            int_divide_round_up(
                data_size,
                exec_params.maximum_processable_segment,
            ),
        )
        correct_answer = AnswerFileSectional.read_answer_file_sectional(data_description)
        datasets.append(
            SectionDataSetPyspark(
                # for SectionDataSetBase
                data_description=data_description,
                correct_answer=correct_answer,
                section_maximum=SECTION_SIZE_MAXIMUM,
                # for SectionDataSetPyspark
                maximum_processable_segment=exec_params.maximum_processable_segment,
                source_data_file_path=file_path,
                target_num_partitions=src_num_partitions,
            ))
    return datasets


class SectionPysparkRunResultFileWriter(SectionPythonRunResultFileWriter):
    RUN_LOG_FILE_PATH: str = os.path.abspath('results/section_pyspark_runs.csv')

    def __init__(self):
        super().__init__(
            engine=ENGINE,
            rel_log_file_path=__class__.RUN_LOG_FILE_PATH,
        )


def do_test_runs(
        args: Arguments,
        spark_session: TidySparkSession,
) -> None:
    logger = spark_session.logger
    itinerary = assemble_itinerary(args)
    if len(itinerary) == 0:
        logger.info("No runs to execute.")
        return
    keyed_implementation_list = {
        x.strategy_name: x for x in SECTIONAL_STRATEGIES_USING_PYSPARK_REGISTRY}
    keyed_data_sets = {x.data_description.size_code: x for x in prepare_data_sets(args)}
    with SectionPysparkRunResultFileWriter() as file:
        for index, (strategy_name, size_code) in enumerate(itinerary):
            challenge_method_registration = keyed_implementation_list[strategy_name]
            data_set = keyed_data_sets[size_code]
            logger.info("Working on %d of %d" % (index, len(itinerary)))
            logger.info(f"Working on {challenge_method_registration.strategy_name} "
                        f"for {data_set.data_description.size_code}")
            run_result = run_one_itinerary_step(args, spark_session, challenge_method_registration, data_set)
            match run_result:
                case ("infeasible", _):
                    pass
                case _:
                    if not data_set.data_description.debugging_only:
                        file.write_run_result(challenge_method_registration, run_result)
            gc.collect()
            time.sleep(0.1)


def run_one_itinerary_step(
        args: Arguments,
        spark_session: TidySparkSession,
        challenge_method_registration: SectionChallengeMethodPysparkRegistration,
        data_set: SectionDataSetPyspark,
) -> SectionRunResult | tuple[Literal["infeasible"], str]:
    logger = spark_session.logger
    startedTime = time.time()
    rdd: RDD | None = None
    found_students: Iterable[StudentSummary] | pd.DataFrame
    match challenge_method_registration.delegate(
        spark_session=spark_session,
        exec_params=args.exec_params,
        data_set=data_set,
    ):
        case ("infeasible", msg):
            return "infeasible", msg
        case PySparkDataFrame() as df:
            logger.info(f"output rdd has {df.rdd.getNumPartitions()} partitions")
            rdd = df.rdd
            # found_students_iterable = [StudentSummary(*x) for x in rdd.toLocalIterator()]
            found_students = df.toPandas()
        case RDD() as rdd:
            logger.info(f"output rdd has {rdd.getNumPartitions()} partitions")
            count = rdd.count()
            logger.info("Got a count", count)
            found_students = rdd.toLocalIterator()
        case pd.DataFrame() as df:
            found_students = df
        case list() as iter_tuple:
            found_students = iter_tuple
        case _:
            raise ValueError("Unexpected return type")
    if rdd is not None and rdd.getNumPartitions() > max(
        args.exec_params.default_parallelism,
        data_set.target_num_partitions,
    ):
        logger.info(f"{challenge_method_registration.strategy_name} "
                    f"output rdd has {rdd.getNumPartitions()} partitions")
        findings = rdd.collect()
        logger.info(f"size={len(findings)}!", findings)
        exit(1)
    df_concrete_students: pd.DataFrame
    match found_students:
        case pd.DataFrame() as df:
            df_concrete_students = df
            finishedTime = time.time()
        case list() as concrete_students:
            finishedTime = time.time()
            df_concrete_students = make_pd_dataframe_from_list_of_named_tuples(
                concrete_students,
                row_type=StudentSummary,
            )
        case _:
            raise ValueError("Unexpected return type")
    error_message = section_verify_correctness(
        data_set,
        df_concrete_students,
    )
    status = "success" if error_message is None else "failure"
    if error_message is not None:
        logger.error("section_verify_correctness returned", error_message)
    data_description = data_set.data_description
    result = SectionRunResult(
        status=status,
        num_source_rows=data_description.num_source_rows,
        section_maximum=data_set.section_maximum,
        elapsed_time=finishedTime - startedTime,
        num_output_rows=len(df_concrete_students),
        finished_at=dt.datetime.now().isoformat(),
    )
    logger.info("%s Took %f secs" % (challenge_method_registration.strategy_name, finishedTime - startedTime))
    return result


def update_challenge_registration():
    update_challenge_strategy_registration(
        language=LANGUAGE,
        engine=ENGINE,
        challenge=CHALLENGE,
        registration=ChallengeResultLogFileRegistration(
            result_file_path=SectionPysparkRunResultFileWriter.RUN_LOG_FILE_PATH,
            regressor_column_name=SectionDataSetDescription.regressor_field_name(),
            elapsed_time_column_name=ELAPSED_TIME_COLUMN_NAME,
            expected_regressor_values=[
                x.regressor_value
                for x in DATA_SIZE_LIST_SECTIONAL
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
                for x in SECTIONAL_STRATEGIES_USING_PYSPARK_REGISTRY
            ]
        ),
    )


def spark_configs(
        default_parallelism: int,
) -> dict[str, str | int]:
    return {
        "spark.sql.shuffle.partitions": default_parallelism,
        "spark.default.parallelism": default_parallelism,
        "spark.worker.cleanup.enabled": "true",
        "spark.driver.memory": "2g",
        "spark.executor.memory": "3g",
        "spark.executor.memoryOverhead": "1g",
        "spark.port.maxRetries": "1",
        "spark.reducer.maxReqsInFlight": "1",
        "spark.executor.heartbeatInterval": "3600s",
        "spark.network.timeout": "36000s",
        "spark.shuffle.io.maxRetries": "10",
        "spark.shuffle.io.retryWait": "600s",
        "spark.sql.execution.arrow.pyspark.enabled": "true",
    }


def main():
    logger.info(f"Running {__file__}")
    try:
        args = parse_args()
        update_challenge_registration()
        with TidySparkSession(
            spark_configs(args.exec_params.default_parallelism),
            enable_hive_support=False,
        ) as spark_session:
            os.chdir(spark_session.python_src_code_path)
            do_test_runs(args, spark_session)
    except KeyboardInterrupt:
        logger.warning("Interrupted!")
        return
    logger.info("Done!")


if __name__ == "__main__":
    setup_logging()
    main()
