#! python
# usage: python -O -m src.challenges.bi_level.bi_level_runner_pyspark
import argparse
import gc
import logging
import os
import time
from dataclasses import dataclass

import pandas as pd
from spark_agg_methods_common_python.challenge_strategy_registry import (
    ChallengeResultLogFileRegistration, ChallengeStrategyRegistration, update_challenge_strategy_registration,
)
from spark_agg_methods_common_python.challenges.bi_level.bi_level_record_runs import (
    BiLevelPythonRunResultFileWriter, BiLevelRunResult,
)
from spark_agg_methods_common_python.challenges.bi_level.bi_level_test_data_types import (
    BI_LEVEL_RESULT_COLUMNS, DATA_SIZES_LIST_BI_LEVEL, BiLevelDataSetDescription,
)
from spark_agg_methods_common_python.challenges.six_field_test_data.six_test_data_types import (
    SixTestExecutionParameters, fetch_six_data_set_answer,
)
from spark_agg_methods_common_python.perf_test_common import (
    ELAPSED_TIME_COLUMN_NAME, LOCAL_NUM_EXECUTORS, CalcEngine, Challenge, RunnerArgumentsBase, RunResultBase,
    SolutionLanguage, assemble_itinerary,
)
from spark_agg_methods_common_python.utils.platform import setup_logging

from src.challenges.bi_level.bi_level_strategy_directory_pyspark import BI_LEVEL_STRATEGIES_USING_PYSPARK_REGISTRY
from src.challenges.six_field_test_data.six_runner_base_pyspark import (
    run_one_step_in_pyspark_itinerary, six_spark_config_base,
)
from src.challenges.six_field_test_data.six_test_data_for_pyspark import (
    SixFieldDataSetPyspark, six_prepare_data_set_pyspark,
)
from src.utils.tidy_session_pyspark import TidySparkSession

logger = logging.getLogger(__name__)


DEBUG_ARGS = None if True else (
    []
    + '--size 3_3_10'.split()
    + '--runs 1'.split()
    # + '--random-seed 1234'.split()
    + ['--no-shuffle']
    + ['--strategy',
       #    'bi_level_pyspark_rdd_grp_map',
       #    'bi_level_pyspark_rdd_map_part',
       #    'bi_level_pyspark_rdd_reduce_1',
       'bi_level_pyspark_rdd_reduce_2',
       ]
)

LANGUAGE = SolutionLanguage.PYTHON
ENGINE = CalcEngine.PYSPARK
CHALLENGE = Challenge.BI_LEVEL


@dataclass(frozen=True)
class BiLevelDataSetWAnswerPyspark(SixFieldDataSetPyspark):
    answer: pd.DataFrame


@dataclass(frozen=True)
class Arguments(RunnerArgumentsBase):
    exec_params: SixTestExecutionParameters


def parse_args() -> Arguments:
    sizes = [x.size_code for x in DATA_SIZES_LIST_BI_LEVEL]
    strategy_names = sorted(x.strategy_name for x in BI_LEVEL_STRATEGIES_USING_PYSPARK_REGISTRY)

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
        exec_params=SixTestExecutionParameters(
            default_parallelism=2 * LOCAL_NUM_EXECUTORS,
            num_executors=LOCAL_NUM_EXECUTORS,
        ),
    )


def prepare_data_sets(
        args: Arguments,
        spark_session: TidySparkSession,
) -> list[BiLevelDataSetWAnswerPyspark]:
    data_sets = [
        BiLevelDataSetWAnswerPyspark(
            data_description=size,
            data=six_prepare_data_set_pyspark(
                spark_session,
                args.exec_params,
                data_description=size,
            ),
            answer=fetch_six_data_set_answer(
                CHALLENGE,
                size,
                spark_logger=spark_session.logger,
            ),
        )
        for size in DATA_SIZES_LIST_BI_LEVEL
        if size.size_code in args.sizes
    ]
    return data_sets


class BiLevelPysparkRunResultFileWriter(BiLevelPythonRunResultFileWriter):
    RUN_LOG_FILE_PATH: str = os.path.abspath('results/bi_level_pyspark_runs.csv')

    def __init__(self):
        super().__init__(
            engine=ENGINE,
            rel_log_file_path=__class__.RUN_LOG_FILE_PATH,
        )


def do_test_runs(
        args: Arguments,
        spark_session: TidySparkSession
) -> None:
    logger = spark_session.logger
    itinerary = assemble_itinerary(args)
    if len(itinerary) == 0:
        logger.info("No runs to execute.")
        return
    keyed_implementation_list = {
        x.strategy_name: x for x in BI_LEVEL_STRATEGIES_USING_PYSPARK_REGISTRY}
    keyed_data_sets = {x.data_description.size_code: x for x in prepare_data_sets(args, spark_session)}
    with BiLevelPysparkRunResultFileWriter() as file:
        for index, (strategy_name, size_code) in enumerate(itinerary):
            challenge_method_registration = keyed_implementation_list[strategy_name]
            data_set = keyed_data_sets[size_code]
            logger.info("Working on %d of %d" % (index, len(itinerary)))
            logger.info(f"Working on {challenge_method_registration.strategy_name} "
                        f"for {data_set.data_description.size_code}")
            match run_one_step_in_pyspark_itinerary(
                challenge=CHALLENGE,
                spark_session=spark_session,
                exec_params=args.exec_params,
                challenge_method_registration=challenge_method_registration,
                result_columns=BI_LEVEL_RESULT_COLUMNS,
                data_set=data_set,
                correct_answer=data_set.answer,
            ):
                case ("infeasible", _):
                    pass
                case RunResultBase() as run_result:
                    if not data_set.data_description.debugging_only:
                        file.write_run_result(
                            challenge_method_registration=challenge_method_registration,
                            run_result=BiLevelRunResult(
                                num_source_rows=data_set.data_description.num_source_rows,
                                elapsed_time=run_result.elapsed_time,
                                num_output_rows=run_result.num_output_rows,
                                relative_cardinality_between_groupings=(
                                    data_set.data_description.relative_cardinality_between_groupings
                                ),
                                finished_at=run_result.finished_at,
                            ))
                case _:
                    raise ValueError("Unexpected return type")
            gc.collect()
            time.sleep(0.1)


def update_challenge_registration():
    update_challenge_strategy_registration(
        language=LANGUAGE,
        engine=ENGINE,
        challenge=CHALLENGE,
        registration=ChallengeResultLogFileRegistration(
            result_file_path=BiLevelPysparkRunResultFileWriter.RUN_LOG_FILE_PATH,
            regressor_column_name=BiLevelDataSetDescription.regressor_field_name(),
            elapsed_time_column_name=ELAPSED_TIME_COLUMN_NAME,
            expected_regressor_values=[
                x.regressor_value
                for x in DATA_SIZES_LIST_BI_LEVEL
                if not x.debugging_only
            ],
            strategies=[
                ChallengeStrategyRegistration(
                    language=LANGUAGE,
                    engine=ENGINE,
                    challenge=CHALLENGE,
                    interface=x.interface,
                    strategy_name=x.strategy_name,
                    numerical_tolerance=x.numerical_tolerance.value,
                    requires_gpu=x.requires_gpu,
                )
                for x in BI_LEVEL_STRATEGIES_USING_PYSPARK_REGISTRY
            ]
        ),
    )


def main():
    logger.info(f"Running {__file__}")
    try:
        args = parse_args()
        update_challenge_registration()
        with TidySparkSession(
            six_spark_config_base(args.exec_params),
            enable_hive_support=False
        ) as spark_session:
            do_test_runs(args, spark_session)
    except KeyboardInterrupt:
        logger.warning("Interrupted!")
        return
    logger.info("Done!")


if __name__ == "__main__":
    setup_logging()
    main()
