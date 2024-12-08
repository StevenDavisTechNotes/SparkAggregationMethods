#!python
# usage: p.\venv\Scripts\activate.ps1; python -O -m src.challenges.vanilla.vanilla_runner_pyspark
import argparse
import gc
import logging
import os
import time
from dataclasses import dataclass

import pandas as pd
from spark_agg_methods_common_python.challenge_strategy_registry import (
    ChallengeResultLogFileRegistration, ChallengeStrategyRegistration,
    update_challenge_strategy_registration,
)
from spark_agg_methods_common_python.challenges.six_field_test_data.six_test_data_types import (
    SixTestExecutionParameters, fetch_six_data_set_answer,
)
from spark_agg_methods_common_python.challenges.vanilla.vanilla_record_runs import (
    VanillaPythonRunResultFileWriter, VanillaRunResult,
)
from spark_agg_methods_common_python.challenges.vanilla.vanilla_test_data_types import (
    DATA_SIZES_LIST_VANILLA, VANILLA_RESULT_COLUMNS, VanillaDataSetDescription,
)
from spark_agg_methods_common_python.perf_test_common import (
    ELAPSED_TIME_COLUMN_NAME, LOCAL_NUM_EXECUTORS, CalcEngine, Challenge,
    RunnerArgumentsBase, RunResultBase, SolutionLanguage, assemble_itinerary,
)
from spark_agg_methods_common_python.utils.platform import setup_logging

from src.challenges.six_field_test_data.six_runner_base_pyspark import (
    six_run_one_step_in_pyspark_itinerary, six_spark_config_base,
)
from src.challenges.six_field_test_data.six_test_data_for_pyspark import (
    SixFieldDataSetPyspark, six_prepare_data_set_pyspark,
)
from src.challenges.vanilla.vanilla_strategy_directory_pyspark import (
    VANILLA_STRATEGY_REGISTRY_PYSPARK,
)
from src.utils.tidy_session_pyspark import TidySparkSession

logger = logging.getLogger(__name__)

LANGUAGE = SolutionLanguage.PYTHON
ENGINE = CalcEngine.PYSPARK
CHALLENGE = Challenge.VANILLA


DEBUG_ARGS = None if True else (
    []
    + '--size 3_3_10m'.split()
    + '--runs 1'.split()
    # + '--random-seed 1234'.split()
    + ['--no-shuffle']
    + ['--strategy',
       #    'vanilla_pyspark_df_grp_builtin',
       #    'vanilla_pyspark_df_grp_numba',
       #    'vanilla_pyspark_df_grp_numpy',
       #    'vanilla_pyspark_df_grp_pandas',
       #    'vanilla_pyspark_rdd_grp_map',
       #    'vanilla_pyspark_rdd_mappart',
       'vanilla_pyspark_rdd_reduce',
       #    'vanilla_pyspark_sql',
       ]
)


@dataclass(frozen=True)
class VanillaDataSetWAnswerPyspark(SixFieldDataSetPyspark):
    answer: pd.DataFrame


@dataclass(frozen=True)
class Arguments(RunnerArgumentsBase):
    exec_params: SixTestExecutionParameters


def parse_args() -> Arguments:
    sizes = [x.size_code for x in DATA_SIZES_LIST_VANILLA]
    strategy_names = sorted(x.strategy_name for x in VANILLA_STRATEGY_REGISTRY_PYSPARK)

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
) -> list[VanillaDataSetWAnswerPyspark]:
    data_sets = [
        VanillaDataSetWAnswerPyspark(
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
        for size in DATA_SIZES_LIST_VANILLA
        if size.size_code in args.sizes
    ]
    return data_sets


class VanillaPysparkRunResultFileWriter(VanillaPythonRunResultFileWriter):
    RUN_LOG_FILE_PATH: str = os.path.abspath('results/vanilla_pyspark_runs.csv')

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
    num_itinerary_stops = len(itinerary)
    if num_itinerary_stops == 0:
        logger.info("No runs to execute.")
        return
    keyed_implementation_list = {
        x.strategy_name: x for x in VANILLA_STRATEGY_REGISTRY_PYSPARK}
    keyed_data_sets = {x.data_description.size_code: x for x in prepare_data_sets(args, spark_session)}
    with VanillaPysparkRunResultFileWriter() as file:
        for index, (strategy_name, size_code) in enumerate(itinerary):
            challenge_method_registration = keyed_implementation_list[strategy_name]
            data_set = keyed_data_sets[size_code]
            logger.info(
                "Step {index}/{out_of}: {strategy} for {size_code}"
                .format(
                    index=index, out_of=num_itinerary_stops,
                    strategy=challenge_method_registration.strategy_name,
                    size_code=data_set.data_description.size_code))
            try:
                match six_run_one_step_in_pyspark_itinerary(
                    challenge_method_registration=challenge_method_registration,
                    challenge=CHALLENGE,
                    correct_answer=data_set.answer,
                    data_set=data_set,
                    exec_params=args.exec_params,
                    result_columns=VANILLA_RESULT_COLUMNS,
                    spark_session=spark_session,
                ):
                    case RunResultBase() as run_result:
                        if not data_set.data_description.debugging_only:
                            file.write_run_result(
                                challenge_method_registration=challenge_method_registration,
                                run_result=VanillaRunResult(
                                    num_source_rows=data_set.data_description.num_source_rows,
                                    elapsed_time=run_result.elapsed_time,
                                    num_output_rows=run_result.num_output_rows,
                                    finished_at=run_result.finished_at,
                                ))
                    case ("infeasible", _):
                        pass
                    case answer:
                        raise ValueError(f"Unexpected return type: {type(answer)}")
            except KeyboardInterrupt as ex:
                raise ex
            except Exception as ex:
                logger.error(
                    "Error in {strategy_name} for {size_code}: {ex}"
                    .format(
                        strategy_name=challenge_method_registration.strategy_name,
                        size_code=data_set.data_description.size_code,
                        ex=ex,
                    )
                )
                raise ex
            gc.collect()
            time.sleep(0.1)


def update_challenge_registration():
    update_challenge_strategy_registration(
        language=LANGUAGE,
        engine=ENGINE,
        challenge=CHALLENGE,
        registration=ChallengeResultLogFileRegistration(
            result_file_path=VanillaPysparkRunResultFileWriter.RUN_LOG_FILE_PATH,
            regressor_column_name=VanillaDataSetDescription.regressor_field_name(),
            elapsed_time_column_name=ELAPSED_TIME_COLUMN_NAME,
            expected_regressor_values=[
                x.regressor_value
                for x in DATA_SIZES_LIST_VANILLA
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
                for x in VANILLA_STRATEGY_REGISTRY_PYSPARK
            ]
        ),
    )


def main() -> None:
    logger.info(f"Running {__file__}")
    args = parse_args()
    update_challenge_registration()
    with TidySparkSession(
        six_spark_config_base(args.exec_params),
        enable_hive_support=False
    ) as spark_session:
        do_test_runs(args, spark_session)
    logger.info("Done!")


if __name__ == "__main__":
    setup_logging()
    try:
        main()
    except KeyboardInterrupt:
        logger.warning("Interrupted!")
