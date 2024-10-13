#! python
# usage: python -m src.challenges.bi_level.bi_level_pyspark_runner
import argparse
import gc
import os
import random
import time
from dataclasses import dataclass

from src.challenge_strategy_registry import (
    ChallengeResultLogFileRegistration, ChallengeStrategyRegistration, update_challenge_strategy_registration,
)
from src.challenges.bi_level.bi_level_record_runs import (
    PYTHON_PYSPARK_RUN_LOG_FILE_PATH, BiLevelPythonRunResultFileWriter, BiLevelRunResult,
)
from src.challenges.bi_level.bi_level_strategy_directory import STRATEGIES_USING_PYSPARK_REGISTRY
from src.challenges.bi_level.bi_level_test_data_types import (
    DATA_SIZES_LIST_BI_LEVEL, RESULT_COLUMNS, BiLevelDataSetDescription,
)
from src.perf_test_common import ELAPSED_TIME_COLUMN_NAME, CalcEngine, SolutionLanguage
from src.six_field_test_data.six_generate_test_data import (
    SixFieldChallengeMethodPythonPysparkRegistration, SixFieldDataSetPysparkWithAnswer, populate_data_set_pyspark,
)
from src.six_field_test_data.six_runner_base import test_one_step_in_pyspark_itinerary
from src.six_field_test_data.six_test_data_types import (
    SHARED_LOCAL_TEST_DATA_FILE_LOCATION, Challenge, SixTestExecutionParameters,
)
from src.utils.tidy_spark_session import LOCAL_NUM_EXECUTORS, TidySparkSession
from src.utils.utils import always_true, set_random_seed

DEBUG_ARGS = None if True else (
    []
    + '--size 3_3k_100'.split()
    + '--runs 1'.split()
    # + '--random-seed 1234'.split()
    # + ['--no-shuffle']
    # + ['--strategy',
    #    'bi_level_pyspark_rdd_grp_map',
    #    'bi_level_pyspark_rdd_map_part',
    #    'bi_level_pyspark_rdd_reduce_1',
    #    'bi_level_pyspark_rdd_reduce_2',
    #    ]
)

LANGUAGE = SolutionLanguage.PYTHON
ENGINE = CalcEngine.PYSPARK
CHALLENGE = Challenge.BI_LEVEL


@dataclass(frozen=True)
class Arguments:
    num_runs: int
    random_seed: int | None
    shuffle: bool
    sizes: list[str]
    strategy_names: list[str]
    exec_params: SixTestExecutionParameters


def parse_args() -> Arguments:
    sizes = [x.size_code for x in DATA_SIZES_LIST_BI_LEVEL]
    strategy_names = [x.strategy_name for x in STRATEGIES_USING_PYSPARK_REGISTRY]

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
            test_data_folder_location=SHARED_LOCAL_TEST_DATA_FILE_LOCATION,
        ),
    )


def populate_data_sets(
        args: Arguments,
        spark_session: TidySparkSession,
) -> list[SixFieldDataSetPysparkWithAnswer]:
    data_sets = [
        populate_data_set_pyspark(
            spark_session, args.exec_params,
            data_size=data_size,
        )
        for data_size in DATA_SIZES_LIST_BI_LEVEL
        if data_size.size_code in args.sizes
    ]
    return data_sets


def do_test_runs(
        args: Arguments,
        spark_session: TidySparkSession
) -> None:
    data_sets = populate_data_sets(args, spark_session)
    keyed_implementation_list = {
        x.strategy_name: x for x in STRATEGIES_USING_PYSPARK_REGISTRY}
    itinerary: list[tuple[SixFieldChallengeMethodPythonPysparkRegistration, SixFieldDataSetPysparkWithAnswer]] = [
        (challenge_method_registration, data_set)
        for strategy_name in args.strategy_names
        if always_true(challenge_method_registration := keyed_implementation_list[strategy_name])
        for data_set in data_sets
        for _ in range(0, args.num_runs)
    ]
    if len(itinerary) == 0:
        print("No runs to execute.")
        return
    if args.random_seed is not None:
        set_random_seed(args.random_seed)
    if args.shuffle:
        random.shuffle(itinerary)
    with BiLevelPythonRunResultFileWriter(ENGINE) as file:
        for index, (challenge_method_registration, data_set) in enumerate(itinerary):
            spark_session.log.info("Working on %d of %d" %
                                   (index, len(itinerary)))
            print(f"Working on {challenge_method_registration.strategy_name} for {data_set.data_description.size_code}")
            base_run_result = test_one_step_in_pyspark_itinerary(
                challenge=CHALLENGE,
                spark_session=spark_session,
                exec_params=args.exec_params,
                challenge_method_registration=challenge_method_registration,
                result_columns=RESULT_COLUMNS,
                data_set=data_set,
            )
            if base_run_result is None:
                continue
            if data_set.data_description.debugging_only:
                continue
            file.write_run_result(
                challenge_method_registration=challenge_method_registration,
                run_result=BiLevelRunResult(
                    num_source_rows=data_set.data_description.num_source_rows,
                    elapsed_time=base_run_result.elapsed_time,
                    num_output_rows=base_run_result.num_output_rows,
                    relative_cardinality_between_groupings=(
                        data_set.data_description.relative_cardinality_between_groupings
                    ),
                    finished_at=base_run_result.finished_at,
                ))
            gc.collect()
            time.sleep(0.1)


def update_challenge_registration():
    update_challenge_strategy_registration(
        language=LANGUAGE,
        engine=ENGINE,
        challenge=CHALLENGE,
        registration=ChallengeResultLogFileRegistration(
            result_file_path=os.path.abspath(PYTHON_PYSPARK_RUN_LOG_FILE_PATH),
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
        "spark.driver.memory": "2g",
        "spark.executor.memory": "3g",
        "spark.executor.memoryOverhead": "1g",
        "spark.sql.execution.arrow.pyspark.enabled": "true",
    }
    with TidySparkSession(
        config,
        enable_hive_support=False
    ) as spark_session:
        do_test_runs(args, spark_session)


if __name__ == "__main__":
    print(f"Running {__file__}")
    main()
    print("Done!")
