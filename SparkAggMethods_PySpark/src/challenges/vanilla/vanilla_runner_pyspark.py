#! python
# usage: python -m src.challenges.vanilla.vanilla_pyspark_runner
import argparse
import gc
import random
import time
from dataclasses import dataclass

import pandas as pd
from spark_agg_methods_common_python.challenge_strategy_registry import (
    ChallengeResultLogFileRegistration, ChallengeStrategyRegistration, update_challenge_strategy_registration,
)
from spark_agg_methods_common_python.challenges.six_field_test_data.six_test_data_types import (
    SixTestExecutionParameters, fetch_six_data_set_answer,
)
from spark_agg_methods_common_python.challenges.vanilla.vanilla_record_runs import VanillaRunResult
from spark_agg_methods_common_python.challenges.vanilla.vanilla_test_data_types import (
    DATA_SIZES_LIST_VANILLA, VANILLA_RESULT_COLUMNS, VanillaDataSetDescription,
)
from spark_agg_methods_common_python.perf_test_common import (
    ELAPSED_TIME_COLUMN_NAME, LOCAL_NUM_EXECUTORS, CalcEngine, Challenge, SolutionLanguage,
)
from spark_agg_methods_common_python.utils.utils import set_random_seed

from src.challenges.six_field_test_data.six_runner_base_pyspark import test_one_step_in_pyspark_itinerary
from src.challenges.six_field_test_data.six_test_data_for_pyspark import (
    SixFieldDataSetPyspark, six_populate_data_set_pyspark,
)
from src.challenges.vanilla.vanilla_record_runs_pyspark import (
    VanillaPysparkPersistedRunResultLog, VanillaPysparkRunResultFileWriter,
)
from src.challenges.vanilla.vanilla_strategy_directory_pyspark import VANILLA_STRATEGIES_USING_PYSPARK_REGISTRY
from src.utils.tidy_session_pyspark import TidySparkSession

LANGUAGE = SolutionLanguage.PYTHON
ENGINE = CalcEngine.PYSPARK
CHALLENGE = Challenge.VANILLA


DEBUG_ARGS = None if False else (
    []
    # + '--size 3_3_1'.split()
    + '--runs 0'.split()
    # + '--random-seed 1234'.split()
    + ['--no-shuffle']
    # + ['--strategy',
    #    'vanilla_pyspark_rdd_grp_map',
    #    'vanilla_pyspark_rdd_mappart',
    #    'vanilla_pyspark_rdd_reduce',
    #    ]
)


@dataclass(frozen=True)
class VanillaDataSetWAnswerPyspark(SixFieldDataSetPyspark):
    answer: pd.DataFrame


@dataclass(frozen=True)
class Arguments:
    num_runs: int
    random_seed: int | None
    shuffle: bool
    sizes: list[str]
    strategy_names: list[str]
    exec_params: SixTestExecutionParameters


def parse_args() -> Arguments:
    sizes = [x.size_code for x in DATA_SIZES_LIST_VANILLA]
    strategy_names = [x.strategy_name for x in VANILLA_STRATEGIES_USING_PYSPARK_REGISTRY]

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
            data=six_populate_data_set_pyspark(
                spark_session,
                args.exec_params,
                data_description=size,
            ),
            answer=fetch_six_data_set_answer(CHALLENGE, size),
        )
        for size in DATA_SIZES_LIST_VANILLA
        if size.size_code in args.sizes
    ]
    return data_sets


def do_test_runs(
        args: Arguments,
        spark_session: TidySparkSession,
) -> None:
    itinerary: list[tuple[str, str]] = [
        (strategy_name, size_code)
        for strategy_name in args.strategy_names
        for size_code in args.sizes
        for _ in range(0, args.num_runs)
    ]
    if len(itinerary) == 0:
        print("No runs to execute.")
        return
    if args.random_seed is not None:
        set_random_seed(args.random_seed)
    if args.shuffle:
        random.shuffle(itinerary)
    keyed_implementation_list = {
        x.strategy_name: x for x in VANILLA_STRATEGIES_USING_PYSPARK_REGISTRY}
    keyed_data_sets = {x.data_description.size_code: x for x in prepare_data_sets(args, spark_session)}
    with VanillaPysparkRunResultFileWriter() as file:
        for index, (strategy_name, size_code) in enumerate(itinerary):
            challenge_method_registration = keyed_implementation_list[strategy_name]
            data_set = keyed_data_sets[size_code]
            spark_session.log.info(
                "Working on %d of %d" % (index, len(itinerary)))
            print(f"Working on {challenge_method_registration.strategy_name} for {data_set.data_description.size_code}")
            base_run_result = test_one_step_in_pyspark_itinerary(
                challenge=CHALLENGE,
                spark_session=spark_session,
                exec_params=args.exec_params,
                challenge_method_registration=challenge_method_registration,
                result_columns=VANILLA_RESULT_COLUMNS,
                data_set=data_set,
                correct_answer=data_set.answer,
            )
            if base_run_result is None:
                continue
            if data_set.data_description.debugging_only:
                continue
            file.write_run_result(
                challenge_method_registration=challenge_method_registration,
                run_result=VanillaRunResult(
                    num_source_rows=data_set.data_description.num_source_rows,
                    elapsed_time=base_run_result.elapsed_time,
                    num_output_rows=base_run_result.num_output_rows,
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
            result_file_path=VanillaPysparkPersistedRunResultLog().log_file_path,
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
                for x in VANILLA_STRATEGIES_USING_PYSPARK_REGISTRY
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
