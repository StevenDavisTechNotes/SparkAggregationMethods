#! python
# usage: python -m src.challenges.bi_level.bi_level_python_only_runner
import argparse
import gc
import logging
import os
import random
import time
from dataclasses import dataclass

from spark_agg_methods_common_python.challenge_strategy_registry import (
    ChallengeResultLogFileRegistration, ChallengeStrategyRegistration, update_challenge_strategy_registration,
)
from spark_agg_methods_common_python.challenges.six_field_test_data.six_test_data_types import (
    SHARED_LOCAL_TEST_DATA_FILE_LOCATION, SixTestExecutionParameters,
)
from spark_agg_methods_common_python.perf_test_common import (
    ELAPSED_TIME_COLUMN_NAME, CalcEngine, Challenge, SolutionLanguage,
)
from spark_agg_methods_common_python.utils.utils import always_true, set_random_seed

from src.challenges.bi_level.bi_level_python_only_strategy_directory import (
    BI_LEVEL_STRATEGIES_USING_PYTHON_ONLY_REGISTRY,
)
from src.challenges.bi_level.bi_level_record_runs import (
    PYTHON_ONLY_RUN_LOG_FILE_PATH, BiLevelPythonRunResultFileWriter, BiLevelRunResult,
)
from src.challenges.bi_level.bi_level_test_data_types import DATA_SIZES_LIST_BI_LEVEL, BiLevelDataSetDescription
from src.challenges.six_field_test_data.six_runner_python_only_base import test_one_step_in_python_only_itinerary
from src.challenges.six_field_test_data.six_test_data_for_python_only import (
    ChallengeMethodPythonOnlyRegistration, DataSetPythonOnlyWithAnswer, populate_data_set_python_only,
)

logger = logging.getLogger(__name__)
LANGUAGE = SolutionLanguage.PYTHON
ENGINE = CalcEngine.PYTHON_ONLY
CHALLENGE = Challenge.BI_LEVEL

DEBUG_ARGS = None if True else (
    []
    + '--size 3_3_10'.split()
    + '--runs 1'.split()
    # + '--random-seed 1234'.split()
    + ['--no-shuffle']
    + ['--strategy',
       #    'bi_level_py_only_pd_grp_numpy',
       ]
)


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
    strategy_names = [x.strategy_name for x in BI_LEVEL_STRATEGIES_USING_PYTHON_ONLY_REGISTRY]

    parser = argparse.ArgumentParser()
    parser.add_argument('--random-seed', type=int)
    parser.add_argument('--runs', type=int, default=1)
    parser.add_argument('--size', choices=sizes, default=sizes, nargs="+")
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
            default_parallelism=1,
            test_data_folder_location=SHARED_LOCAL_TEST_DATA_FILE_LOCATION,
        ),
    )


def do_test_runs(
        args: Arguments,
) -> None:
    data_sets = populate_data_sets(args)
    keyed_implementation_list = {
        x.strategy_name: x for x in BI_LEVEL_STRATEGIES_USING_PYTHON_ONLY_REGISTRY}
    itinerary: list[tuple[ChallengeMethodPythonOnlyRegistration, DataSetPythonOnlyWithAnswer]] = [
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
            logger.info(
                "Working on %d of %d" % (index, len(itinerary)))
            print(f"Working on {challenge_method_registration.strategy_name} for {data_set.data_description.size_code}")
            base_run_result = test_one_step_in_python_only_itinerary(
                challenge=CHALLENGE,
                exec_params=args.exec_params,
                challenge_method_registration=challenge_method_registration,
                numerical_tolerance=challenge_method_registration.numerical_tolerance,
                data_set=data_set,
                correct_answer=data_set.answer,
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


def populate_data_sets(
        args: Arguments,
) -> list[DataSetPythonOnlyWithAnswer]:
    data_sets = [
        populate_data_set_python_only(
            exec_params=args.exec_params,
            data_size=size,
        )
        for size in DATA_SIZES_LIST_BI_LEVEL
        if size.size_code in args.sizes
    ]
    return data_sets


def update_challenge_registration():
    update_challenge_strategy_registration(
        language=LANGUAGE,
        engine=ENGINE,
        challenge=CHALLENGE,
        registration=ChallengeResultLogFileRegistration(
            result_file_path=os.path.abspath(PYTHON_ONLY_RUN_LOG_FILE_PATH),
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
                for x in BI_LEVEL_STRATEGIES_USING_PYTHON_ONLY_REGISTRY
            ]
        ),
    )


def main():
    args = parse_args()
    update_challenge_registration()
    do_test_runs(args)


if __name__ == "__main__":
    print(f"Running {__file__}")
    main()
    print("Done!")
