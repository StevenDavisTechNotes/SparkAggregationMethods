#! python
# usage: python -m src.challenges.bi_level.bi_level_dask_runner
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
    DATA_SIZES_LIST_BI_LEVEL, BiLevelDataSetDescription,
)
from spark_agg_methods_common_python.challenges.six_field_test_data.six_test_data_types import (
    SixTestExecutionParameters, fetch_six_data_set_answer,
)
from spark_agg_methods_common_python.perf_test_common import (
    ELAPSED_TIME_COLUMN_NAME, LOCAL_NUM_EXECUTORS, CalcEngine, Challenge, RunnerArgumentsBase, SolutionLanguage,
    assemble_itinerary,
)

from src.challenges.bi_level.bi_level_strategy_directory_dask import BI_LEVEL_STRATEGIES_USING_DASK_REGISTRY
from src.challenges.six_field_test_data.six_runner_dask_base import test_one_step_in_dask_itinerary
from src.challenges.six_field_test_data.six_test_data_for_dask import SixTestDataSetDask, six_populate_data_set_dask

LANGUAGE = SolutionLanguage.PYTHON
ENGINE = CalcEngine.DASK
CHALLENGE = Challenge.BI_LEVEL

logger = logging.getLogger(__name__)

DEBUG_ARGS = None if False else (
    []
    + '--size 3_3_10'.split()
    + '--runs 0'.split()
    # + '--random-seed 1234'.split()
    # + ['--no-shuffle']
    # + ['--strategy',       ]
)


@dataclass(frozen=True)
class BiLevelDataSetWAnswerDask(SixTestDataSetDask):
    answer: pd.DataFrame


@dataclass(frozen=True)
class Arguments(RunnerArgumentsBase):
    have_gpu: bool
    exec_params: SixTestExecutionParameters


def parse_args() -> Arguments:
    sizes = [x.size_code for x in DATA_SIZES_LIST_BI_LEVEL]
    default_sizes = [x.size_code for x in DATA_SIZES_LIST_BI_LEVEL if not x.debugging_only]
    strategy_names = [x.strategy_name for x in BI_LEVEL_STRATEGIES_USING_DASK_REGISTRY]

    parser = argparse.ArgumentParser()
    parser.add_argument('--random-seed', type=int)
    parser.add_argument('--runs', type=int, default=1)
    parser.add_argument('--size', choices=sizes, default=default_sizes, nargs="*")
    parser.add_argument('--shuffle', default=True, action=argparse.BooleanOptionalAction)
    parser.add_argument('--have-gpu', default=True, action=argparse.BooleanOptionalAction)
    parser.add_argument('--strategy', choices=strategy_names, default=strategy_names, nargs="*")
    if DEBUG_ARGS is None:
        args = parser.parse_args()
    else:
        args = parser.parse_args(DEBUG_ARGS)
    return Arguments(
        have_gpu=args.have_gpu,
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
) -> list[BiLevelDataSetWAnswerDask]:
    data_sets = [
        BiLevelDataSetWAnswerDask(
            data_description=size,
            data=six_populate_data_set_dask(
                exec_params=args.exec_params,
                data_description=size,
            ),
            answer=fetch_six_data_set_answer(CHALLENGE, size),
        )
        for size in DATA_SIZES_LIST_BI_LEVEL
        if size.size_code in args.sizes
    ]
    return data_sets


class BiLevelDaskRunResultFileWriter(BiLevelPythonRunResultFileWriter):
    RUN_LOG_FILE_PATH: str = os.path.abspath('results/bi_level_dask_runs.csv')

    def __init__(self):
        super().__init__(
            engine=ENGINE,
            rel_log_file_path=__class__.RUN_LOG_FILE_PATH,
        )


def do_test_runs(
        args: Arguments,
) -> None:
    itinerary = assemble_itinerary(args)
    if len(itinerary) == 0:
        logger.info("No runs to execute.")
        return
    keyed_implementation_list = {
        x.strategy_name: x for x in BI_LEVEL_STRATEGIES_USING_DASK_REGISTRY}
    keyed_data_sets = {x.data_description.size_code: x for x in prepare_data_sets(args)}
    with BiLevelDaskRunResultFileWriter() as file:
        for index, (strategy_name, size_code) in enumerate(itinerary):
            challenge_method_registration = keyed_implementation_list[strategy_name]
            data_set = keyed_data_sets[size_code]
            logger.info("Working on %d of %d" % (index, len(itinerary)))
            logger.info(f"Working on {challenge_method_registration.strategy_name} "
                        f"for {data_set.data_description.size_code}")
            base_run_result = test_one_step_in_dask_itinerary(
                challenge=CHALLENGE,
                exec_params=args.exec_params,
                challenge_method_registration=challenge_method_registration,
                data_set=data_set,
                correct_answer=data_set.answer,
            )
            if base_run_result is None:
                continue
            if data_set.data_description.debugging_only:
                continue
            file.write_run_result(
                challenge_method_registration,
                BiLevelRunResult(
                    num_source_rows=base_run_result.num_source_rows,
                    elapsed_time=base_run_result.elapsed_time,
                    num_output_rows=base_run_result.num_output_rows,
                    relative_cardinality_between_groupings=(
                        data_set.data_description.relative_cardinality_between_groupings
                    ),
                    finished_at=base_run_result.finished_at,
                )
            )
            gc.collect()
            time.sleep(0.1)


def update_challenge_registration():
    update_challenge_strategy_registration(
        language=LANGUAGE,
        engine=ENGINE,
        challenge=CHALLENGE,
        registration=ChallengeResultLogFileRegistration(
            result_file_path=BiLevelDaskRunResultFileWriter.RUN_LOG_FILE_PATH,
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
                for x in BI_LEVEL_STRATEGIES_USING_DASK_REGISTRY
            ]
        ),
    )


def do_with_client():
    args = parse_args()
    update_challenge_registration()
    return do_test_runs(args)


def main():
    # with DaskClient(
    #         processes=True,
    #         n_workers=LOCAL_NUM_EXECUTORS,
    #         threads_per_worker=1,
    # ) as dask_client:
    do_with_client()


if __name__ == "__main__":
    print(f"Running {__file__}")
    main()
    print("Done!")
