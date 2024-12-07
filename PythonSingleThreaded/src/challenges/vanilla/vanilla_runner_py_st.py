#! python
# usage: .\venv\Scripts\activate.ps1; python -O -m src.challenges.vanilla.vanilla_runner_py_only

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
    DATA_SIZES_LIST_VANILLA, VanillaDataSetDescription,
)
from spark_agg_methods_common_python.perf_test_common import (
    ELAPSED_TIME_COLUMN_NAME, CalcEngine, Challenge, RunnerArgumentsBase,
    SolutionLanguage, assemble_itinerary,
)
from spark_agg_methods_common_python.utils.platform import setup_logging

from src.challenges.six_field_test_data.six_runner_base_py_st import (
    run_one_step_in_python_single_threaded_itinerary,
)
from src.challenges.six_field_test_data.six_test_data_for_py_st import (
    SixDataSetPythonOnly, six_prepare_data_set_python_single_threaded,
)
from src.challenges.vanilla.vanilla_strategy_directory_py_st import (
    VANILLA_STRATEGY_REGISTRY_PYTHON_SINGLE_THREADED,
)

LANGUAGE = SolutionLanguage.PYTHON
ENGINE = CalcEngine.SINGLE_THREADED
CHALLENGE = Challenge.VANILLA


logger = logging.getLogger(__name__)

DEBUG_ARGS = None if True else (
    []
    + '--size 3_3_100m'.split()
    + '--runs 1'.split()
    # + '--random-seed 1234'.split()
    + ['--no-shuffle']
    + ['--strategy',
       #    'vanilla_py_st_pd_grp_numpy',
       #    'vanilla_py_st_pd_grp_numba',
       #    'vanilla_py_st_pd_prog_numpy',
       #    'vanilla_py_mt_queue_pd_prog_numpy_1_reader',
       'vanilla_py_mt_queue_pd_prog_numpy',
       ]
)


@dataclass(frozen=True)
class VanillaDataSetWAnswerPythonOnly(SixDataSetPythonOnly):
    answer: pd.DataFrame


@dataclass(frozen=True)
class Arguments(RunnerArgumentsBase):
    exec_params: SixTestExecutionParameters


def parse_args() -> Arguments:
    sizes = [x.size_code for x in DATA_SIZES_LIST_VANILLA]
    strategy_names = sorted(x.strategy_name for x in VANILLA_STRATEGY_REGISTRY_PYTHON_SINGLE_THREADED)

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
            default_parallelism=1,
            num_executors=1,
        ),
    )


def prepare_data_sets(
        args: Arguments,
) -> list[VanillaDataSetWAnswerPythonOnly]:
    data_sets = [
        VanillaDataSetWAnswerPythonOnly(
            data_description=size,
            data=six_prepare_data_set_python_single_threaded(
                exec_params=args.exec_params,
                data_description=size,
            ),
            answer=fetch_six_data_set_answer(CHALLENGE, size),
        )
        for size in DATA_SIZES_LIST_VANILLA
        if size.size_code in args.sizes
    ]
    return data_sets


class VanillaPythonOnlyRunResultFileWriter(VanillaPythonRunResultFileWriter):
    RUN_LOG_FILE_PATH: str = os.path.abspath('results/vanilla_python_only_runs.csv')

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
        x.strategy_name: x for x in VANILLA_STRATEGY_REGISTRY_PYTHON_SINGLE_THREADED}
    keyed_data_sets = {x.data_description.size_code: x for x in prepare_data_sets(args)}
    with VanillaPythonOnlyRunResultFileWriter() as file:
        for index, (strategy_name, size_code) in enumerate(itinerary):
            challenge_method_registration = keyed_implementation_list[strategy_name]
            data_set = keyed_data_sets[size_code]
            logger.info("Working on %d of %d" % (index, len(itinerary)))
            logger.info(f"Working on {challenge_method_registration.strategy_name} "
                        f"for {data_set.data_description.size_code}")
            base_run_result = run_one_step_in_python_single_threaded_itinerary(
                challenge=CHALLENGE,
                exec_params=args.exec_params,
                challenge_method_registration=challenge_method_registration,
                numerical_tolerance=challenge_method_registration.numerical_tolerance,
                data_set=data_set,
                correct_answer=data_set.answer,
            )
            if base_run_result is None:
                logger.info("Failed!")
                break
            if not data_set.data_description.debugging_only:
                file.write_run_result(challenge_method_registration, VanillaRunResult(
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
            result_file_path=VanillaPythonOnlyRunResultFileWriter.RUN_LOG_FILE_PATH,
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
                for x in VANILLA_STRATEGY_REGISTRY_PYTHON_SINGLE_THREADED
            ]
        ),
    )


def main(
) -> None:
    logger.info(f"Running {__file__}")
    try:
        args = parse_args()
        update_challenge_registration()
        do_test_runs(args)
    except KeyboardInterrupt:
        logger.warning("Interrupted!")
        return
    logger.info("Done!")


if __name__ == "__main__":
    setup_logging()
    main()
