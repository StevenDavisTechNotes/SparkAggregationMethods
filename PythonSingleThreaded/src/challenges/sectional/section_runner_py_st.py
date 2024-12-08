#!python
# usage: .\venv\Scripts\activate.ps1; python -O -m src.challenges.sectional.section_runner_py_st
# cSpell: ignore wasb, sparkperftesting, Reqs
import argparse
import datetime as dt
import gc
import logging
import os
import time
from dataclasses import dataclass
from typing import Literal

import pandas as pd
from spark_agg_methods_common_python.challenge_strategy_registry import (
    ChallengeResultLogFileRegistration, ChallengeStrategyRegistration,
    update_challenge_strategy_registration,
)
from spark_agg_methods_common_python.challenges.sectional.section_persist_test_data import (
    AnswerFileSectional,
)
from spark_agg_methods_common_python.challenges.sectional.section_record_runs import (
    SectionPythonRunResultFileWriter, SectionRunResult,
)
from spark_agg_methods_common_python.challenges.sectional.section_test_data_types import (
    DATA_SIZE_LIST_SECTIONAL, SECTION_SIZE_MAXIMUM, SectionDataSetDescription,
    StudentSummary, section_derive_source_test_data_file_path,
    section_verify_correctness,
)
from spark_agg_methods_common_python.perf_test_common import (
    ELAPSED_TIME_COLUMN_NAME, LOCAL_NUM_EXECUTORS, CalcEngine, Challenge,
    NumericalToleranceExpectations, RunnerArgumentsBase, RunResultBase,
    SolutionLanguage, assemble_itinerary,
)
from spark_agg_methods_common_python.utils.pandas_helpers import (
    make_pd_dataframe_from_list_of_named_tuples,
)
from spark_agg_methods_common_python.utils.platform import setup_logging

from src.challenges.sectional.section_strategy_directory_py_st import (
    SECTIONAL_STRATEGY_REGISTRY_PYTHON_SINGLE_THREADED,
)
from src.challenges.sectional.section_test_data_types_py_st import (
    SectionChallengeMethodPythonSingleThreadedRegistration, SectionDataSetPyST,
    SectionExecutionParametersPyOnly,
)

logger = logging.getLogger(__name__)

DEBUG_ARGS = None if True else (
    []
    + '--size 1 10 100 1000 10000'.split()
    + '--runs 0'.split()
    # + '--random-seed 1234'.split()
    # + ['--no-shuffle']
    # + ['--strategy',
    #    ]
)

LANGUAGE = SolutionLanguage.PYTHON
ENGINE = CalcEngine.SINGLE_THREADED
CHALLENGE = Challenge.SECTIONAL


@dataclass(frozen=True)
class Arguments(RunnerArgumentsBase):
    check_answers: bool
    exec_params: SectionExecutionParametersPyOnly


def parse_args() -> Arguments:
    sizes = [x.size_code for x in DATA_SIZE_LIST_SECTIONAL]
    strategy_names = sorted(x.strategy_name for x in SECTIONAL_STRATEGY_REGISTRY_PYTHON_SINGLE_THREADED)

    parser = argparse.ArgumentParser()
    parser.add_argument('--check', default=True, action=argparse.BooleanOptionalAction,
                        help="When debugging skipping this will speed up the startup")
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
        check_answers=args.check,
        num_runs=args.runs,
        random_seed=args.random_seed,
        shuffle=args.shuffle,
        sizes=args.size,
        strategy_names=args.strategy,
        exec_params=SectionExecutionParametersPyOnly(
            # for ExecutionParametersBase
            default_parallelism=2 * LOCAL_NUM_EXECUTORS,
            num_executors=LOCAL_NUM_EXECUTORS,
        ),
    )


def prepare_data_sets(
        args: Arguments,
) -> list[SectionDataSetPyST]:
    datasets: list[SectionDataSetPyST] = []
    for data_description in DATA_SIZE_LIST_SECTIONAL:
        if data_description.size_code not in args.sizes:
            continue
        file_path = section_derive_source_test_data_file_path(data_description)
        assert os.path.exists(file_path)
        correct_answer = AnswerFileSectional.read_answer_file_sectional(data_description)
        datasets.append(
            SectionDataSetPyST(
                data_description=data_description,
                correct_answer=correct_answer,
                section_maximum=SECTION_SIZE_MAXIMUM,
            ))
    return datasets


class SectionPythonSTRunResultFileWriter(SectionPythonRunResultFileWriter):
    RUN_LOG_FILE_PATH: str = os.path.abspath('results/section_python_single_threaded_runs.csv')

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
        x.strategy_name: x for x in SECTIONAL_STRATEGY_REGISTRY_PYTHON_SINGLE_THREADED}
    keyed_data_sets = {x.data_description.size_code: x for x in prepare_data_sets(args)}
    with SectionPythonSTRunResultFileWriter() as file:
        for index, (strategy_name, size_code) in enumerate(itinerary):
            challenge_method_registration = keyed_implementation_list[strategy_name]
            data_set = keyed_data_sets[size_code]
            logger.info(
                "Working on %d of %d" %
                (index, len(itinerary)))
            logger.info(f"Working on {challenge_method_registration.strategy_name} "
                        f"for {data_set.data_description.num_source_rows}")
            try:
                match section_run_one_itinerary_step(args, challenge_method_registration, data_set):
                    case RunResultBase() as run_result:
                        if not data_set.data_description.debugging_only:
                            file.write_run_result(challenge_method_registration, run_result)
                    case ("infeasible", _):
                        pass
                    case other:
                        raise ValueError(
                            "{strategy_name} unexpected returned a {other_type}"
                            .format(
                                strategy_name=challenge_method_registration.strategy_name,
                                other_type=type(other)
                            )
                        )
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


def section_run_one_itinerary_step(
        args: Arguments,
        challenge_method_registration: SectionChallengeMethodPythonSingleThreadedRegistration,
        data_set: SectionDataSetPyST,
) -> SectionRunResult | tuple[Literal["infeasible"], str]:
    startedTime = time.time()
    df_concrete_students: pd.DataFrame
    match challenge_method_registration.delegate(
        data_set=data_set,
        exec_params=args.exec_params,
    ):
        case "infeasible", reason:
            return "infeasible", reason
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
            result_file_path=SectionPythonSTRunResultFileWriter.RUN_LOG_FILE_PATH,
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
                for x in SECTIONAL_STRATEGY_REGISTRY_PYTHON_SINGLE_THREADED
            ]
        ),
    )


def main() -> None:
    logger.info(f"Running {__file__}")
    args = parse_args()
    update_challenge_registration()
    do_test_runs(args)
    logger.info("Done!")


if __name__ == "__main__":
    setup_logging()
    try:
        main()
    except KeyboardInterrupt:
        logger.warning("Interrupted!")
