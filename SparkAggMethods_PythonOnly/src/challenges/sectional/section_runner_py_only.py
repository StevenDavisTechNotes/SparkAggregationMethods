#! python
# usage: python -m src.challenges.sectional.section_pyspark_runner
# cSpell: ignore wasb, sparkperftesting, Reqs
import argparse
import datetime as dt
import gc
import logging
import os
import random
import time
from dataclasses import dataclass
from typing import Literal

from spark_agg_methods_common_python.challenge_strategy_registry import (
    ChallengeResultLogFileRegistration, ChallengeStrategyRegistration, update_challenge_strategy_registration,
)
from spark_agg_methods_common_python.challenges.sectional.section_persist_test_data import AnswerFileSectional
from spark_agg_methods_common_python.challenges.sectional.section_record_runs import SectionRunResult
from spark_agg_methods_common_python.challenges.sectional.section_test_data_types import (
    DATA_SIZE_LIST_SECTIONAL, LARGEST_EXPONENT_SECTIONAL, SECTION_SIZE_MAXIMUM, SectionDataSetDescription,
    StudentSummary, derive_source_test_data_file_path,
)
from spark_agg_methods_common_python.perf_test_common import (
    ELAPSED_TIME_COLUMN_NAME, LOCAL_NUM_EXECUTORS, CalcEngine, Challenge, NumericalToleranceExpectations,
    SolutionLanguage,
)
from spark_agg_methods_common_python.utils.utils import always_true, count_iter, set_random_seed

from src.challenges.sectional.section_record_runs_py_only import (
    SectionPythonOnlyPersistedRunResultLog, SectionPythonOnlyRunResultFileWriter,
)
from src.challenges.sectional.section_strategy_directory_py_only import SECTIONAL_STRATEGIES_USING_PYTHON_ONLY_REGISTRY
from src.challenges.sectional.section_test_data_types_py_only import (
    SectionChallengeMethodPythonOnlyRegistration, SectionDataSetPyOnly, SectionExecutionParametersPyOnly,
)

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
class Arguments:
    check_answers: bool
    num_runs: int
    random_seed: int | None
    shuffle: bool
    sizes: list[str]
    strategy_names: list[str]
    exec_params: SectionExecutionParametersPyOnly


def parse_args() -> Arguments:
    sizes = [x.size_code for x in DATA_SIZE_LIST_SECTIONAL]
    strategy_names = [x.strategy_name for x in SECTIONAL_STRATEGIES_USING_PYTHON_ONLY_REGISTRY]

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


def sectional_populate_data_sets_py_only(
        exec_params: SectionExecutionParametersPyOnly,
) -> list[SectionDataSetPyOnly]:

    datasets: list[SectionDataSetPyOnly] = []
    num_students = 1
    for i_scale in range(0, LARGEST_EXPONENT_SECTIONAL + 1):
        num_students = 10**i_scale
        file_path = derive_source_test_data_file_path(num_students)
        # data_size = num_students * SECTION_SIZE_MAXIMUM
        assert os.path.exists(file_path)
        data_description = DATA_SIZE_LIST_SECTIONAL[i_scale]
        correct_answer = AnswerFileSectional.read_answer_file_sectional(data_description)
        datasets.append(
            SectionDataSetPyOnly(
                data_description=data_description,
                # exec_params=SectionExecutionParametersPyOnly(
                #     default_parallelism=exec_params.default_parallelism,
                #     maximum_processable_segment=exec_params.maximum_processable_segment,
                #     section_maximum=SECTION_SIZE_MAXIMUM,
                #     source_data_file_path=file_path,
                #     target_num_partitions=src_num_partitions,
                # ),
                correct_answer=correct_answer,
                section_maximum=SECTION_SIZE_MAXIMUM,
            ))
    return datasets


def do_test_runs(
        args: Arguments,
) -> None:
    data_sets_w_answers = [
        x for x in sectional_populate_data_sets_py_only(
            args.exec_params,
        )
        if x.data_description.size_code in args.sizes
    ]
    keyed_data_sets = {
        str(x.data_description.num_students): x
        for x in data_sets_w_answers}
    keyed_implementation_list = {
        x.strategy_name: x for x in SECTIONAL_STRATEGIES_USING_PYTHON_ONLY_REGISTRY}
    itinerary: list[tuple[SectionChallengeMethodPythonOnlyRegistration, SectionDataSetPyOnly]] = [
        (challenge_method_registration, data_set)
        for strategy_name in args.strategy_names
        if always_true(challenge_method_registration := keyed_implementation_list[strategy_name])
        for data_set in keyed_data_sets.values()
        for _ in range(0, args.num_runs)
    ]
    if len(itinerary) == 0:
        print("No runs to execute.")
        return
    if args.random_seed is not None:
        set_random_seed(args.random_seed)
    if args.shuffle:
        random.shuffle(itinerary)

    with SectionPythonOnlyRunResultFileWriter() as file:
        for index, (challenge_method_registration, data_set) in enumerate(itinerary):
            logger.info(
                "Working on %d of %d" %
                (index, len(itinerary)))
            print(f"Working on {challenge_method_registration.strategy_name} "
                  f"for {data_set.data_description.num_source_rows}")
            run_result = run_one_itinerary_step(args, challenge_method_registration, data_set)
            match run_result:
                case "infeasible":
                    pass
                case _:
                    if not data_set.data_description.debugging_only:
                        file.write_run_result(challenge_method_registration, run_result)
            gc.collect()
            time.sleep(0.1)


def run_one_itinerary_step(
        args: Arguments,
        challenge_method_registration: SectionChallengeMethodPythonOnlyRegistration,
        data_set: SectionDataSetPyOnly,
) -> SectionRunResult | Literal["infeasible"]:
    startedTime = time.time()
    num_students_found: int
    found_students: list[StudentSummary]
    match challenge_method_registration.delegate(
        data_set=data_set,
        exec_params=args.exec_params,
    ):
        case "infeasible":
            return "infeasible"
        case list() as iter_tuple:
            found_students = iter_tuple
        case _:
            raise ValueError("Unexpected return type")
    concrete_students: list[StudentSummary]
    if args.check_answers:
        concrete_students = found_students
        num_students_found = len(concrete_students)
    else:
        concrete_students = []
        num_students_found = count_iter(found_students)
    finishedTime = time.time()
    success = verify_correctness(data_set, concrete_students, num_students_found, args.check_answers)
    status = "success" if success else "failure"
    data_description = data_set.data_description
    result = SectionRunResult(
        status=status,
        num_source_rows=data_description.num_source_rows,
        section_maximum=data_set.section_maximum,
        elapsed_time=finishedTime - startedTime,
        num_output_rows=num_students_found,
        finished_at=dt.datetime.now().isoformat(),
    )
    print("%s Took %f secs" % (challenge_method_registration.strategy_name, finishedTime - startedTime))
    return result


def verify_correctness(
    data_set: SectionDataSetPyOnly,
    found_students: list[StudentSummary] | None,
    num_students_found: int,
    check_answers: bool,
) -> bool:
    success = True
    if check_answers is False:
        return num_students_found == data_set.data_description.num_students
    assert found_students is not None
    correct_answer = AnswerFileSectional.read_answer_file_sectional(data_set.data_description)
    actual_num_students = data_set.data_description.num_source_rows // data_set.section_maximum
    assert actual_num_students == len(correct_answer)
    if num_students_found != data_set.data_description.num_students:
        success = False
        raise ValueError("Found student ids don't match")
    if {x.StudentId for x in found_students} != {x.StudentId for x in correct_answer}:
        success = False
        raise ValueError("Found student ids don't match")
    else:
        map_of_found_students = {x.StudentId: x for x in found_students}
        for correct_student in correct_answer:
            found_student = map_of_found_students[correct_student.StudentId]
            if found_student != correct_student:
                success = False
                raise ValueError("Found student data doesn't match")
    return success


def update_challenge_registration():
    update_challenge_strategy_registration(
        language=LANGUAGE,
        engine=ENGINE,
        challenge=CHALLENGE,
        registration=ChallengeResultLogFileRegistration(
            result_file_path=SectionPythonOnlyPersistedRunResultLog().log_file_path,
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
                for x in SECTIONAL_STRATEGIES_USING_PYTHON_ONLY_REGISTRY
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
    args = parse_args()
    update_challenge_registration()
    do_test_runs(args)


if __name__ == "__main__":
    print(f"Running {__file__}")
    main()
    print("Done!")
