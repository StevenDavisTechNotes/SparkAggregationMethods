#! python
# usage: python -m src.challenges.sectional.section_pyspark_runner
# cSpell: ignore wasb, sparkperftesting, Reqs
import argparse
import gc
import os
import random
import time
from dataclasses import dataclass
from typing import Iterable, TextIO

from pyspark import RDD
from pyspark.sql import DataFrame as PySparkDataFrame

from src.challenges.sectional.section_generate_test_data import (
    DATA_SIZE_LIST_SECTIONAL, populate_data_sets)
from src.challenges.sectional.section_record_runs import (
    MAXIMUM_PROCESSABLE_SEGMENT, derive_run_log_file_path_for_recording,
    write_header, write_run_result)
from src.challenges.sectional.section_strategy_directory import \
    STRATEGIES_USING_PYSPARK_REGISTRY
from src.challenges.sectional.section_test_data_types import (
    ChallengeMethodPysparkRegistration, DataSetWithAnswer, ExecutionParameters,
    RunResult, StudentSummary)
from src.challenges.sectional.using_pyspark.section_nospark_single_threaded import \
    section_nospark_logic
from src.perf_test_common import CalcEngine, count_iter
from src.utils.tidy_spark_session import LOCAL_NUM_EXECUTORS, TidySparkSession
from src.utils.utils import always_true, set_random_seed

ENGINE = CalcEngine.PYSPARK
DEBUG_ARGS = None if False else (
    []
    + '--size 1'.split()
    # + ['--no-check']
    + '--runs 1'.split()
    # + '--random-seed 1234'.split()
    + ['--no-shuffle']
    # + ['--strategy',
    #    ]
)


LOCAL_TEST_DATA_FILE_LOCATION = "d:/temp/SparkPerfTesting"
REMOTE_TEST_DATA_LOCATION = "wasb:///sparkperftesting"


@dataclass(frozen=True)
class Arguments:
    check_answers: bool
    make_new_data_files: bool
    num_runs: int
    random_seed: int | None
    shuffle: bool
    sizes: list[str]
    strategy_names: list[str]
    exec_params: ExecutionParameters


def parse_args() -> Arguments:
    sizes = [x.size_code for x in DATA_SIZE_LIST_SECTIONAL]
    strategy_names = [x.strategy_name for x in STRATEGIES_USING_PYSPARK_REGISTRY]

    parser = argparse.ArgumentParser()
    parser.add_argument('--check', default=True, action=argparse.BooleanOptionalAction,
                        help="When debugging skipping this will speed up the startup")
    parser.add_argument('--random-seed', type=int)
    parser.add_argument('--runs', type=int, default=30)
    parser.add_argument(
        '--new-files',
        default=False,
        action=argparse.BooleanOptionalAction)
    parser.add_argument(
        '--size',
        choices=sizes,
        default=sizes,
        nargs="+")
    parser.add_argument(
        '--shuffle', default=True,
        action=argparse.BooleanOptionalAction)
    parser.add_argument(
        '--strategy',
        choices=strategy_names,
        default=strategy_names,
        nargs="+")
    if DEBUG_ARGS is None:
        args = parser.parse_args()
    else:
        args = parser.parse_args(DEBUG_ARGS)
    return Arguments(
        check_answers=args.check,
        make_new_data_files=args.new_files,
        num_runs=args.runs,
        random_seed=args.random_seed,
        shuffle=args.shuffle,
        sizes=args.size,
        strategy_names=args.strategy,
        exec_params=ExecutionParameters(
            default_parallelism=2 * LOCAL_NUM_EXECUTORS,
            test_data_folder_location=LOCAL_TEST_DATA_FILE_LOCATION,
            maximum_processable_segment=MAXIMUM_PROCESSABLE_SEGMENT,
        )
    )


def do_test_runs(
        args: Arguments,
        spark_session: TidySparkSession,
) -> None:
    data_sets_wo_answers = populate_data_sets(
        args.exec_params,
        args.make_new_data_files)
    data_sets_wo_answers = [x for x in data_sets_wo_answers if x.data_size.size_code in args.sizes]
    if args.check_answers is True:
        data_sets_w_answers = [
            DataSetWithAnswer(
                data_size=data_set.data_size,
                data=data_set.data,
                exec_params=args.exec_params,
                answer_generator=lambda: section_nospark_logic(data_set),
            ) for data_set in data_sets_wo_answers
        ]
    else:
        data_sets_w_answers = [
            DataSetWithAnswer(
                data_size=data_set.data_size,
                data=data_set.data,
                exec_params=args.exec_params,
                answer_generator=None,
            ) for data_set in data_sets_wo_answers
        ]
    keyed_data_sets = {
        str(x.data_size.num_students): x
        for x in data_sets_w_answers}
    keyed_implementation_list = {
        x.strategy_name: x for x in STRATEGIES_USING_PYSPARK_REGISTRY}
    itinerary: list[tuple[ChallengeMethodPysparkRegistration, DataSetWithAnswer]] = [
        (challenge_method_registration, data_set)
        for strategy in args.strategy_names
        if always_true(challenge_method_registration := keyed_implementation_list[strategy])
        for data_set in keyed_data_sets.values()
        for _ in range(0, args.num_runs)
    ]
    if args.random_seed is not None:
        set_random_seed(args.random_seed)
    if args.shuffle:
        random.shuffle(itinerary)

    with open(derive_run_log_file_path_for_recording(ENGINE), 'at+') as file:
        write_header(file)
        for index, (challenge_method_registration, data_set) in enumerate(itinerary):
            spark_session.log.info(
                "Working on %d of %d" %
                (index, len(itinerary)))
            print(f"Working on {challenge_method_registration.strategy_name} for {data_set.data_size.num_rows}")
            run_one_itinerary_step(args, spark_session, challenge_method_registration, file, data_set)
            gc.collect()
            time.sleep(0.1)


def run_one_itinerary_step(
        args: Arguments,
        spark_session: TidySparkSession,
        challenge_method_registration: ChallengeMethodPysparkRegistration,
        file: TextIO,
        data_set: DataSetWithAnswer,
):
    startedTime = time.time()
    num_students_found: int
    rdd: RDD | None = None
    found_students_iterable: Iterable[StudentSummary]
    match challenge_method_registration.delegate(
        spark_session=spark_session,
        data_set=data_set,
    ):
        case PySparkDataFrame() as df:
            print(f"output rdd has {df.rdd.getNumPartitions()} partitions")
            rdd = df.rdd
            found_students_iterable = [StudentSummary(*x) for x in rdd.toLocalIterator()]
        case RDD() as rdd:
            print(f"output rdd has {rdd.getNumPartitions()} partitions")
            count = rdd.count()
            print("Got a count", count)
            found_students_iterable = rdd.toLocalIterator()
        case "infeasible":
            return "infeasible"
        case list() as iter_tuple:
            found_students_iterable = iter_tuple
        case _:
            raise ValueError("Unexpected return type")
    concrete_students: list[StudentSummary]
    if args.check_answers:
        concrete_students = list(found_students_iterable)
        num_students_found = len(concrete_students)
    else:
        concrete_students = []
        num_students_found = count_iter(found_students_iterable)
    finishedTime = time.time()
    if rdd is not None and rdd.getNumPartitions() > max(
        args.exec_params.default_parallelism,
        data_set.data.target_num_partitions,
    ):
        print(f"{challenge_method_registration.strategy_name} output rdd has {rdd.getNumPartitions()} partitions")
        findings = rdd.collect()
        print(f"size={len(findings)}!", findings)
        exit(1)
    success = verify_correctness(data_set, concrete_students, num_students_found, args.check_answers)
    result = RunResult(
        strategy_name=challenge_method_registration.strategy_name,
        engine=ENGINE,
        success=success,
        data=data_set,
        elapsed_time=finishedTime - startedTime,
        record_count=num_students_found)
    write_run_result(challenge_method_registration, result, file)
    print("%s Took %f secs" % (challenge_method_registration.strategy_name, finishedTime - startedTime))


def verify_correctness(
    data_set: DataSetWithAnswer,
    found_students: list[StudentSummary] | None,
    num_students_found: int,
    check_answers: bool,
) -> bool:
    success = True
    if check_answers is False:
        return num_students_found == data_set.data_size.num_students
    assert data_set.answer_generator is not None
    assert found_students is not None
    correct_answer: list[StudentSummary] = list(data_set.answer_generator())
    actual_num_students = data_set.data_size.num_rows // data_set.data.section_maximum
    assert actual_num_students == len(correct_answer)
    if num_students_found != data_set.data_size.num_students:
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
    with TidySparkSession(
        spark_configs(args.exec_params.default_parallelism),
        enable_hive_support=False,
    ) as spark_session:
        os.chdir(spark_session.python_src_code_path)
        do_test_runs(args, spark_session)


if __name__ == "__main__":
    main()
    print("Done!")
