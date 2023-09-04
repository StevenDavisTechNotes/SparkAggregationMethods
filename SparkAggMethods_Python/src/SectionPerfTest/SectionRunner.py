import argparse
import gc
import os
import random
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, TextIO, Tuple

from PerfTestCommon import count_iter
from SectionPerfTest.SectionDirectory import (
    implementation_list, strategy_name_list)
from SectionPerfTest.SectionRunResult import (
    MAXIMUM_PROCESSABLE_SEGMENT, PYTHON_RESULT_FILE_PATH, infeasible, write_header, write_run_result)
from SectionPerfTest.SectionTestData import (
    available_data_sizes, populate_data_sets)
from SectionPerfTest.SectionTypeDefs import (
    DataSetWithAnswer, ExecutionParameters, PythonTestMethod, RunResult, StudentSummary)
from SectionPerfTest.Strategy.SectionNoSparkST import section_nospark_logic
from Utils.TidySparkSession import LOCAL_NUM_EXECUTORS, TidySparkSession
from Utils.Utils import always_true, set_random_seed

DEBUG_ARGS = None if False else (
    []
    + '--size 1000'.split()
    # + ['--no-check']
    + '--runs 1'.split()
    # + '--random-seed 1234'.split()
    + ['--no-shuffle']
    # + '--strategy section_mappart_partials'.split()
)


LOCAL_TEST_DATA_FILE_LOCATION = "d:/temp/SparkPerfTesting"
REMOTE_TEST_DATA_LOCATION = "wasb:///sparkperftesting"


@dataclass(frozen=True)
class Arguments:
    check_answers: bool
    make_new_data_files: bool
    num_runs: int
    random_seed: Optional[int]
    shuffle: bool
    sizes: List[str]
    strategies: List[str]
    exec_params: ExecutionParameters


def parse_args() -> Arguments:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--check', default=True,
        action=argparse.BooleanOptionalAction)
    parser.add_argument('--random-seed', type=int)
    parser.add_argument('--runs', type=int, default=30)
    parser.add_argument(
        '--new-files',
        default=False,
        action=argparse.BooleanOptionalAction)
    parser.add_argument(
        '--size',
        choices=available_data_sizes,
        default=available_data_sizes,
        nargs="+")
    parser.add_argument(
        '--shuffle', default=True,
        action=argparse.BooleanOptionalAction)
    parser.add_argument(
        '--strategy',
        choices=strategy_name_list,
        default=strategy_name_list,
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
        strategies=args.strategy,
        exec_params=ExecutionParameters(
            DefaultParallelism=2 * LOCAL_NUM_EXECUTORS,
            TestDataFolderLocation=LOCAL_TEST_DATA_FILE_LOCATION,
            MaximumProcessableSegment=MAXIMUM_PROCESSABLE_SEGMENT,
        )
    )


def do_test_runs(
        args: Arguments,
        spark_session: TidySparkSession,
) -> None:
    data_sets_wo_answers = populate_data_sets(
        args.exec_params,
        args.make_new_data_files)
    data_sets_wo_answers = [x for x in data_sets_wo_answers if x.description.size_code in args.sizes]
    if args.check_answers is True:
        data_sets_w_answers = [
            DataSetWithAnswer(
                description=data_set.description,
                data=data_set.data,
                exec_params=args.exec_params,
                answer_generator=lambda: section_nospark_logic(data_set),
            ) for data_set in data_sets_wo_answers
        ]
    else:
        data_sets_w_answers = [
            DataSetWithAnswer(
                description=data_set.description,
                data=data_set.data,
                exec_params=args.exec_params,
                answer_generator=None,
            ) for data_set in data_sets_wo_answers
        ]
    keyed_data_sets = {
        str(x.description.num_students): x
        for x in data_sets_w_answers}
    keyed_implementation_list = {
        x.strategy_name: x for x in implementation_list}
    itinerary: List[Tuple[PythonTestMethod, DataSetWithAnswer]] = [
        (test_method, data_set)
        for strategy in args.strategies
        if always_true(test_method := keyed_implementation_list[strategy])
        for data_set in keyed_data_sets.values()
        if not infeasible(strategy, data_set.description)
        for _ in range(0, args.num_runs)
    ]
    if args.random_seed is not None:
        set_random_seed(args.random_seed)
    if args.shuffle:
        random.shuffle(itinerary)

    result_log_path_name = os.path.join(
        spark_session.python_code_root_path,
        PYTHON_RESULT_FILE_PATH)
    with open(result_log_path_name, 'at+') as file:
        write_header(file)
        for index, (test_method, data_set) in enumerate(itinerary):
            spark_session.log.info(
                "Working on %d of %d" %
                (index, len(itinerary)))
            print(f"Working on {test_method.strategy_name} for {data_set.description.num_rows}")
            run_one_itinerary_step(args, spark_session, test_method, file, data_set)
            gc.collect()
            time.sleep(0.1)


def run_one_itinerary_step(
        args: Arguments,
        spark_session: TidySparkSession,
        test_method: PythonTestMethod,
        file: TextIO,
        data_set: DataSetWithAnswer,
):
    startedTime = time.time()
    found_students_iterable, rdd, df = test_method.delegate(spark_session, data_set)
    num_students_found: int
    if found_students_iterable is not None:
        pass
    elif rdd is not None:
        print(f"output rdd has {rdd.getNumPartitions()} partitions")
        count = rdd.count()
        print(f"Got a count", count)
        found_students_iterable = rdd.toLocalIterator()
    elif df is not None:
        print(f"output rdd has {df.rdd.getNumPartitions()} partitions")
        rdd = df.rdd
        found_students_iterable = [StudentSummary(*x) for x in rdd.toLocalIterator()]
    else:
        raise ValueError("Not data returned")
    concrete_students: Optional[List[StudentSummary]]
    if args.check_answers:
        concrete_students = list(found_students_iterable)
        num_students_found = len(concrete_students)
    else:
        concrete_students = None
        num_students_found = count_iter(found_students_iterable)
    finishedTime = time.time()
    if rdd is not None and rdd.getNumPartitions() > max(
        args.exec_params.DefaultParallelism,
        data_set.data.target_num_partitions,
    ):
        print(f"{test_method.strategy_name} output rdd has {rdd.getNumPartitions()} partitions")
        findings = rdd.collect()
        print(f"size={len(findings)}!", findings)
        exit(1)
    success = verify_correctness(data_set, concrete_students, num_students_found, args.check_answers)
    result = RunResult(
        success=success,
        data=data_set,
        elapsed_time=finishedTime - startedTime,
        record_count=num_students_found)
    write_run_result(test_method, result, file)
    print("%s Took %f secs" % (test_method.strategy_name, finishedTime - startedTime))


def verify_correctness(
    data_set: DataSetWithAnswer,
    found_students: Optional[List[StudentSummary]],
    num_students_found: int,
    check_answers: bool,
) -> bool:
    success = True
    if check_answers is False:
        return num_students_found == data_set.description.num_students
    assert data_set.answer_generator is not None
    assert found_students is not None
    correct_answer: List[StudentSummary] = list(data_set.answer_generator())
    actual_num_students = data_set.description.num_rows // data_set.data.section_maximum
    assert actual_num_students == len(correct_answer)
    if num_students_found != data_set.description.num_students:
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
) -> Dict[str, str | int]:
    return {
        "spark.sql.shuffle.partitions": default_parallelism,
        "spark.default.parallelism": default_parallelism,
        "spark.worker.cleanup.enabled": "true",
        "spark.driver.memory": "2g",
        "spark.executor.memory": "3g",
        "spark.executor.memoryOverhead": "1g",
        "spark.port.maxRetries": "1",
        "spark.rpc.retry.wait": "120s",
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
        spark_configs(args.exec_params.DefaultParallelism),
        enable_hive_support=False
    ) as spark_session:
        os.chdir(spark_session.python_src_code_path)
        do_test_runs(args, spark_session)


if __name__ == "__main__":
    main()
