from typing import List, TextIO, Tuple, Optional

import argparse
from dataclasses import dataclass
import gc
import random
import time

from SectionPerfTest.SectionTypeDefs import (
    DataSetAnswer, DataSetWithAnswer, ExecutionParameters, PythonTestMethod, RunResult, StudentSummary)
from SectionPerfTest.Strategy.SectionNoSparkST import section_nospark_logic
from Utils.SparkUtils import LOCAL_NUM_EXECUTORS, TidySparkSession
from Utils.Utils import always_true

from SectionPerfTest.SectionDirectory import implementation_list, strategy_name_list
from SectionPerfTest.SectionRunResult import MAXIMUM_PROCESSABLE_SEGMENT, PYTHON_RESULT_FILE_PATH, infeasible, write_header, write_run_result
from SectionPerfTest.SectionTestData import populate_data_sets, available_data_sizes

DEBUG_ARGS = None if False else (
    []
    + '--size 1'.split()
    + '--runs 1'.split()
    # + '--random-seed 1234'.split()
    + ['--no-shuffle']
    +'--strategy section_mappart_partials'.split()
)


LOCAL_TEST_DATA_FILE_LOCATION = "d:/temp/SparkPerfTesting"
REMOTE_TEST_DATA_LOCATION = "wasb:///sparkperftesting"


@dataclass(frozen=True)
class Arguments:
    make_new_data_files: bool
    num_runs: int
    random_seed: Optional[int]
    shuffle: bool
    sizes: List[str]
    strategies: List[str]
    exec_params: ExecutionParameters


def parse_args() -> Arguments:
    parser = argparse.ArgumentParser()
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


def do_test_runs(args: Arguments, spark_session: TidySparkSession):
    data_sets_wo_answers = populate_data_sets(
        args.exec_params,
        args.make_new_data_files)
    data_sets_wo_answers = [x for x in data_sets_wo_answers if x.description.size_code in args.sizes]
    data_sets_w_answers = [
        DataSetWithAnswer(
            description=data_set.description,
            data=data_set.data,
            exec_params=args.exec_params,
            answer=DataSetAnswer(
                correct_answer=section_nospark_logic(data_set),
            )
        ) for data_set in data_sets_wo_answers
    ]
    keyed_data_sets = {str(x.description.num_students): x for x in data_sets_w_answers}
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
        random.seed(args.random_seed)
    if args.shuffle:
        random.shuffle(itinerary)

    with open(PYTHON_RESULT_FILE_PATH, 'at+') as file:
        write_header(file)
        for index, (test_method, data_set) in enumerate(itinerary):
            spark_session.log.info(
                "Working on %d of %d" %
                (index, len(itinerary)))
            print(f"Working on {test_method.strategy_name} for {data_set.description.num_rows}")
            run_one_itinerary_step(spark_session, args.exec_params, test_method, file, data_set)
            gc.collect()
            time.sleep(0.1)


def run_one_itinerary_step(
        spark_session: TidySparkSession,
        exec_params: ExecutionParameters,
        test_method: PythonTestMethod,
        file: TextIO,
        data_set: DataSetWithAnswer,
):
    startedTime = time.time()
    lst, rdd, df = test_method.delegate(spark_session, data_set)
    foundStudents: List[StudentSummary]
    if lst is not None:
        foundStudents = lst
    elif rdd is not None:
        print(f"output rdd has {rdd.getNumPartitions()} partitions")
        foundStudents = [StudentSummary(*x) for x in rdd.toLocalIterator()]
    elif df is not None:
        print(f"output rdd has {df.rdd.getNumPartitions()} partitions")
        foundStudents = [StudentSummary(*x) for x in df.rdd.toLocalIterator()]
        rdd = df.rdd
    else:
        raise ValueError("Not data returned")
    finishedTime = time.time()
    if rdd is not None and rdd.getNumPartitions() > max(
        exec_params.DefaultParallelism,
        data_set.data.target_num_partitions,
    ):
        print(f"{test_method.strategy_name} output rdd has {rdd.getNumPartitions()} partitions")
        findings = rdd.collect()
        print(f"size={len(findings)}!", findings)
        exit(1)
    success = verify_correctness(data_set, foundStudents)
    result = RunResult(
        success=success,
        data=data_set,
        elapsed_time=finishedTime - startedTime,
        record_count=len(foundStudents))
    write_run_result(test_method, result, file)
    print("%s Took %f secs" % (test_method.strategy_name, finishedTime - startedTime))


def verify_correctness(
    data_set: DataSetWithAnswer,
    foundStudents: List[StudentSummary],
) -> bool:
    actualNumStudents = data_set.description.num_rows // data_set.data.section_maximum
    correct_answer = data_set.answer.correct_answer
    assert actualNumStudents == len(correct_answer)
    success = True
    if len(foundStudents) != actualNumStudents:
        success = False
    elif {x.StudentId for x in foundStudents} != {x.StudentId for x in correct_answer}:
        success = False
        raise ValueError("Found student ids don't match")
    else:
        map_of_found_students = {x.StudentId: x for x in foundStudents}
        for correct_student in correct_answer:
            found_student = map_of_found_students[correct_student.StudentId]
            if found_student != correct_student:
                success = False
                raise ValueError("Found student data doesn't match")
    return success


def main():
    args = parse_args()
    config = {
        "spark.sql.shuffle.partitions": args.exec_params.DefaultParallelism,
        "spark.default.parallelism": args.exec_params.DefaultParallelism,
        "spark.worker.cleanup.enabled": "true",
        "spark.driver.memory": "2g",
        "spark.executor.memory": "3g",
        "spark.executor.memoryOverhead": "1g",
        "spark.port.maxRetries": "1",
        "spark.rpc.retry.wait": "10s",
        "spark.reducer.maxReqsInFlight": "1",
        "spark.executor.heartbeatInterval": "10s",
        "spark.network.timeout": "120s",
        # "spark.storage.blockManagerHeartbeatTimeoutMs": "300s",
        "spark.shuffle.io.maxRetries": "10",
        "spark.shuffle.io.retryWait": "60s",
        "spark.sql.execution.arrow.pyspark.enabled": "true",
    }
    with TidySparkSession(
        config,
        enable_hive_support=False
    ) as spark_session:
        do_test_runs(args, spark_session)


if __name__ == "__main__":
    main()
