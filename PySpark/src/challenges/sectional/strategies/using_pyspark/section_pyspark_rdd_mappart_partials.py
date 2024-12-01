from typing import Callable, Iterable, NamedTuple, cast

from pyspark import RDD, SparkContext, StorageLevel
from spark_agg_methods_common_python.challenges.sectional.section_test_data_types import LabeledTypedRow, StudentSummary
from spark_agg_methods_common_python.utils.utils import int_divide_round_up

from src.challenges.sectional.domain_logic.section_data_parsers_pyspark import rdd_typed_with_index_factory
from src.challenges.sectional.domain_logic.section_snippet_subtotal_type import (
    FIRST_LAST_FIRST, FIRST_LAST_LAST, FIRST_LAST_NEITHER, CompletedStudent, StudentSnippet2, complete_snippets_2,
    grade_summary, marge_snippets_2, student_snippet_from_typed_row_2,
)
from src.challenges.sectional.section_test_data_types_pyspark import (
    SectionDataSetPyspark, SectionExecutionParametersPyspark, TChallengePythonPysparkAnswer,
)
from src.utils.tidy_session_pyspark import TidySparkSession


class StudentSnippetWIndex(NamedTuple):
    Index: int
    Value: StudentSnippet2


def section_pyspark_rdd_mappart_partials(
        spark_session: TidySparkSession,
        exec_params: SectionExecutionParametersPyspark,
        data_set: SectionDataSetPyspark,
) -> TChallengePythonPysparkAnswer:
    logger = spark_session.logger
    if data_set.data_description.num_students > pow(10, 5-1):
        return "infeasible", "Unreliable in local mode"
    sc = spark_session.spark_context
    expected_row_count = data_set.data_description.num_source_rows
    filename = data_set.source_data_file_path
    default_parallelism = exec_params.default_parallelism
    maximum_processable_segment = exec_params.maximum_processable_segment
    targetNumPartitions = int_divide_round_up(expected_row_count + 2, maximum_processable_segment)
    rdd_orig: RDD[LabeledTypedRow] = rdd_typed_with_index_factory(spark_session, filename, targetNumPartitions)

    def report_num_completed(complete_count: int) -> None:
        logger.info(f"Completed {complete_count} of {data_set.data_description.num_students}")

    rdd_answer = section_mappart_partials_logic(
        sc=sc,
        rdd_orig=rdd_orig,
        default_parallelism=default_parallelism,
        maximum_processable_segment=maximum_processable_segment,
        report_num_completed=report_num_completed,
    )
    return rdd_answer


def section_mappart_partials_logic(
        sc: SparkContext,
        rdd_orig: RDD[LabeledTypedRow],
        default_parallelism: int,
        maximum_processable_segment: int,
        report_num_completed: Callable[[int], None] | None = None,
) -> RDD[StudentSummary]:
    num_lines_in_orig = rdd_orig.count()
    rdd_accumulative_completed: RDD[CompletedStudent] = sc.parallelize([], numSlices=1)

    rdd_start: RDD[StudentSnippet2] = (
        sc.parallelize([
            StudentSnippet2(
                FirstLastFlag=FIRST_LAST_FIRST,
                FirstLineIndex=-1,
                LastLineIndex=-1)],
            numSlices=1)
        .union(
            rdd_orig
            .map(lambda x: student_snippet_from_typed_row_2(x.Index, x.Value)))
        .union(
            sc.parallelize([
                StudentSnippet2(
                    FirstLastFlag=FIRST_LAST_LAST,
                    FirstLineIndex=num_lines_in_orig,
                    LastLineIndex=num_lines_in_orig)],
                numSlices=1))
    )
    data_to_process: tuple[RDD[StudentSnippet2], int, RDD[CompletedStudent]] \
        = (rdd_start, num_lines_in_orig + 2, rdd_accumulative_completed)

    passNumber = 0
    rdd_start.persist(StorageLevel.DISK_ONLY)
    rdd_accumulative_completed.persist(StorageLevel.DISK_ONLY)
    while True:
        passNumber += 1
        rdd_start, num_rows_start, rdd_accumulative_completed_start = data_to_process
        # rdd_start and rdd_accumulative_completed_start should be cached
        try:
            rdd_remaining, num_remaining_rows, rdd_accumulative_completed \
                = inner_iteration(
                    passNumber=passNumber,
                    data_to_process=data_to_process,
                    maximum_processable_segment=maximum_processable_segment,
                    report_num_completed=report_num_completed,
                )
            if rdd_remaining is None:
                break
            data_to_process \
                = (rdd_remaining, num_remaining_rows, rdd_accumulative_completed)
        finally:
            rdd_start.unpersist()
            rdd_accumulative_completed_start.unpersist()
            pass
        if passNumber == 20:
            raise RecursionError("Too many passes")
    rdd_answer: RDD[StudentSummary] = (
        rdd_accumulative_completed
        .map(grade_summary)
        .sortBy(
            lambda x: x.StudentId,  # pyright: ignore[reportArgumentType]
            numPartitions=min(default_parallelism, rdd_accumulative_completed.getNumPartitions()))
    )

    return rdd_answer


def inner_iteration(
        passNumber: int,
        data_to_process: tuple[RDD[StudentSnippet2], int, RDD[CompletedStudent]],
        maximum_processable_segment: int,
        report_num_completed: Callable[[int], None] | None = None,
) -> tuple[RDD[StudentSnippet2] | None, int, RDD[CompletedStudent]]:
    rdd0, num_lines_in_rdd0, rdd_accumulative_completed = data_to_process

    rdd2: RDD[StudentSnippetWIndex] = (
        rdd0
        .zipWithIndex()
        .map(lambda pair: StudentSnippetWIndex(Index=pair[1], Value=pair[0]))
    )

    def key_by_function(x: StudentSnippetWIndex) -> tuple[int, int]:
        return x.Index // maximum_processable_segment, x.Index % maximum_processable_segment
    rdd3: RDD[tuple[tuple[int, int], StudentSnippetWIndex]] \
        = rdd2.keyBy(key_by_function)

    def partition_function(key: tuple[int, int]) -> int:
        return key[0]
    target_num_partitions = int_divide_round_up(num_lines_in_rdd0, maximum_processable_segment)
    rdd4: RDD[tuple[tuple[int, int], StudentSnippetWIndex]] = (
        rdd3.repartitionAndSortWithinPartitions(
            numPartitions=target_num_partitions,
            partitionFunc=partition_function)  # type: ignore
        if rdd3.getNumPartitions() != target_num_partitions else
        rdd3.sortByKey(  # type: ignore
            numPartitions=target_num_partitions,
        )
    )

    def mark_start_of_segment(
            x: tuple[tuple[int, int], StudentSnippetWIndex]
    ) -> tuple[int, bool, int, StudentSnippet2]:
        (segment_id, offset), row = x
        return (segment_id, offset == 0, passNumber, row.Value)

    rdd5: RDD[tuple[int, bool, int, StudentSnippet2]] \
        = rdd4.map(mark_start_of_segment)

    rdd6: RDD[tuple[bool, CompletedStudent | StudentSnippet2]] \
        = rdd5.mapPartitions(consolidate_snippets_in_partition, preservesPartitioning=True)
    rdd6.persist(StorageLevel.DISK_ONLY)
    try:

        rdd_completed: RDD[CompletedStudent] \
            = rdd6.filter(lambda x: x[0]).map(lambda x: cast(CompletedStudent, x[1]))

        rdd_accumulative_completed = rdd_accumulative_completed.union(rdd_completed)
        rdd_accumulative_completed.persist(StorageLevel.DISK_ONLY)

        complete_count = rdd_completed.count()
        if report_num_completed is not None:
            report_num_completed(complete_count)

        rdd7: RDD[StudentSnippet2] \
            = rdd6.filter(lambda x: not x[0]).map(lambda x: cast(StudentSnippet2, x[1]))

        remaining_num_rows7 = rdd7.filter(lambda x: x.FirstLastFlag == FIRST_LAST_NEITHER).count()
        if remaining_num_rows7 == 0:
            return None, 0, rdd_accumulative_completed

        target_num_partitions = int_divide_round_up(remaining_num_rows7, maximum_processable_segment)
        rdd8: RDD[StudentSnippet2] = rdd7.sortBy(
            lambda x: x.FirstLineIndex,  # type: ignore
            numPartitions=target_num_partitions)
        rdd8.persist(StorageLevel.DISK_ONLY)
        remaining_num_rows8 = rdd8.count()
        return rdd8, remaining_num_rows8, rdd_accumulative_completed

    finally:
        rdd6.unpersist()


def consolidate_snippets_in_partition(
        iter: Iterable[tuple[int, bool, int, StudentSnippet2]]
) -> Iterable[tuple[bool, CompletedStudent | StudentSnippet2]]:
    front_is_clean: bool = False
    building_snippet: StudentSnippet2 | None = None

    rMember: StudentSnippet2
    for rGroupNumber, _rIsAtStartOfSegment, _passNumber, rMember in iter:
        if rMember.FirstLastFlag == FIRST_LAST_FIRST:
            yield False, rMember
            front_is_clean = True
            continue
        if building_snippet is not None:
            if (
                (rMember.FirstLastFlag == FIRST_LAST_LAST)
                or (rMember.StudentId is not None)
            ):
                completedItems, remaining_snippets = complete_snippets_2(
                    building_snippet, front_is_clean=front_is_clean, back_is_clean=True)
                for item in completedItems:
                    yield True, item
                for item in remaining_snippets:
                    yield False, item
                if rMember.FirstLastFlag == FIRST_LAST_LAST:
                    yield False, rMember
                    return
                else:
                    front_is_clean = True
                    building_snippet = None
        if building_snippet is None:
            building_snippet = rMember
        else:
            building_snippet = marge_snippets_2(building_snippet, rMember)
    if building_snippet is not None:
        yield False, building_snippet
