from typing import Iterable, List, Optional, Tuple, Union, cast, Callable

from pyspark import RDD, SparkContext, StorageLevel
from pyspark.sql import DataFrame as spark_DataFrame

from SectionPerfTest.SectionLogic import rddTypedWithIndexFactory
from SectionPerfTest.SectionSnippetSubtotal import (
    FIRST_LAST_FIRST, FIRST_LAST_LAST, FIRST_LAST_NEITHER, 
    CompletedStudent, StudentSnippet2, 
    completeSnippets2, gradeSummary, margeSnippets2, studentSnippetFromTypedRow2)
from SectionPerfTest.SectionTypeDefs import (
    DataSet, LabeledTypedRow, StudentSummary)
from Utils.TidySparkSession import TidySparkSession
from Utils.Utils import int_divide_round_up


def section_mappart_partials(
        spark_session: TidySparkSession,
        data_set: DataSet,
) -> Tuple[List[StudentSummary] | None, RDD | None, spark_DataFrame | None]:
    sc = spark_session.spark_context
    expected_row_count = data_set.description.num_rows
    filename = data_set.data.test_filepath
    default_parallelism = data_set.exec_params.DefaultParallelism
    maximum_processable_segment = data_set.exec_params.MaximumProcessableSegment
    targetNumPartitions = int_divide_round_up(expected_row_count, maximum_processable_segment)
    rdd_orig: RDD[LabeledTypedRow] = rddTypedWithIndexFactory(spark_session, filename, targetNumPartitions)

    def report_num_completed(complete_count: int) -> None:
        print(f"Completed {complete_count} of {expected_row_count}")

    rdd_answer = section_mappart_partials_logic(
        sc,
        rdd_orig,
        default_parallelism,
        maximum_processable_segment,
        report_num_completed)
    return None, rdd_answer, None


def section_mappart_partials_logic(
        sc: SparkContext,
        rdd_orig: RDD[LabeledTypedRow],
        default_parallelism: int,
        maximum_processable_segment: int = 10000,
        report_num_completed: Optional[Callable[[int], None]] = None,
) -> RDD[StudentSummary]:
    num_lines_in_orig = rdd_orig.count()

    rdd_loop: RDD[StudentSnippet2] = (
        sc.parallelize([
            StudentSnippet2(FirstLastFlag=FIRST_LAST_FIRST, FirstLineIndex=-1, LastLineIndex=-1)])
        .union(
            rdd_orig
            .map(lambda x: studentSnippetFromTypedRow2(x.Index, x.Value)))
        .union(
            sc.parallelize([
                StudentSnippet2(FirstLastFlag=FIRST_LAST_LAST,
                                FirstLineIndex=num_lines_in_orig, LastLineIndex=num_lines_in_orig)]))
    )
    rdd_loop.persist(StorageLevel.DISK_ONLY)

    rdd_accumulative_completed: RDD[CompletedStudent] = sc.parallelize([])
    passNumber = 0
    while True:
        passNumber += 1
        remaining_num_rows = rdd_loop.count()
        rdd2: RDD[LabeledTypedRow] = (
            rdd_loop
            .zipWithIndex()
            .map(lambda pair: LabeledTypedRow(Index=pair[1], Value=pair[0]))
        )

        def key_by_function(x: LabeledTypedRow) -> Tuple[int, int]:
            return x.Index // maximum_processable_segment, x.Index % maximum_processable_segment
        rdd3: RDD[Tuple[Tuple[int, int], LabeledTypedRow]] \
            = rdd2.keyBy(key_by_function)

        def partition_function(key: Tuple[int, int]) -> int:
            return key[0]
        targetNumPartitions = int_divide_round_up(remaining_num_rows, maximum_processable_segment)
        rdd4: RDD[Tuple[Tuple[int, int], LabeledTypedRow]] \
            = rdd3.repartitionAndSortWithinPartitions(
            numPartitions=targetNumPartitions,
            partitionFunc=partition_function)  # type: ignore

        rdd5: RDD[Tuple[int, bool, int, StudentSnippet2]] \
            = rdd4.map(lambda x: (x[0][0], x[0][1] == 0, passNumber, x[1].Value))

        rdd6: RDD[Tuple[bool, Union[CompletedStudent, StudentSnippet2]]] \
            = rdd5.mapPartitions(consolidateSnippetsInPartition)
        rdd6.persist(StorageLevel.DISK_ONLY)

        rdd_completed: RDD[CompletedStudent] \
            = rdd6.filter(lambda x: x[0]).map(lambda x: cast(CompletedStudent, x[1]))

        rdd_accumulative_completed = rdd_accumulative_completed.union(rdd_completed)
        rdd_accumulative_completed.localCheckpoint()

        complete_count = rdd_accumulative_completed.count()
        if report_num_completed is not None:
            report_num_completed(complete_count)

        rdd7: RDD[StudentSnippet2] \
            = rdd6.filter(lambda x: not x[0]).map(lambda x: cast(StudentSnippet2, x[1]))

        rdd8: RDD[StudentSnippet2] = rdd7.sortBy(lambda x: x.FirstLineIndex)  # type: ignore
        rdd8.persist(StorageLevel.DISK_ONLY)

        NumRowsLeftToProcess = rdd8.filter(lambda x: x.FirstLastFlag == FIRST_LAST_NEITHER).count()
        if passNumber == 20:
            raise RecursionError("Too many passes")
        if NumRowsLeftToProcess == 0:
            break
        rdd_loop = rdd8
    rdd_answer: RDD[StudentSummary] = (
        rdd_accumulative_completed
        .map(gradeSummary)
        .repartition(default_parallelism)
    )

    return rdd_answer


def consolidateSnippetsInPartition(
        iter: Iterable[Tuple[int, bool, int, StudentSnippet2]]
) -> Iterable[Tuple[bool, Union[CompletedStudent, StudentSnippet2]]]:
    front_is_clean: bool = False
    building_snippet: Optional[StudentSnippet2] = None

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
                completedItems, remaining_snippets = completeSnippets2(
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
            building_snippet = margeSnippets2(building_snippet, rMember)
    if building_snippet is not None:
        yield False, building_snippet
