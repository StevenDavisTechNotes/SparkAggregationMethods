from typing import Iterable, List, Optional, Tuple, Union, cast

from pyspark import RDD, StorageLevel
from pyspark.sql import DataFrame as spark_DataFrame

from SectionPerfTest.SectionLogic import rddTypedWithIndexFactory
from SectionPerfTest.SectionSnippetSubtotal2 import (
    FIRST_LAST_FIRST, FIRST_LAST_LAST, CompletedStudent2, StudentSnippet2, completeSnippets2, completedFromSnippet2, gradeSummary2, margeSnippets2, studentSnippetFromTypedRow2)
from SectionPerfTest.SectionTypeDefs import (
    DataSet, LabeledTypedRow, StudentSummary)
from Utils.SparkUtils import TidySparkSession
from Utils.Utils import int_divide_round_up


def section_mappart_partials_2(
    spark_session: TidySparkSession, data_set: DataSet
) -> Tuple[List[StudentSummary] | None, RDD | None, spark_DataFrame | None]:
    sc = spark_session.spark_context
    initial_num_rows = data_set.description.num_rows
    filename = data_set.data.test_filepath
    default_parallelism = data_set.exec_params.DefaultParallelism
    maximum_processable_segment = data_set.exec_params.MaximumProcessableSegment

    targetNumPartitions = int_divide_round_up(initial_num_rows, maximum_processable_segment)

    rdd_orig: RDD[LabeledTypedRow] = rddTypedWithIndexFactory(spark_session, filename, targetNumPartitions)
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

    rdd_accumulative_completed: RDD[CompletedStudent2] = sc.parallelize([])
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

        rdd6: RDD[Tuple[bool, Union[CompletedStudent2, StudentSnippet2]]] \
            = rdd5.mapPartitions(consolidateSnippetsInPartition)
        rdd6.persist(StorageLevel.DISK_ONLY)

        rdd_completed: RDD[CompletedStudent2] \
            = rdd6.filter(lambda x: x[0]).map(lambda x: cast(CompletedStudent2, x[1]))

        rdd_accumulative_completed = rdd_accumulative_completed.union(rdd_completed)
        rdd_accumulative_completed.localCheckpoint()

        complete_count = rdd_accumulative_completed.count()
        print(f"Completed {complete_count} of {initial_num_rows}")

        rdd7: RDD[StudentSnippet2] \
            = rdd6.filter(lambda x: not x[0]).map(lambda x: cast(StudentSnippet2, x[1]))

        rdd8: RDD[StudentSnippet2] = rdd7.sortBy(lambda x: x.FirstLineIndex)  # type: ignore
        rdd8.persist(StorageLevel.DISK_ONLY)

        NumRowsLeftToProcess = rdd8.count()
        if passNumber == 20:
            print("Failed to complete")
            print(rdd8.collect())
            raise Exception("Failed to complete")
        if NumRowsLeftToProcess > 1:
            rdd_loop = rdd8
            continue
        else:
            rdd_final: RDD[CompletedStudent2] \
                = rdd8.map(completedFromSnippet2)
            rdd_accumulative_completed \
                = rdd_accumulative_completed.union(rdd_final)
            break
    rdd_answer: RDD[StudentSummary] = (
        rdd_accumulative_completed
        .map(gradeSummary2)
        .repartition(default_parallelism))
    return None, rdd_answer, None


def consolidateSnippetsInPartition(
        iter: Iterable[Tuple[int, bool, int, StudentSnippet2]]
) -> Iterable[Tuple[bool, Union[CompletedStudent2, StudentSnippet2]]]:
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
