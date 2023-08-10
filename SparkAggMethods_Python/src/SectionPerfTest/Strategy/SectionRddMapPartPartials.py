from typing import Iterable, List, Tuple, cast

from pyspark import RDD, StorageLevel
from pyspark.sql import DataFrame as spark_DataFrame

from SectionPerfTest.SectionLogic import rddTypedWithIndexFactory
from SectionPerfTest.SectionSnippetSubtotal import (
    CompletedStudent, StudentSnippet, StudentSnippetBuilder)
from SectionPerfTest.SectionTypeDefs import (
    DataSet, LabeledTypedRow, StudentSummary)
from Utils.SparkUtils import TidySparkSession
from Utils.Utils import int_divide_round_up


def section_mappart_partials(
    spark_session: TidySparkSession, data_set: DataSet
) -> Tuple[List[StudentSummary] | None, RDD | None, spark_DataFrame | None]:
    sc = spark_session.spark_context
    initial_num_rows = data_set.description.num_rows
    filename = data_set.data.test_filepath
    default_parallelism = data_set.exec_params.DefaultParallelism
    maximum_processable_segment = data_set.exec_params.MaximumProcessableSegment

    targetNumPartitions = int_divide_round_up(initial_num_rows, maximum_processable_segment)

    rdd_loop: RDD[StudentSnippet] = rddTypedWithIndexFactory(spark_session, filename, targetNumPartitions) \
        .map(lambda x: StudentSnippetBuilder.studentSnippetFromTypedRow(x.Index, x.Value))
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

        targetNumPartitions = int_divide_round_up(remaining_num_rows, maximum_processable_segment)

        def partition_function(key: Tuple[int, int]) -> int:
            return key[0]
        rdd4: RDD[Tuple[Tuple[int, int], LabeledTypedRow]] \
            = rdd3.repartitionAndSortWithinPartitions(
            numPartitions=targetNumPartitions,
            partitionFunc=partition_function)  # type: ignore

        rdd5: RDD[Tuple[int, bool, int, StudentSnippet]] \
            = rdd4.map(lambda x: (x[0][0], x[0][1] == 0, passNumber, x[1].Value))

        rdd6: RDD[Tuple[bool, CompletedStudent | StudentSnippet]] \
            = rdd5.mapPartitions(consolidateSnippetsInPartition)
        rdd6.persist(StorageLevel.DISK_ONLY)

        rdd_completed: RDD[CompletedStudent] \
            = rdd6.filter(lambda x: x[0]).map(lambda x: cast(CompletedStudent, x[1]))

        rdd_accumulative_completed = rdd_accumulative_completed.union(rdd_completed)
        rdd_accumulative_completed.localCheckpoint()

        complete_count = rdd_accumulative_completed.count()
        print(f"Completed {complete_count} of {initial_num_rows}")

        rdd7: RDD[StudentSnippet] \
            = rdd6.filter(lambda x: not x[0]).map(lambda x: cast(StudentSnippet, x[1]))

        rdd8: RDD[StudentSnippet] = rdd7.sortBy(lambda x: x.FirstLineIndex)
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
            rdd_final: RDD[CompletedStudent] \
                = rdd8.map(StudentSnippetBuilder.completedFromSnippet)
            rdd_accumulative_completed \
                = rdd_accumulative_completed.union(rdd_final)
            break
    rdd_answer: RDD[StudentSummary] = (
        rdd_accumulative_completed
        .map(StudentSnippetBuilder.gradeSummary)
        .repartition(default_parallelism))
    return None, rdd_answer, None


def strainCompletedItems(
        lgroup: List[StudentSnippet],
) -> Tuple[List[CompletedStudent], List[StudentSnippet]]:
    completedList = []
    for i in range(len(lgroup) - 2, 0, -1):
        rec = lgroup[i]
        if rec.__class__.__name__ == 'CompletedStudent':
            completedList.append(rec)
            del lgroup[i]
        else:
            break
    return completedList, lgroup


def consolidateSnippetsInPartition(
        iter: Iterable[Tuple[int, bool, int, StudentSnippet]]
) -> Iterable[Tuple[bool, CompletedStudent | StudentSnippet]]:
    residual = []
    lGroupNumber = None
    lgroup = None

    rMember: StudentSnippet
    for rGroupNumber, rIsAtStartOfSegment, _passNumber, rMember in iter:
        if lGroupNumber is not None and rGroupNumber != lGroupNumber:
            assert lgroup is not None
            completedItems, lgroup = strainCompletedItems(lgroup)
            for item in completedItems:
                yield True, item
            assert lgroup is not None
            residual.extend(lgroup)
            lGroupNumber = None
            lgroup = None
        if lGroupNumber is None:
            lGroupNumber = rGroupNumber
            lgroup = [rMember]
            assert rIsAtStartOfSegment is True
        else:
            StudentSnippetBuilder.addSnippets(lgroup, [rMember])
    if lGroupNumber is not None:
        assert lgroup is not None
        completedItems, lgroup = strainCompletedItems(lgroup)
        for item in completedItems:
            yield True, item
        assert lgroup is not None
        residual.extend(lgroup)

    for item in residual:
        yield item.__class__.__name__ == 'CompletedStudent', item
