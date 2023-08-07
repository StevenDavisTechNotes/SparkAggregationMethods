from typing import Iterable, List, Tuple

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

    rdd = rddTypedWithIndexFactory(spark_session, filename, targetNumPartitions) \
        .map(lambda x: StudentSnippetBuilder.studentSnippetFromTypedRow(x.Index, x.Value))
    rdd.persist(StorageLevel.DISK_ONLY)
    # rdd type = RDD<StudentSnippet>
    #
    rddCumulativeCompleted: RDD[CompletedStudent] = sc.parallelize([])
    passNumber = 0
    while True:
        passNumber += 1
        # rdd type = RDD<StudentSnippet>
        #
        remaining_num_rows = rdd.count()
        rdd = rdd.zipWithIndex() \
            .map(lambda pair: LabeledTypedRow(Index=pair[1], Value=pair[0]))
        # rdd type = RDD<LabeledTypedRow(index, snippet)>
        #
        rdd = rdd.keyBy(lambda x: (x.Index // maximum_processable_segment,
                                   x.Index % maximum_processable_segment))
        # type = RDD<((igroup, iremainder),(index, snippet))>
        #
        targetNumPartitions = int_divide_round_up(remaining_num_rows, maximum_processable_segment)
        rdd = rdd.repartitionAndSortWithinPartitions(
            numPartitions=targetNumPartitions,
            partitionFunc=lambda x: x[0])  # type: ignore
        #
        rdd = rdd.map(lambda x: (
            x[0][0], x[0][1] == 0, passNumber, x[1].Value))
        # type = RDD[Tuple[igroup, bool, pass, StudentSnippet]]
        #
        rdd = rdd.mapPartitions(consolidateSnippetsInPartition)
        rdd.persist(StorageLevel.DISK_ONLY)
        # type RDD<(bool, StudentSnippet)>
        #
        rddCompleted: RDD[CompletedStudent] \
            = rdd.filter(lambda x: x[0]).map(lambda x: x[1])
        # type: RDD[CompletedStudent]
        #
        rddCumulativeCompleted = rddCumulativeCompleted.union(rddCompleted)
        rddCumulativeCompleted.localCheckpoint()
        # type: RDD[StudentSnippet]
        #
        complete_count = rddCumulativeCompleted.count()
        print(f"Completed {complete_count} of {initial_num_rows}")
        #
        rdd = rdd.filter(lambda x: not x[0]).map(lambda x: x[1])
        # type: RDD[StudentSnippet]
        #
        rdd = rdd.sortBy(lambda x: x.FirstLineIndex)
        rdd.persist(StorageLevel.DISK_ONLY)
        # type: RDD[StudentSnippet]
        #
        NumRowsLeftToProcess = rdd.count()
        if passNumber == 20:
            print("Failed to complete")
            print(rdd.collect())
            raise Exception("Failed to complete")
        if NumRowsLeftToProcess == 1:
            rddFinal: RDD[CompletedStudent] \
                = rdd.map(StudentSnippetBuilder.completedFromSnippet)
            # type: RDD[CompletedStudent]
            #
            rddCumulativeCompleted \
                = rddCumulativeCompleted.union(rddFinal)
            # type: RDD[CompletedStudent]
            #
            break
    rdd = (
        rddCumulativeCompleted
        .map(StudentSnippetBuilder.gradeSummary)
        .repartition(default_parallelism))
    # type: RDD[StudentSummary]
    #
    return None, rdd, None


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
