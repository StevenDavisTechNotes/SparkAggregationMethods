from typing import List, Tuple

from pyspark import RDD, StorageLevel
from pyspark.sql import DataFrame as spark_DataFrame

from Utils.SparkUtils import TidySparkSession

from ..SectionLogic import CompletedStudent, StudentSnippetBuilder, rddTypedWithIndexFactory
from ..SectionRunResult import MaximumProcessableSegment, NumExecutors
from ..SectionTypeDefs import (
    DataSetDescription, LabeledTypedRow, StudentSummary)


def method_mappart_partials(
    spark_session: TidySparkSession, data_set: DataSetDescription
) -> Tuple[List[StudentSummary] | None, RDD | None, spark_DataFrame | None]:
    sc = spark_session.spark_context
    dataSize = data_set.dataSize
    filename = data_set.filename

    maximumProcessableSegment = MaximumProcessableSegment
    targetNumPartitions = max(
        NumExecutors, (dataSize + maximumProcessableSegment - 1) // maximumProcessableSegment)

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
        dataSize = rdd.count()
        rdd = rdd.zipWithIndex() \
            .map(lambda pair: LabeledTypedRow(Index=pair[1], Value=pair[0]))
        # rdd type = RDD<LabeledTypedRow(index, snippet)>
        #
        rdd = rdd.keyBy(lambda x: (x.Index // maximumProcessableSegment,
                                   x.Index % maximumProcessableSegment))
        # type = RDD<((igroup, iremainder),(index, snippet))>
        #
        targetNumPartitions = max(
            NumExecutors, (dataSize + maximumProcessableSegment - 1) // maximumProcessableSegment)
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
        rddCumulativeCompleted.count()
        # type: RDD[StudentSnippet]
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
    rdd = rddCumulativeCompleted \
        .map(StudentSnippetBuilder.gradeSummary)
    # type: RDD[StudentSummary]
    #
    return None, rdd, None


def strainCompletedItems(lgroup):
    completedList = []
    for i in range(len(lgroup) - 2, 0, -1):
        rec = lgroup[i]
        if rec.__class__.__name__ == 'CompletedStudent':
            completedList.append(rec)
            del lgroup[i]
        else:
            break
    return completedList, lgroup


def consolidateSnippetsInPartition(iter):
    residual = []
    lGroupNumber = None
    lgroup = None
    for rGroupNumber, rIsAtStartOfSegment, _passNumber, rMember in iter:
        if lGroupNumber is not None and rGroupNumber != lGroupNumber:
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
        completedItems, lgroup = strainCompletedItems(lgroup)
        for item in completedItems:
            yield True, item
        assert lgroup is not None
        residual.extend(lgroup)
    for item in residual:
        yield item.__class__.__name__ == 'CompletedStudent', item
