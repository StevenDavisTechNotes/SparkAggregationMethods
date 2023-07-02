import math
from typing import List, Tuple

from pyspark import RDD, StorageLevel
from pyspark.sql import DataFrame as spark_DataFrame

from Utils.SparkUtils import TidySparkSession

from ..SectionLogic import (
    StudentSnippetBuilder, parseLineToTypes, rddTypedWithIndexFactory)
from ..SectionRunResult import MaximumProcessableSegment, NumExecutors
from ..SectionTypeDefs import DataSetDescription, LabeledTypedRow, StudentSummary


def method_reduce_partials_broken(
        spark_session: TidySparkSession,
        data_set: DataSetDescription,
) -> Tuple[List[StudentSummary] | None, RDD | None, spark_DataFrame | None]:
    sectionMaximum = data_set.sectionMaximum
    dataSize = data_set.dataSize
    filename = data_set.filename
    TargetNumPartitions = max(
        NumExecutors, (dataSize + MaximumProcessableSegment - 1) // MaximumProcessableSegment)
    rdd = rddTypedWithIndexFactory(
        spark_session, filename, TargetNumPartitions)
    dataSize = rdd.count()
    rdd = rdd \
        .map(lambda x: [StudentSnippetBuilder.studentSnippetFromTypedRow(x.Index, x.Value)])
    targetDepth = max(1, math.ceil(
        math.log(dataSize / MaximumProcessableSegment, MaximumProcessableSegment - 2)))
    students = rdd.treeAggregate(
        [], StudentSnippetBuilder.addSnippets, StudentSnippetBuilder.addSnippets, depth=targetDepth)
    if len(students) > 0:
        students[-1] = StudentSnippetBuilder.completedFromSnippet(students[-1])
    students = [StudentSnippetBuilder.gradeSummary(x) for x in students]
    return students, None, None


def nonCommutativeTreeAggregate(
        rdd, zeroValueFactory, seqOp, combOp, depth=2, divisionBase=2):
    def aggregatePartition(ipart, iterator):
        acc = zeroValueFactory()
        lastindex = None
        for index, obj in iterator:
            acc = seqOp(acc, obj)
            lastindex = index
        yield (lastindex, acc)

    def combineInPartition(ipart, iterator):
        acc = zeroValueFactory()
        lastindex = None
        for (ipart, subindex), (index, obj) in iterator:
            acc = seqOp(acc, obj)
            lastindex = index
        if lastindex is not None:
            yield (lastindex, acc)

    persistedRDD = rdd
    rdd.persist(StorageLevel.DISK_ONLY)
    rdd = rdd \
        .zipWithIndex() \
        .persist(StorageLevel.DISK_ONLY)
    numRows = rdd.count()
    persistedRDD.unpersist()
    persistedRDD = rdd
    rdd = rdd \
        .map(lambda x: (x[1], x[0])) \
        .mapPartitionsWithIndex(aggregatePartition)
    for idepth in range(depth - 1, -1, -1):
        rdd.localCheckpoint()
        numSegments = pow(divisionBase, idepth)
        if numSegments >= numRows:
            continue
        segmentSize = (numRows + numSegments - 1) // numSegments
        rdd = rdd \
            .keyBy(lambda x: (x[0] // segmentSize, x[0] % segmentSize)) \
            .repartitionAndSortWithinPartitions(
                numPartitions=numSegments,
                partitionFunc=lambda x: x[0]) \
            .mapPartitionsWithIndex(combineInPartition)
    rdd = rdd \
        .flatMap(lambda x: x[1])
    return rdd


def method_asymreduce_partials(
        spark_session: TidySparkSession,
        data_set: DataSetDescription,
) -> Tuple[List[StudentSummary] | None, RDD | None, spark_DataFrame | None]:
    sc = spark_session.spark_context
    dataSize = data_set.dataSize
    filename = data_set.filename
    TargetNumPartitions = max(
        NumExecutors, (dataSize + MaximumProcessableSegment - 1) // MaximumProcessableSegment)
    rdd = sc.textFile(filename, minPartitions=TargetNumPartitions) \
        .zipWithIndex() \
        .map(lambda x: LabeledTypedRow(Index=x[1], Value=parseLineToTypes(x[0]))) \
        .map(lambda x: [StudentSnippetBuilder.studentSnippetFromTypedRow(x.Index, x.Value)])
    divisionBase = 2
    targetDepth = max(1, math.ceil(
        math.log(dataSize / MaximumProcessableSegment, divisionBase)))
    rdd = nonCommutativeTreeAggregate(
        rdd,
        lambda: [],
        StudentSnippetBuilder.addSnippetsWOCompleting,
        StudentSnippetBuilder.addSnippetsWOCompleting,
        depth=targetDepth,
        divisionBase=divisionBase)
    rdd = rdd \
        .map(StudentSnippetBuilder.completedFromSnippet) \
        .map(StudentSnippetBuilder.gradeSummary)
    return None, rdd, None
