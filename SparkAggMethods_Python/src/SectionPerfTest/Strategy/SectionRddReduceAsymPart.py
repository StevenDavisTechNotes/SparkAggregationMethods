import math
from typing import Callable, Iterable, List, Tuple, cast

from pyspark import RDD, StorageLevel
from pyspark.sql import DataFrame as spark_DataFrame

from SectionPerfTest.SectionLogic import (
    parseLineToTypes, rddTypedWithIndexFactory)
from SectionPerfTest.SectionSnippetSubtotal import (
    StudentSnippet, StudentSnippetBuilder)
from SectionPerfTest.SectionTypeDefs import (
    DataSet, LabeledTypedRow, StudentSummary)
from Utils.SparkUtils import TidySparkSession


def section_reduce_partials_broken(
        spark_session: TidySparkSession,
        data_set: DataSet,
) -> Tuple[List[StudentSummary] | None, RDD | None, spark_DataFrame | None]:
    dataSize = data_set.description.num_rows
    filename = data_set.data.test_filepath
    TargetNumPartitions = data_set.data.target_num_partitions
    MaximumProcessableSegment = data_set.exec_params.MaximumProcessableSegment

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


def section_asymreduce_partials(
        spark_session: TidySparkSession,
        data_set: DataSet,
) -> Tuple[List[StudentSummary] | None, RDD | None, spark_DataFrame | None]:
    sc = spark_session.spark_context
    dataSize = data_set.description.num_rows
    filename = data_set.data.test_filepath
    TargetNumPartitions = data_set.data.target_num_partitions
    MaximumProcessableSegment = data_set.exec_params.MaximumProcessableSegment
    rdd1a: RDD[str] = sc.textFile(filename, minPartitions=TargetNumPartitions)
    rdd1b: RDD[Tuple[str, int]] = rdd1a.zipWithIndex()
    rdd1c: RDD[LabeledTypedRow] = rdd1b \
        .map(lambda x: LabeledTypedRow(Index=x[1], Value=parseLineToTypes(x[0])))
    rdd1: RDD[List[StudentSnippet]] = rdd1c \
        .map(lambda x: [StudentSnippetBuilder.studentSnippetFromTypedRow(x.Index, x.Value)])
    divisionBase = 2
    targetDepth = max(1, math.ceil(
        math.log(dataSize / MaximumProcessableSegment, divisionBase)))
    rdd2 = nonCommutativeTreeAggregate(
        rdd1,
        lambda: cast(List[StudentSnippet], []),
        StudentSnippetBuilder.addSnippetsWOCompleting,
        StudentSnippetBuilder.addSnippetsWOCompleting,
        depth=targetDepth,
        divisionBase=divisionBase)
    rdd3 = rdd2 \
        .map(StudentSnippetBuilder.completedFromSnippet)
    rdd4: RDD[StudentSummary] = rdd3 \
        .map(StudentSnippetBuilder.gradeSummary)
    return None, rdd4, None


def nonCommutativeTreeAggregate(
        rdd1: RDD[List[StudentSnippet]],
        zeroValueFactory: Callable[[], List[StudentSnippet]],
        seqOp: Callable[[List[StudentSnippet], List[StudentSnippet]], List[StudentSnippet]],
        combOp: Callable[[List[StudentSnippet], List[StudentSnippet]], List[StudentSnippet]],
        depth: int = 2,
        divisionBase: int = 2
) -> RDD[StudentSnippet]:
    def aggregatePartition(
            ipart: int,
            iterator: Iterable[Tuple[int, List[StudentSnippet]]],
    ) -> Iterable[Tuple[int, List[StudentSnippet]]]:
        acc = zeroValueFactory()
        lastindex = None
        for index, obj in iterator:
            acc = seqOp(acc, obj)
            lastindex = index
        assert lastindex is not None
        yield (lastindex, acc)

    def combineInPartition(
            ipart: int,
            iterator: Iterable[Tuple[Tuple[int, int], Tuple[int, List[StudentSnippet]]]],
    ) -> Iterable[Tuple[int, List[StudentSnippet]]]:
        acc = zeroValueFactory()
        lastindex = None
        for (ipart, subindex), (index, obj) in iterator:
            acc = seqOp(acc, obj)
            lastindex = index
        if lastindex is not None:
            yield (lastindex, acc)

    persistedRDD = rdd1
    rdd1.persist(StorageLevel.DISK_ONLY)
    rdd2: RDD[Tuple[List[StudentSnippet], int]] = rdd1.zipWithIndex()
    rdd2.persist(StorageLevel.DISK_ONLY)
    numRows = rdd2.count()
    persistedRDD.unpersist()
    persistedRDD = rdd2
    rdd3: RDD[Tuple[int, List[StudentSnippet]]] = rdd2.map(lambda x: (x[1], x[0]))
    rdd_loop: RDD[Tuple[int, List[StudentSnippet]]] = rdd3.mapPartitionsWithIndex(aggregatePartition)
    for idepth in range(depth - 1, -1, -1):
        rdd_loop.localCheckpoint()
        numSegments = pow(divisionBase, idepth)
        if numSegments >= numRows:
            continue
        segmentSize = (numRows + numSegments - 1) // numSegments
        rdd5: RDD[Tuple[Tuple[int, int], Tuple[int, List[StudentSnippet]]]] = rdd_loop \
            .keyBy(lambda x: (x[0] // segmentSize, x[0] % segmentSize))
        rdd6 = rdd5 \
            .repartitionAndSortWithinPartitions(
                numPartitions=numSegments,
                partitionFunc=lambda x: x[0])  # type: ignore
        rdd7: RDD[Tuple[int, List[StudentSnippet]]] = rdd6 \
            .mapPartitionsWithIndex(combineInPartition)
        rdd_loop = rdd7
    rdd8: RDD[StudentSnippet] = rdd_loop \
        .flatMap(lambda x: cast(List[StudentSnippet], x[1]))
    return rdd8
