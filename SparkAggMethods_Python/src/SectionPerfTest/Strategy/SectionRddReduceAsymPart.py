import math
from typing import List, Tuple, cast

from pyspark import RDD, StorageLevel
from pyspark.sql import DataFrame as spark_DataFrame

from SectionPerfTest.SectionLogic import (
    parseLineToTypes, rddTypedWithIndexFactory)
from SectionPerfTest.SectionSnippetSubtotal import CompletedStudent, StudentSnippet1, mergeSnippetLists1, completedFromSnippet1, gradeSummary, studentSnippetFromTypedRow1
from SectionPerfTest.SectionTypeDefs import (
    DataSet, LabeledTypedRow, StudentSummary)
from Utils.NonCommutativeTreeAggregate import nonCommutativeTreeAggregate
from Utils.TidySparkSession import TidySparkSession


def section_reduce_partials_broken(
        spark_session: TidySparkSession,
        data_set: DataSet,
) -> Tuple[List[StudentSummary] | None, RDD | None, spark_DataFrame | None]:
    dataSize = data_set.description.num_rows
    filename = data_set.data.test_filepath
    TargetNumPartitions = data_set.data.target_num_partitions
    MaximumProcessableSegment = data_set.exec_params.MaximumProcessableSegment

    rdd1: RDD[LabeledTypedRow] \
        = rddTypedWithIndexFactory(
        spark_session, filename, TargetNumPartitions)
    dataSize = rdd1.count()
    rdd2: RDD[List[StudentSnippet1]] = rdd1 \
        .map(lambda x: [studentSnippetFromTypedRow1(x.Index, x.Value)])
    targetDepth = max(1, math.ceil(
        math.log(dataSize / MaximumProcessableSegment, MaximumProcessableSegment - 2)))
    students1: List[StudentSnippet1] \
        = rdd2.treeAggregate(
        [], mergeSnippetLists1, mergeSnippetLists1, depth=targetDepth)
    students2: List[CompletedStudent] \
        = [completedFromSnippet1(x) for x in students1]
    students3: List[StudentSummary] \
        = [gradeSummary(x) for x in students2]
    return students3, None, None


def section_asymreduce_partials(
        spark_session: TidySparkSession,
        data_set: DataSet,
) -> Tuple[List[StudentSummary] | None, RDD | None, spark_DataFrame | None]:
    sc = spark_session.spark_context
    dataSize = data_set.description.num_rows
    filename = data_set.data.test_filepath
    TargetNumPartitions = data_set.data.target_num_partitions
    MaximumProcessableSegment = data_set.exec_params.MaximumProcessableSegment
    rdd1: RDD[str] = sc.textFile(filename, minPartitions=TargetNumPartitions)
    rdd2: RDD[Tuple[str, int]] = rdd1.zipWithIndex()
    rdd13: RDD[LabeledTypedRow] = rdd2 \
        .map(lambda x: LabeledTypedRow(Index=x[1], Value=parseLineToTypes(x[0])))
    rdd4: RDD[List[StudentSnippet1]] = rdd13 \
        .map(lambda x: [studentSnippetFromTypedRow1(x.Index, x.Value)])
    divisionBase = 2
    targetDepth = max(1, math.ceil(
        math.log(dataSize / MaximumProcessableSegment, divisionBase)))
    rdd5: RDD[List[StudentSnippet1]] = nonCommutativeTreeAggregate(
        rdd4,
        lambda: cast(List[StudentSnippet1], list()),
        mergeSnippetLists1,
        mergeSnippetLists1,
        depth=targetDepth,
        divisionBase=divisionBase,
        storage_level=StorageLevel.DISK_ONLY,
    )
    rdd6: RDD[StudentSnippet1] = rdd5 \
        .flatMap(lambda x: x)
    rdd7 = rdd6 \
        .map(completedFromSnippet1)
    rdd8: RDD[StudentSummary] = rdd7 \
        .map(gradeSummary)
    return None, rdd8, None
