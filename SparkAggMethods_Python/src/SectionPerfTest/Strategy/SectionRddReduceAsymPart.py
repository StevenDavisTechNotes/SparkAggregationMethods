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
    rdd1: RDD[str] = sc.textFile(filename, minPartitions=TargetNumPartitions)
    rdd2: RDD[Tuple[str, int]] = rdd1.zipWithIndex()
    rdd13: RDD[LabeledTypedRow] = rdd2 \
        .map(lambda x: LabeledTypedRow(Index=x[1], Value=parseLineToTypes(x[0])))
    rdd4: RDD[List[StudentSnippet]] = rdd13 \
        .map(lambda x: [StudentSnippetBuilder.studentSnippetFromTypedRow(x.Index, x.Value)])
    divisionBase = 2
    targetDepth = max(1, math.ceil(
        math.log(dataSize / MaximumProcessableSegment, divisionBase)))
    rdd5 = nonCommutativeTreeAggregate(
        rdd4,
        lambda: cast(List[StudentSnippet], []),
        StudentSnippetBuilder.addSnippetsWOCompleting,
        StudentSnippetBuilder.addSnippetsWOCompleting,
        depth=targetDepth,
        divisionBase=divisionBase,
        storage_level=StorageLevel.DISK_ONLY,
    )
    rdd6: RDD[StudentSnippet] = rdd5 \
        .flatMap(lambda x: x)
    rdd7 = rdd6 \
        .map(StudentSnippetBuilder.completedFromSnippet)
    rdd8: RDD[StudentSummary] = rdd7 \
        .map(StudentSnippetBuilder.gradeSummary)
    return None, rdd8, None
