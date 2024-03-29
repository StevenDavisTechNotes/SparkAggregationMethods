from typing import List, Tuple

from pyspark import RDD
from pyspark.sql import DataFrame as spark_DataFrame
from SectionPerfTest.SectionMutuableSubtotal import aggregateTypedRowsToGrades

from Utils.TidySparkSession import TidySparkSession

from SectionPerfTest.SectionLogic import identifySectionUsingIntermediateFile
from SectionPerfTest.SectionTypeDefs import (
    ClassLine, DataSet, StudentHeader, StudentSummary, TrimesterFooter, TrimesterHeader, TypedLine)


def section_prep_mappart(
        spark_session: TidySparkSession,
        data_set: DataSet,
) -> Tuple[List[StudentSummary] | None, RDD | None, spark_DataFrame | None]:
    sc = spark_session.spark_context
    filename = data_set.data.test_filepath
    TargetNumPartitions = data_set.data.target_num_partitions

    interFileName = identifySectionUsingIntermediateFile(filename)
    rdd1: RDD[Tuple[Tuple[int, int], TypedLine]] = (
        sc.textFile(interFileName, TargetNumPartitions)
        .zipWithIndex()
        .map(lambda pair: parseLineToTypesWithLineNo(filename, pair))
        .map(lambda x: ((x[0], x[1]), x[2]))
    )
    rdd: RDD[StudentSummary] = (
        rdd1
        .repartitionAndSortWithinPartitions(
            numPartitions=TargetNumPartitions,
            partitionFunc=lambda x: x[0]) # type: ignore
        .map(lambda x: x[1])
        .mapPartitions(aggregateTypedRowsToGrades)
        .sortBy(lambda x: x.StudentId)
    )
    return None, rdd, None


def parseLineToTypesWithLineNo(
        filename: str,
        pair: Tuple[str, int],
) -> Tuple[int, int, TypedLine]:
    lineNumber = pair[1]
    line = pair[0]
    fields = line.split(',')
    sectionId = int(fields[0])
    fields = fields[1:]
    rowType = fields[0]
    if rowType == 'S':
        return (sectionId, lineNumber, StudentHeader(
            StudentId=int(fields[1]), StudentName=fields[2]))
    if rowType == 'TH':
        return (sectionId, lineNumber, TrimesterHeader(
            Date=fields[1], WasAbroad=(fields[2] == 'True')))
    if rowType == 'C':
        return (sectionId, lineNumber, ClassLine(
            Dept=int(fields[1]), Credits=int(fields[2]), Grade=int(fields[3])))
    if rowType == 'TF':
        return (sectionId, lineNumber, TrimesterFooter(
            Major=int(fields[1]), GPA=float(fields[2]), Credits=int(fields[3])))
    raise Exception(
        f"Unknown parsed row type {rowType} on line {lineNumber} in file {filename}")
