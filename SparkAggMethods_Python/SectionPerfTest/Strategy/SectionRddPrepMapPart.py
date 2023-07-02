from typing import Any, List, Tuple, cast

from pyspark import RDD
from pyspark.sql import DataFrame as spark_DataFrame

from Utils.SparkUtils import TidySparkSession

from ..SectionLogic import (
    aggregateTypedRowsToGrades, identifySectionUsingIntermediateFile)
from ..SectionRunResult import MaximumProcessableSegment, NumExecutors
from ..SectionTypeDefs import (
    ClassLine, DataSetDescription,
    StudentHeader, StudentSummary, TrimesterFooter, TrimesterHeader)


def method_prep_mappart(
    spark_session: TidySparkSession, data_set: DataSetDescription
) -> Tuple[List[StudentSummary] | None, RDD | None, spark_DataFrame | None]:
    sc = spark_session.spark_context
    dataSize = data_set.dataSize
    filename = data_set.filename

    def parseLineToTypesWithLineNo(args):
        lineNumber = args[1]
        line = args[0]
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
    #
    interFileName = identifySectionUsingIntermediateFile(filename)
    TargetNumPartitions = max(
        NumExecutors, (dataSize + MaximumProcessableSegment - 1) // MaximumProcessableSegment)
    rdd = \
        sc.textFile(interFileName, TargetNumPartitions) \
        .zipWithIndex() \
        .map(parseLineToTypesWithLineNo) \
        .map(lambda x: ((x[0], x[1]), x[2]))
    rdd = \
        cast(Any, rdd) \
        .repartitionAndSortWithinPartitions(
            numPartitions=TargetNumPartitions,
            partitionFunc=lambda x: x[0]) \
        .map(lambda x: x[1]) \
        .mapPartitions(aggregateTypedRowsToGrades)
    return None, rdd, None
