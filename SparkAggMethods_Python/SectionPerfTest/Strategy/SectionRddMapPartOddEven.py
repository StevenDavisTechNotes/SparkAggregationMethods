from typing import Any, List, Tuple, cast

from pyspark import RDD
from pyspark.sql import DataFrame as spark_DataFrame

from Utils.SparkUtils import TidySparkSession

from ..SectionLogic import (
    MutableStudent, MutableTrimester,
    aggregateTypedRowsToGrades, parseLineToTypes,
    rddTypedWithIndexFactory)
from ..SectionRunResult import MaximumProcessableSegment, NumExecutors
from ..SectionTypeDefs import DataSetDescription, StudentSummary


def method_mappart_single_threaded(
    spark_session: TidySparkSession, data_set: DataSetDescription
) -> Tuple[List[StudentSummary] | None, RDD | None, spark_DataFrame | None]:
    sc = spark_session.spark_context
    rdd = sc.textFile(data_set.filename, minPartitions=1)
    rdd = rdd \
        .map(parseLineToTypes) \
        .mapPartitions(aggregateTypedRowsToGrades)
    return None, rdd, None


def method_mappart_odd_even(
    spark_session: TidySparkSession, data_set: DataSetDescription
) -> Tuple[List[StudentSummary] | None, RDD | None, spark_DataFrame | None]:
    sectionMaximum = data_set.sectionMaximum
    dataSize = data_set.dataSize
    filename = data_set.filename

    SegmentOffset = sectionMaximum - 1
    SegmentExtra = 2 * sectionMaximum
    SegmentSize = SegmentOffset + sectionMaximum - 1 + SegmentExtra
    TargetNumPartitions = max(
        NumExecutors, (dataSize + MaximumProcessableSegment - 1) // MaximumProcessableSegment)
    #
    rddTypedWithIndex = rddTypedWithIndexFactory(
        spark_session, filename, TargetNumPartitions)
    rddSegmentsEven = cast(Any, rddTypedWithIndex) \
        .keyBy(lambda x: (x.Index // SegmentSize, x.Index)) \
        .repartitionAndSortWithinPartitions(
            numPartitions=TargetNumPartitions,
            partitionFunc=lambda x: x[0]) \
        .map(lambda x: x[1])
    rddSegmentsOdd = cast(Any, rddTypedWithIndex) \
        .keyBy(lambda x: ((x.Index - SegmentOffset) // SegmentSize, x.Index)) \
        .repartitionAndSortWithinPartitions(
            numPartitions=TargetNumPartitions,
            partitionFunc=lambda x: x[0]) \
        .filter(lambda x: x[0][0] >= 0) \
        .map(lambda x: x[1])
    rddSegments = rddSegmentsEven.union(rddSegmentsOdd)

    def aggregate(iterator):
        student = None
        trimester = None
        prevIndex = -1
        for labeled_row in iterator:
            index = labeled_row.Index
            if prevIndex + 1 != index:
                if student is not None:
                    yield student.gradeSummary()
                    student = None
            prevIndex = index
            rec = labeled_row.Value
            if rec.__class__.__name__ == 'StudentHeader':
                if student is not None:
                    yield student.gradeSummary()
                    student = None
                student = MutableStudent(rec.StudentId, rec.StudentName)
            elif student is None:
                pass
            elif rec.__class__.__name__ == 'TrimesterHeader':
                trimester = MutableTrimester(rec.Date, rec.WasAbroad)
            elif trimester is None:
                pass
            elif rec.__class__.__name__ == 'ClassLine':
                trimester.addClass(rec.Dept, rec.Credits, rec.Grade)
            elif rec.__class__.__name__ == 'TrimesterFooter':
                trimester.addFooter(rec.Major, rec.GPA, rec.Credits)
                student.addTrimester(trimester)
                trimester = None
            else:
                raise Exception("Unknown parsed row type")
        if student is not None:
            yield student.gradeSummary()

    def chooseCompleteSection(iterator):
        held = None
        for rec in iterator:
            if held is not None:
                if held.StudentId != rec.StudentId:
                    yield held
                    held = rec
                elif rec.SourceLines > held.SourceLines:
                    held = rec
            else:
                held = rec
        if held is not None:
            yield held

    rddParallelMapPartitionsInter = rddSegments.mapPartitions(aggregate)
    rddParallelMapPartitions = rddParallelMapPartitionsInter \
        .keyBy(lambda x: (x.StudentId, x.SourceLines)) \
        .repartitionAndSortWithinPartitions(
            numPartitions=TargetNumPartitions,
            partitionFunc=lambda x: x[0]) \
        .map(lambda x: x[1]) \
        .mapPartitions(chooseCompleteSection) \
        .sortBy(lambda x: x.StudentId)
    rdd = rddParallelMapPartitions
    return None, rdd, None
