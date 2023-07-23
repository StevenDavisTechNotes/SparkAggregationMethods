from typing import Iterable, List, Tuple

from pyspark import RDD
from pyspark.sql import DataFrame as spark_DataFrame

from Utils.SparkUtils import TidySparkSession

from ..SectionLogic import MutableStudent, MutableTrimester, rddTypedWithIndexFactory
from ..SectionRunResult import MaximumProcessableSegment, NumExecutors
from ..SectionTypeDefs import DataSetDescription, StudentSummary


def method_join_mappart(
    spark_session: TidySparkSession, data_set: DataSetDescription
) -> Tuple[List[StudentSummary] | None, RDD | None, spark_DataFrame | None]:
    sc = spark_session.spark_context
    TargetNumPartitions = max(
        NumExecutors, (data_set.dataSize + MaximumProcessableSegment - 1) // MaximumProcessableSegment)
    rdd = rddTypedWithIndexFactory(
        spark_session, data_set.filename, TargetNumPartitions)
    NumRows = rdd.count()
    rdd = (
        rdd
        .map(lambda x: (x.Index, x.Value.__class__.__name__, x.Value)))
    rddSH0 = (
        rdd
        .filter(lambda x: x[1] == 'StudentHeader')
        .zipWithIndex()
        .map(lambda x: (x[1], (x[0][0], x[0][2].StudentId))))
    rssSH1 = (
        rddSH0
        .map(lambda x: (x[0] - 1, x[1]))
        .filter(lambda x: x[0] >= 0)
        .union(sc.parallelize([(rddSH0.count() - 1, (NumRows, -1))])))

    def unpackStudentHeaderFromTuples(x):
        shLineNumber, ((firstLineNo, studentId), (nextLineNo, _)) = x
        return [(lineNo, studentId) for lineNo in range(firstLineNo, nextLineNo)]
    rddSH = (
        rddSH0.join(rssSH1)
        .flatMap(unpackStudentHeaderFromTuples))
    rdd = (
        rdd
        .map(lambda x: (x[0], x[2]))
        .join(rddSH))

    def repackageTypedLineWithSH(x):
        lineNo, (typedRow, studentId) = x
        return ((studentId, lineNo), typedRow)
    rdd = (
        rdd
        .map(repackageTypedLineWithSH)
        .repartitionAndSortWithinPartitions(
            numPartitions=TargetNumPartitions,
            partitionFunc=lambda x: x[0]))  # type: ignore

    rdd = (
        rdd
        .mapPartitions(extractStudentSummary))
    return None, rdd, None


def extractStudentSummary(iterator) -> Iterable[StudentSummary]:
    student = None
    trimester = None
    for lineno, x in enumerate(iterator):
        (studentId, lineNo), rec = x
        if rec.__class__.__name__ == 'StudentHeader':
            if student is not None:
                yield student.gradeSummary()
            student = MutableStudent(rec.StudentId, rec.StudentName)
        elif rec.__class__.__name__ == 'TrimesterHeader':
            trimester = MutableTrimester(rec.Date, rec.WasAbroad)
        elif rec.__class__.__name__ == 'ClassLine':
            assert trimester is not None
            trimester.addClass(rec.Dept, rec.Credits, rec.Grade)
        elif rec.__class__.__name__ == 'TrimesterFooter':
            assert student is not None
            assert trimester is not None
            trimester.addFooter(rec.Major, rec.GPA, rec.Credits)
            student.addTrimester(trimester)
            trimester = None
        else:
            raise Exception(
                f"Unknown parsed row type {rec.__class__.__name__} on line {lineno}")
    if student is not None:
        yield student.gradeSummary()
