from typing import Iterable, List, Tuple, cast

from pyspark import RDD
from pyspark.sql import DataFrame as spark_DataFrame

from challenges.sectional.SectionLogic import rddTypedWithIndexFactory
from challenges.sectional.SectionMutuableSubtotal import (MutableStudent,
                                                          MutableTrimester)
from challenges.sectional.SectionTypeDefs import (ClassLine, DataSet,
                                                  LabeledTypedRow,
                                                  StudentHeader,
                                                  StudentSummary,
                                                  TrimesterFooter,
                                                  TrimesterHeader, TypedLine)
from utils.TidySparkSession import TidySparkSession


def section_join_mappart(
        spark_session: TidySparkSession,
        data_set: DataSet,
) -> Tuple[List[StudentSummary] | None, RDD | None, spark_DataFrame | None]:
    sc = spark_session.spark_context
    rdd1: RDD[LabeledTypedRow] \
        = rddTypedWithIndexFactory(
            spark_session, data_set.data.test_filepath, data_set.data.target_num_partitions)
    NumRows = rdd1.count()
    rdd2: RDD[Tuple[int, str, TypedLine]] = (
        rdd1
        .map(lambda x: (x.Index, x.Value.__class__.__name__, x.Value)))
    rdd8: RDD[Tuple[int, str, TypedLine]] = (
        rdd2
        .filter(lambda x: x[1] == 'StudentHeader')
    )
    rdd9: RDD[Tuple[Tuple[int, str, TypedLine], int]] = (
        rdd8
        .zipWithIndex())
    rdd10: RDD[Tuple[int, Tuple[int, int]]] = (
        rdd9
        .map(lambda x: (x[1], (x[0][0], cast(StudentHeader, x[0][2]).StudentId))))
    rdd11: RDD[Tuple[int, Tuple[int, int]]] = (
        rdd10
        .map(lambda x: (x[0] - 1, x[1]))
        .filter(lambda x: x[0] >= 0)
        .union(sc.parallelize([(rdd10.count() - 1, (NumRows, cast(int, -1)))])))

    def unpackStudentHeaderFromTuples(
            x: Tuple[int, Tuple[Tuple[int, int], Tuple[int, int]]],
    ) -> List[Tuple[int, int]]:
        shLineNumber, ((firstLineNo, studentId), (nextLineNo, _)) = x
        return [(lineNo, studentId) for lineNo in range(firstLineNo, nextLineNo)]

    rdd12: RDD[Tuple[int, Tuple[Tuple[int, int], Tuple[int, int]]]] = rdd10.join(rdd11)
    rdd13: RDD[Tuple[int, int]] = rdd12.flatMap(unpackStudentHeaderFromTuples)
    rdd14: RDD[Tuple[int, TypedLine]] = rdd2.map(lambda x: (x[0], x[2]))
    rdd15: RDD[Tuple[int, Tuple[TypedLine, int]]] = rdd14.join(rdd13)

    def repackageTypedLineWithSH(
            x: Tuple[int, Tuple[TypedLine, int]],
    ) -> Tuple[Tuple[int, int], TypedLine]:
        lineNo, (typedRow, studentId) = x
        return ((studentId, lineNo), typedRow)

    rdd16: RDD[Tuple[Tuple[int, int], TypedLine]] = rdd15.map(repackageTypedLineWithSH)
    rdd17: RDD[Tuple[Tuple[int, int], TypedLine]] = (
        rdd16
        .repartitionAndSortWithinPartitions(
            numPartitions=data_set.data.target_num_partitions,
            partitionFunc=lambda x: cast(Tuple[int, int], x[0])))  # type: ignore

    rdd18: RDD[StudentSummary] = (
        rdd17
        .mapPartitions(extractStudentSummary)
        .sortBy(lambda x: x.StudentId)  # pyright: ignore[reportGeneralTypeIssues]
    )
    return None, rdd18, None


def extractStudentSummary(
        iterator: Iterable[Tuple[Tuple[int, int], TypedLine]],
) -> Iterable[StudentSummary]:
    student = None
    trimester = None
    for lineno, x in enumerate(iterator):
        (studentId, lineNo), rec = x
        match rec:
            case StudentHeader():
                if student is not None:
                    yield student.gradeSummary()
                student = MutableStudent(rec.StudentId, rec.StudentName)
            case TrimesterHeader():
                trimester = MutableTrimester(rec.Date, rec.WasAbroad)
            case ClassLine():
                assert trimester is not None
                trimester.addClass(rec.Dept, rec.Credits, rec.Grade)
            case TrimesterFooter():
                assert student is not None
                assert trimester is not None
                trimester.addFooter(rec.Major, rec.GPA, rec.Credits)
                student.addTrimester(trimester)
                trimester = None
            case _:
                raise Exception(
                    f"Unknown parsed row type {rec.__class__.__name__} on line {lineno}")
    if student is not None:
        yield student.gradeSummary()
