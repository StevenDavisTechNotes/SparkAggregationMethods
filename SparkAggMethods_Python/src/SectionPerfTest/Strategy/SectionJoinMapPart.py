from typing import Iterable, List, Tuple, cast

from pyspark import RDD
from pyspark.sql import DataFrame as spark_DataFrame
from SectionPerfTest.SectionMutuableSubtotal import MutableStudent, MutableTrimester

from Utils.TidySparkSession import TidySparkSession

from SectionPerfTest.SectionLogic import rddTypedWithIndexFactory
from SectionPerfTest.SectionTypeDefs import (
    ClassLine, DataSet, LabeledTypedRow, StudentHeader, StudentSummary, TrimesterFooter, TrimesterHeader)


def section_join_mappart(
        spark_session: TidySparkSession,
        data_set: DataSet,
) -> Tuple[List[StudentSummary] | None, RDD | None, spark_DataFrame | None]:
    sc = spark_session.spark_context
    rdd1: RDD[LabeledTypedRow] \
        = rddTypedWithIndexFactory(
            spark_session, data_set.data.test_filepath, data_set.data.target_num_partitions)
    NumRows = rdd1.count()
    rdd2: RDD[Tuple[int, str, LabeledTypedRow]] = (
        rdd1
        .map(lambda x: (x.Index, x.Value.__class__.__name__, x.Value)))
    rddSH0: RDD[Tuple[int, Tuple[int, int]]] = (
        rdd2
        .filter(lambda x: x[1] == 'StudentHeader')
        .zipWithIndex()
        .map(lambda x: (x[1], (x[0][0], x[0][2].StudentId))))
    rssSH1: RDD[Tuple[int, Tuple[int, int]]] = (
        rddSH0
        .map(lambda x: (x[0] - 1, x[1]))
        .filter(lambda x: x[0] >= 0)
        .union(sc.parallelize([(rddSH0.count() - 1, (NumRows, cast(int, -1)))])))

    def unpackStudentHeaderFromTuples(
            x: Tuple[int, Tuple[Tuple[int, int], Tuple[int, int]]],
    ) -> List[Tuple[int, int]]:
        shLineNumber, ((firstLineNo, studentId), (nextLineNo, _)) = x
        return [(lineNo, studentId) for lineNo in range(firstLineNo, nextLineNo)]
    rddSH2: RDD[Tuple[int, Tuple[Tuple[int, int], Tuple[int, int]]]] = rddSH0.join(rssSH1)
    rddSH: RDD[Tuple[int, int]] = rddSH2.flatMap(unpackStudentHeaderFromTuples)
    rdd2a: RDD[Tuple[int, LabeledTypedRow]] = rdd2.map(lambda x: (x[0], x[2]))
    rdd3: RDD[Tuple[Tuple[int, LabeledTypedRow], Tuple[int, int]]] = rdd2a.join(rddSH)

    def repackageTypedLineWithSH(
            x: Tuple[int, Tuple[LabeledTypedRow, int]],
    ) -> Tuple[Tuple[int, int], LabeledTypedRow]:
        lineNo, (typedRow, studentId) = x
        return ((studentId, lineNo), typedRow)
    rdd3a: RDD[Tuple[Tuple[int, int], LabeledTypedRow]] = rdd3.map(repackageTypedLineWithSH)
    rdd4: RDD[Tuple[Tuple[int, int], LabeledTypedRow]] = (
        rdd3a
        .repartitionAndSortWithinPartitions(
            numPartitions=data_set.data.target_num_partitions,
            partitionFunc=lambda x: cast(Tuple[int, int], x[0])))  # type: ignore

    rdd5: RDD[StudentSummary] = rdd4.mapPartitions(extractStudentSummary)
    return None, rdd5, None


def extractStudentSummary(
        iterator: Iterable[Tuple[Tuple[int, int], LabeledTypedRow]],
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
