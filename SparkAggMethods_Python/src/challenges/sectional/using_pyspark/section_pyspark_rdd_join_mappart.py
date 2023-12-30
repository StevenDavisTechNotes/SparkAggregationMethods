from typing import Iterable, List, Tuple, cast

from pyspark import RDD
from pyspark.sql import DataFrame as spark_DataFrame

from challenges.sectional.domain_logic.section_data_parsers import \
    rdd_typed_with_index_factory
from challenges.sectional.domain_logic.section_mutuable_subtotal_type import (
    MutableStudent, MutableTrimester)
from challenges.sectional.section_test_data_types import (ClassLine, DataSet,
                                                          LabeledTypedRow,
                                                          StudentHeader,
                                                          StudentSummary,
                                                          TrimesterFooter,
                                                          TrimesterHeader,
                                                          TypedLine)
from utils.tidy_spark_session import TidySparkSession


def section_pyspark_rdd_join_mappart(
        spark_session: TidySparkSession,
        data_set: DataSet,
) -> Tuple[List[StudentSummary] | None, RDD | None, spark_DataFrame | None]:
    sc = spark_session.spark_context
    rdd1: RDD[LabeledTypedRow] \
        = rdd_typed_with_index_factory(
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

    def unpack_student_header_from_tuples(
            x: Tuple[int, Tuple[Tuple[int, int], Tuple[int, int]]],
    ) -> List[Tuple[int, int]]:
        shLineNumber, ((firstLineNo, studentId), (nextLineNo, _)) = x
        return [(lineNo, studentId) for lineNo in range(firstLineNo, nextLineNo)]

    rdd12: RDD[Tuple[int, Tuple[Tuple[int, int], Tuple[int, int]]]] = rdd10.join(rdd11)
    rdd13: RDD[Tuple[int, int]] = rdd12.flatMap(unpack_student_header_from_tuples)
    rdd14: RDD[Tuple[int, TypedLine]] = rdd2.map(lambda x: (x[0], x[2]))
    rdd15: RDD[Tuple[int, Tuple[TypedLine, int]]] = rdd14.join(rdd13)

    def repackage_typed_line_with_sh(
            x: Tuple[int, Tuple[TypedLine, int]],
    ) -> Tuple[Tuple[int, int], TypedLine]:
        lineNo, (typedRow, studentId) = x
        return ((studentId, lineNo), typedRow)

    rdd16: RDD[Tuple[Tuple[int, int], TypedLine]] = rdd15.map(repackage_typed_line_with_sh)
    rdd17: RDD[Tuple[Tuple[int, int], TypedLine]] = (
        rdd16
        .repartitionAndSortWithinPartitions(
            numPartitions=data_set.data.target_num_partitions,
            partitionFunc=lambda x: cast(Tuple[int, int], x[0])))  # type: ignore

    rdd18: RDD[StudentSummary] = (
        rdd17
        .mapPartitions(extract_student_summary)
        .sortBy(lambda x: x.StudentId)  # pyright: ignore[reportGeneralTypeIssues]
    )
    return None, rdd18, None


def extract_student_summary(
        iterator: Iterable[Tuple[Tuple[int, int], TypedLine]],
) -> Iterable[StudentSummary]:
    student = None
    trimester = None
    for lineno, x in enumerate(iterator):
        (studentId, lineNo), rec = x
        match rec:
            case StudentHeader():
                if student is not None:
                    yield student.grade_summary()
                student = MutableStudent(rec.StudentId, rec.StudentName)
            case TrimesterHeader():
                trimester = MutableTrimester(rec.Date, rec.WasAbroad)
            case ClassLine():
                assert trimester is not None
                trimester.add_class(rec.Dept, rec.Credits, rec.Grade)
            case TrimesterFooter():
                assert student is not None
                assert trimester is not None
                trimester.add_footer(rec.Major, rec.GPA, rec.Credits)
                student.add_trimester(trimester)
                trimester = None
            case _:
                raise Exception(
                    f"Unknown parsed row type {rec.__class__.__name__} on line {lineno}")
    if student is not None:
        yield student.grade_summary()
