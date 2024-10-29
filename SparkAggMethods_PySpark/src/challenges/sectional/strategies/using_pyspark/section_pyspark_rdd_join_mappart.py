from typing import Iterable, cast

from pyspark import RDD
from spark_agg_methods_common_python.challenges.sectional.domain_logic.section_mutable_subtotal_type import (
    MutableStudent, MutableTrimester,
)
from spark_agg_methods_common_python.challenges.sectional.section_test_data_types import (
    ClassLine, LabeledTypedRow, StudentHeader, StudentSummary, TrimesterFooter, TrimesterHeader, TypedLine,
)

from src.challenges.sectional.domain_logic.section_data_parsers_pyspark import rdd_typed_with_index_factory
from src.challenges.sectional.section_test_data_types_pyspark import (
    SectionDataSetPyspark, SectionExecutionParametersPyspark, TChallengePythonPysparkAnswer,
)
from src.utils.tidy_session_pyspark import TidySparkSession


def section_pyspark_rdd_join_mappart(
        spark_session: TidySparkSession,
        exec_params: SectionExecutionParametersPyspark,
        data_set: SectionDataSetPyspark,
) -> TChallengePythonPysparkAnswer:
    if data_set.data_description.num_students > pow(10, 7-1):
        # times out
        return "infeasible"
    target_num_partitions = data_set.target_num_partitions
    sc = spark_session.spark_context
    rdd1: RDD[LabeledTypedRow] \
        = rdd_typed_with_index_factory(
            spark_session,
            data_set.source_data_file_path,
            data_set.target_num_partitions)
    NumRows = rdd1.count()
    rdd2: RDD[tuple[int, str, TypedLine]] = (
        rdd1
        .map(lambda x: (x.Index, x.Value.__class__.__name__, x.Value)))
    rdd8: RDD[tuple[int, str, TypedLine]] = (
        rdd2
        .filter(lambda x: x[1] == 'StudentHeader')
    )
    rdd9: RDD[tuple[tuple[int, str, TypedLine], int]] = (
        rdd8
        .zipWithIndex())
    rdd10: RDD[tuple[int, tuple[int, int]]] = (
        rdd9
        .map(lambda x: (x[1], (x[0][0], cast(StudentHeader, x[0][2]).StudentId))))
    rdd11: RDD[tuple[int, tuple[int, int]]] = (
        rdd10
        .map(lambda x: (x[0] - 1, x[1]))
        .filter(lambda x: x[0] >= 0)
        .union(sc.parallelize([(rdd10.count() - 1, (NumRows, cast(int, -1)))])))

    def unpack_student_header_from_tuples(
            x: tuple[int, tuple[tuple[int, int], tuple[int, int]]],
    ) -> list[tuple[int, int]]:
        shLineNumber, ((firstLineNo, studentId), (nextLineNo, _)) = x
        return [(lineNo, studentId) for lineNo in range(firstLineNo, nextLineNo)]

    rdd12: RDD[tuple[int, tuple[tuple[int, int], tuple[int, int]]]] = rdd10.join(rdd11)
    rdd13: RDD[tuple[int, int]] = rdd12.flatMap(unpack_student_header_from_tuples)
    rdd14: RDD[tuple[int, TypedLine]] = rdd2.map(lambda x: (x[0], x[2]))
    rdd15: RDD[tuple[int, tuple[TypedLine, int]]] = rdd14.join(rdd13)

    def repackage_typed_line_with_sh(
            x: tuple[int, tuple[TypedLine, int]],
    ) -> tuple[tuple[int, int], TypedLine]:
        lineNo, (typedRow, studentId) = x
        return ((studentId, lineNo), typedRow)

    rdd16: RDD[tuple[tuple[int, int], TypedLine]] = rdd15.map(repackage_typed_line_with_sh)
    rdd17: RDD[tuple[tuple[int, int], TypedLine]] = (
        rdd16
        .repartitionAndSortWithinPartitions(
            numPartitions=target_num_partitions,
            partitionFunc=lambda x: cast(tuple[int, int], x[0])))  # type: ignore

    rdd18: RDD[StudentSummary] = (
        rdd17
        .mapPartitions(extract_student_summary)
        .sortBy(lambda x: x.StudentId)  # pyright: ignore[reportArgumentType]
    )
    return rdd18


def extract_student_summary(
        iterator: Iterable[tuple[tuple[int, int], TypedLine]],
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
