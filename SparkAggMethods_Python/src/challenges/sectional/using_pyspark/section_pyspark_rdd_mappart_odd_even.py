from typing import Any, Iterable, Optional, cast

from challenges.sectional.domain_logic.section_data_parsers import \
    rdd_typed_with_index_factory
from challenges.sectional.domain_logic.section_mutable_subtotal_type import (
    MutableStudent, MutableTrimester)
from challenges.sectional.section_test_data_types import (
    ClassLine, DataSet, LabeledTypedRow, StudentHeader, StudentSummary,
    TChallengePythonPysparkAnswer, TrimesterFooter, TrimesterHeader, TypedLine)
from utils.tidy_spark_session import TidySparkSession


def section_pyspark_rdd_mappart_odd_even(
        spark_session: TidySparkSession,
        data_set: DataSet,
) -> TChallengePythonPysparkAnswer:
    if data_set.description.num_students > pow(10, 7-1):
        # unreliable
        return "infeasible"
    sectionMaximum = data_set.data.section_maximum
    filename = data_set.data.test_filepath
    TargetNumPartitions = data_set.data.target_num_partitions

    SegmentOffset = sectionMaximum - 1
    SegmentExtra = 2 * sectionMaximum
    SegmentSize = SegmentOffset + sectionMaximum - 1 + SegmentExtra
    rddTypedWithIndex = rdd_typed_with_index_factory(
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

    rddParallelMapPartitionsInter = rddSegments.mapPartitions(aggregate)
    rddParallelMapPartitions = (
        rddParallelMapPartitionsInter
        .keyBy(lambda x: (x.StudentId, x.SourceLines))
        .repartitionAndSortWithinPartitions(
            numPartitions=TargetNumPartitions,
            partitionFunc=lambda x: x[0])
        .map(lambda x: x[1])
        .mapPartitions(choose_complete_section)
        .sortBy(lambda x: x.StudentId)
    )
    return rddParallelMapPartitions


def aggregate(
        iterator: Iterable[LabeledTypedRow],
) -> Iterable[StudentSummary]:
    student = None
    trimester = None
    prevIndex = -1
    for labeled_row in iterator:
        index = labeled_row.Index
        if prevIndex + 1 != index:
            if student is not None:
                yield student.grade_summary()
                student = None
        prevIndex = index
        rec = labeled_row.Value
        complete_student, student, trimester = accumulate_one_line(rec, student, trimester)
        if complete_student is not None:
            yield complete_student
    if student is not None:
        yield student.grade_summary()


def accumulate_one_line(
        rec: TypedLine,
        student: Optional[MutableStudent],
        trimester: Optional[MutableTrimester],
) -> tuple[Optional[StudentSummary], Optional[MutableStudent], Optional[MutableTrimester]]:
    complete_student: Optional[StudentSummary] = None
    if isinstance(rec, StudentHeader):
        if student is not None:
            complete_student = student.grade_summary()
        student = MutableStudent(rec.StudentId, rec.StudentName)
    elif student is None:  # since the section may be split
        pass
    elif isinstance(rec, TrimesterHeader):
        trimester = MutableTrimester(rec.Date, rec.WasAbroad)
    elif trimester is None:  # since the section may be split
        pass
    elif isinstance(rec, ClassLine):
        trimester.add_class(rec.Dept, rec.Credits, rec.Grade)
    elif isinstance(rec, TrimesterFooter):
        trimester.add_footer(rec.Major, rec.GPA, rec.Credits)
        student.add_trimester(trimester)
        trimester = None
    else:
        raise Exception("Unknown parsed row type")
    return complete_student, student, trimester


def choose_complete_section(
        iterator: Iterable[StudentSummary]
) -> Iterable[StudentSummary]:
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
