import math
from typing import cast

from pyspark import RDD, StorageLevel

from challenges.sectional.domain_logic.section_data_parsers import (
    parse_line_to_types, rdd_typed_with_index_factory)
from challenges.sectional.domain_logic.section_snippet_subtotal_type import (
    CompletedStudent, StudentSnippet1, completed_from_snippet_1, grade_summary,
    merge_snippet_lists_1, student_snippet_from_typed_row_1)
from challenges.sectional.section_test_data_types import (
    DataSet, LabeledTypedRow, StudentSummary,
    TChallengePendingAnswerPythonPyspark)
from t_utils.non_commutative_tree_aggregate import \
    non_commutative_tree_aggregate
from t_utils.tidy_spark_session import TidySparkSession


def section_reduce_partials_broken(
        spark_session: TidySparkSession,
        data_set: DataSet,
) -> TChallengePendingAnswerPythonPyspark:
    dataSize = data_set.description.num_rows
    filename = data_set.data.test_filepath
    TargetNumPartitions = data_set.data.target_num_partitions
    MaximumProcessableSegment = data_set.exec_params.maximum_processable_segment

    rdd1: RDD[LabeledTypedRow] \
        = rdd_typed_with_index_factory(
        spark_session, filename, TargetNumPartitions)
    dataSize = rdd1.count()
    rdd2: RDD[list[StudentSnippet1]] = rdd1 \
        .map(lambda x: [student_snippet_from_typed_row_1(x.Index, x.Value)])
    targetDepth = max(1, math.ceil(
        math.log(dataSize / MaximumProcessableSegment, MaximumProcessableSegment - 2)))
    students1: list[StudentSnippet1] \
        = rdd2.treeAggregate(
        [], merge_snippet_lists_1, merge_snippet_lists_1, depth=targetDepth)
    students2: list[CompletedStudent] \
        = [completed_from_snippet_1(x) for x in students1]
    students3: list[StudentSummary] \
        = [grade_summary(x) for x in students2]
    return students3


def section_pyspark_rdd_reduce_asymm_part(
        spark_session: TidySparkSession,
        data_set: DataSet,
) -> TChallengePendingAnswerPythonPyspark:
    if data_set.description.num_students > pow(10, 7 - 1):
        # unreliable
        return "infeasible"
    sc = spark_session.spark_context
    data_size = data_set.description.num_rows
    file_path = data_set.data.test_filepath
    target_num_partitions = data_set.data.target_num_partitions
    maximum_processable_segment = data_set.exec_params.maximum_processable_segment
    rdd1: RDD[str] = sc.textFile(file_path, minPartitions=target_num_partitions)
    rdd2: RDD[tuple[str, int]] = rdd1.zipWithIndex()
    rdd13: RDD[LabeledTypedRow] = rdd2 \
        .map(lambda x: LabeledTypedRow(Index=x[1], Value=parse_line_to_types(x[0])))
    rdd4: RDD[list[StudentSnippet1]] = rdd13 \
        .map(lambda x: [student_snippet_from_typed_row_1(x.Index, x.Value)])
    division_base = 2
    target_depth = max(1, math.ceil(
        math.log(data_size / maximum_processable_segment, division_base)))
    rdd5: RDD[list[StudentSnippet1]] = non_commutative_tree_aggregate(
        rdd4,
        lambda: cast(list[StudentSnippet1], list()),
        merge_snippet_lists_1,
        merge_snippet_lists_1,
        depth=target_depth,
        division_base=division_base,
        storage_level=StorageLevel.DISK_ONLY,
    )
    rdd6: RDD[StudentSnippet1] = rdd5 \
        .flatMap(lambda x: x)
    rdd7 = rdd6 \
        .map(completed_from_snippet_1)
    rdd8: RDD[StudentSummary] = (
        rdd7
        .map(grade_summary)
        .sortBy(lambda x: x.StudentId)  # pyright: ignore[reportArgumentType]
    )
    return rdd8
