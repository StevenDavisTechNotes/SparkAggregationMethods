import math
from typing import List, Tuple, cast

from pyspark import RDD, StorageLevel
from pyspark.sql import DataFrame as spark_DataFrame

from challenges.sectional.domain_logic.section_data_parsers import (
    parse_line_to_types, rdd_typed_with_index_factory)
from challenges.sectional.domain_logic.section_snippet_subtotal_type import (
    CompletedStudent, StudentSnippet1, completed_from_snippet_1, grade_summary,
    merge_snippet_lists_1, student_snippet_from_typed_row_1)
from challenges.sectional.section_test_data_types import (DataSet,
                                                          LabeledTypedRow,
                                                          StudentSummary)
from utils.non_commutative_tree_aggregate import non_commutative_tree_aggregate
from utils.tidy_spark_session import TidySparkSession


def section_reduce_partials_broken(
        spark_session: TidySparkSession,
        data_set: DataSet,
) -> Tuple[List[StudentSummary] | None, RDD | None, spark_DataFrame | None]:
    dataSize = data_set.description.num_rows
    filename = data_set.data.test_filepath
    TargetNumPartitions = data_set.data.target_num_partitions
    MaximumProcessableSegment = data_set.exec_params.MaximumProcessableSegment

    rdd1: RDD[LabeledTypedRow] \
        = rdd_typed_with_index_factory(
        spark_session, filename, TargetNumPartitions)
    dataSize = rdd1.count()
    rdd2: RDD[List[StudentSnippet1]] = rdd1 \
        .map(lambda x: [student_snippet_from_typed_row_1(x.Index, x.Value)])
    targetDepth = max(1, math.ceil(
        math.log(dataSize / MaximumProcessableSegment, MaximumProcessableSegment - 2)))
    students1: List[StudentSnippet1] \
        = rdd2.treeAggregate(
        [], merge_snippet_lists_1, merge_snippet_lists_1, depth=targetDepth)
    students2: List[CompletedStudent] \
        = [completed_from_snippet_1(x) for x in students1]
    students3: List[StudentSummary] \
        = [grade_summary(x) for x in students2]
    return students3, None, None


def section_pyspark_rdd_reduce_asymm_part(
        spark_session: TidySparkSession,
        data_set: DataSet,
) -> Tuple[List[StudentSummary] | None, RDD | None, spark_DataFrame | None]:
    sc = spark_session.spark_context
    dataSize = data_set.description.num_rows
    filename = data_set.data.test_filepath
    TargetNumPartitions = data_set.data.target_num_partitions
    MaximumProcessableSegment = data_set.exec_params.MaximumProcessableSegment
    rdd1: RDD[str] = sc.textFile(filename, minPartitions=TargetNumPartitions)
    rdd2: RDD[Tuple[str, int]] = rdd1.zipWithIndex()
    rdd13: RDD[LabeledTypedRow] = rdd2 \
        .map(lambda x: LabeledTypedRow(Index=x[1], Value=parse_line_to_types(x[0])))
    rdd4: RDD[List[StudentSnippet1]] = rdd13 \
        .map(lambda x: [student_snippet_from_typed_row_1(x.Index, x.Value)])
    divisionBase = 2
    targetDepth = max(1, math.ceil(
        math.log(dataSize / MaximumProcessableSegment, divisionBase)))
    rdd5: RDD[List[StudentSnippet1]] = non_commutative_tree_aggregate(
        rdd4,
        lambda: cast(List[StudentSnippet1], list()),
        merge_snippet_lists_1,
        merge_snippet_lists_1,
        depth=targetDepth,
        divisionBase=divisionBase,
        storage_level=StorageLevel.DISK_ONLY,
    )
    rdd6: RDD[StudentSnippet1] = rdd5 \
        .flatMap(lambda x: x)
    rdd7 = rdd6 \
        .map(completed_from_snippet_1)
    rdd8: RDD[StudentSummary] = (
        rdd7
        .map(grade_summary)
        .sortBy(lambda x: x.StudentId)  # pyright: ignore[reportGeneralTypeIssues]
    )
    return None, rdd8, None
