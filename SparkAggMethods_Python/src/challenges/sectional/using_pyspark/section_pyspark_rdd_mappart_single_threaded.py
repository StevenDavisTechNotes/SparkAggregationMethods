from src.challenges.sectional.domain_logic.section_data_parsers import \
    parse_line_to_types
from src.challenges.sectional.domain_logic.section_mutable_subtotal_type import \
    aggregate_typed_rows_to_grades
from src.challenges.sectional.section_record_runs import \
    MAXIMUM_PROCESSABLE_SEGMENT_EXPONENT
from src.challenges.sectional.section_test_data_types import (
    DataSet, TChallengePythonPysparkAnswer)
from src.utils.tidy_spark_session import TidySparkSession


def section_pyspark_rdd_mappart_single_threaded(
        spark_session: TidySparkSession,
        data_set: DataSet,
) -> TChallengePythonPysparkAnswer:
    if data_set.data_size.num_students > pow(10, MAXIMUM_PROCESSABLE_SEGMENT_EXPONENT-1):
        # unreliable
        return "infeasible"
    sc = spark_session.spark_context
    if data_set.data_size.num_rows > data_set.exec_params.maximum_processable_segment:
        raise ValueError("Single thread mapPartitions is limited to 1 segment")
    rdd = sc.textFile(data_set.data.test_filepath, minPartitions=1)
    rdd = (
        rdd
        .map(parse_line_to_types)
        .mapPartitions(aggregate_typed_rows_to_grades)
    )
    return rdd
