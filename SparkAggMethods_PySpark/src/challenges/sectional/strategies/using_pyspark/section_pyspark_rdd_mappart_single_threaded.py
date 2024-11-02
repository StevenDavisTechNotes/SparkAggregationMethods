from spark_agg_methods_common_python.challenges.sectional.domain_logic.section_mutable_subtotal_type import (
    aggregate_typed_rows_to_grades,
)

from src.challenges.sectional.domain_logic.section_data_parsers_pyspark import parse_line_to_types
from src.challenges.sectional.section_test_data_types_pyspark import (
    MAXIMUM_PROCESSABLE_SEGMENT_EXPONENT, SectionDataSetPyspark, SectionExecutionParametersPyspark,
    TChallengePythonPysparkAnswer,
)
from src.utils.tidy_session_pyspark import TidySparkSession


def section_pyspark_rdd_mappart_single_threaded(
        spark_session: TidySparkSession,
        exec_params: SectionExecutionParametersPyspark,
        data_set: SectionDataSetPyspark,
) -> TChallengePythonPysparkAnswer:
    if data_set.data_description.num_students > pow(10, MAXIMUM_PROCESSABLE_SEGMENT_EXPONENT-1):
        # unreliable
        return "infeasible"
    sc = spark_session.spark_context
    if data_set.data_description.num_source_rows > exec_params.maximum_processable_segment:
        raise ValueError("Single thread mapPartitions is limited to 1 segment")
    rdd = sc.textFile(data_set.source_data_file_path, minPartitions=1)
    rdd = (
        rdd
        .map(parse_line_to_types)
        .mapPartitions(aggregate_typed_rows_to_grades)
    )
    return rdd
