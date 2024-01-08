from challenges.sectional.domain_logic.section_data_parsers import \
    parse_line_to_types
from challenges.sectional.domain_logic.section_mutuable_subtotal_type import \
    aggregate_typed_rows_to_grades
from challenges.sectional.section_record_runs import \
    MAXIMUM_PROCESSABLE_SEGMENT_EXPONENT
from challenges.sectional.section_test_data_types import (
    DataSet, PysparkPythonPendingAnswerSet)
from utils.tidy_spark_session import TidySparkSession


def section_pyspark_rdd_mappart_single_threaded(
        spark_session: TidySparkSession,
        data_set: DataSet,
) -> PysparkPythonPendingAnswerSet:
    if data_set.description.num_students > pow(10, MAXIMUM_PROCESSABLE_SEGMENT_EXPONENT-1):
        # unreliable
        return PysparkPythonPendingAnswerSet(feasible=False)
    sc = spark_session.spark_context
    if data_set.description.num_rows > data_set.exec_params.MaximumProcessableSegment:
        raise ValueError("Single thread mapPartitions is limited to 1 segment")
    rdd = sc.textFile(data_set.data.test_filepath, minPartitions=1)
    rdd = (
        rdd
        .map(parse_line_to_types)
        .mapPartitions(aggregate_typed_rows_to_grades)
    )
    return PysparkPythonPendingAnswerSet(rdd_tuple=rdd)
