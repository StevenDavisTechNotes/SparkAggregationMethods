from typing import List, Tuple

from pyspark import RDD
from pyspark.sql import DataFrame as spark_DataFrame

from Utils.SparkUtils import TidySparkSession

from SectionPerfTest.SectionLogic import aggregateTypedRowsToGrades, parseLineToTypes
from SectionPerfTest.SectionTypeDefs import DataSet, StudentSummary


def section_mappart_single_threaded(
    spark_session: TidySparkSession, data_set: DataSet
) -> Tuple[List[StudentSummary] | None, RDD | None, spark_DataFrame | None]:
    sc = spark_session.spark_context
    if data_set.description.num_rows > data_set.exec_params.MaximumProcessableSegment:
        raise ValueError("Single thread mapPartitions is limited to 1 segment")
    rdd = sc.textFile(data_set.data.test_filepath, minPartitions=1)
    rdd = (
        rdd
        .map(parseLineToTypes)
        .mapPartitions(aggregateTypedRowsToGrades)
    )
    return None, rdd, None
