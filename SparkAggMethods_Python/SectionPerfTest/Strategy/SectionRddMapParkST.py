from typing import List, Tuple

from pyspark import RDD
from pyspark.sql import DataFrame as spark_DataFrame

from Utils.SparkUtils import TidySparkSession

from ..SectionLogic import aggregateTypedRowsToGrades, parseLineToTypes
from ..SectionTypeDefs import DataSetDescription, StudentSummary


def method_mappart_single_threaded(
    spark_session: TidySparkSession, data_set: DataSetDescription
) -> Tuple[List[StudentSummary] | None, RDD | None, spark_DataFrame | None]:
    sc = spark_session.spark_context
    rdd = sc.textFile(data_set.filename, minPartitions=1)
    rdd = rdd \
        .map(parseLineToTypes) \
        .mapPartitions(aggregateTypedRowsToGrades)
    return None, rdd, None
