from typing import List, Tuple

from pyspark import RDD
from pyspark.sql import DataFrame as spark_DataFrame

from Utils.SparkUtils import TidySparkSession

from ..SectionLogic import aggregateTypedRowsToGrades, parseLineToTypes
from ..SectionTypeDefs import DataSetDescription, StudentSummary


def method_nospark_single_threaded(
    _spark_session: TidySparkSession, data_set: DataSetDescription
) -> Tuple[List[StudentSummary] | None, RDD | None, spark_DataFrame | None]:
    with open(data_set.filename, "r") as fh:
        return list(aggregateTypedRowsToGrades(
            map(parseLineToTypes, fh))), None, None
