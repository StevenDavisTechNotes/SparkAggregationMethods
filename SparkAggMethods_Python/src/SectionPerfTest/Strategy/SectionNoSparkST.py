from typing import List, Tuple

from pyspark import RDD
from pyspark.sql import DataFrame as spark_DataFrame
from SectionPerfTest.SectionMutuableSubtotal import aggregateTypedRowsToGrades

from Utils.SparkUtils import TidySparkSession

from SectionPerfTest.SectionLogic import parseLineToTypes
from SectionPerfTest.SectionTypeDefs import DataSet, StudentSummary


def section_nospark_logic(
    data_set: DataSet
) -> List[StudentSummary]:
    with open(data_set.data.test_filepath, "r") as fh:
        return list(aggregateTypedRowsToGrades(
            map(parseLineToTypes, fh)))


def section_nospark_single_threaded(
    _spark_session: TidySparkSession, data_set: DataSet
) -> Tuple[List[StudentSummary] | None, RDD | None, spark_DataFrame | None]:
    return section_nospark_logic(data_set), None, None
