from typing import Iterable, Tuple


from pyspark import RDD
from pyspark.sql import DataFrame as spark_DataFrame
from SectionPerfTest.SectionMutuableSubtotal import aggregateTypedRowsToGrades

from Utils.TidySparkSession import TidySparkSession

from SectionPerfTest.SectionLogic import parseLineToTypes
from SectionPerfTest.SectionTypeDefs import DataSet, StudentSummary


def section_nospark_logic(
    data_set: DataSet,
) -> Iterable[StudentSummary]:
    def read_file() -> Iterable[str]:
        with open(data_set.data.test_filepath, "r") as fh:
            for line in fh:
                yield line

    parsed_iterable = map(parseLineToTypes, read_file())
    return aggregateTypedRowsToGrades(parsed_iterable)


def section_nospark_single_threaded(
    _spark_session: TidySparkSession,
    data_set: DataSet,
) -> Tuple[Iterable[StudentSummary] | None, RDD | None, spark_DataFrame | None]:
    return section_nospark_logic(data_set), None, None
