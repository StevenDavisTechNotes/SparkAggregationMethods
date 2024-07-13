from typing import Iterable

from src.challenges.sectional.domain_logic.section_data_parsers import \
    parse_line_to_types
from src.challenges.sectional.domain_logic.section_mutable_subtotal_type import \
    aggregate_typed_rows_to_grades
from src.challenges.sectional.section_test_data_types import (
    DataSet, StudentSummary, TChallengePythonPysparkAnswer)
from src.utils.tidy_spark_session import TidySparkSession


def section_nospark_logic(
    data_set: DataSet,
) -> Iterable[StudentSummary]:
    def read_file() -> Iterable[str]:
        with open(data_set.data.test_filepath, "r") as fh:
            for line in fh:
                yield line

    parsed_iterable = map(parse_line_to_types, read_file())
    return aggregate_typed_rows_to_grades(parsed_iterable)


def section_nospark_single_threaded(
    spark_session: TidySparkSession,
    data_set: DataSet,
) -> TChallengePythonPysparkAnswer:
    return list(section_nospark_logic(data_set))
