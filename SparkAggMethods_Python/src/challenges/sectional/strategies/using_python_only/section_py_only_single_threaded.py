from typing import Iterable

from src.challenges.sectional.domain_logic.section_data_parsers import parse_line_to_types
from src.challenges.sectional.domain_logic.section_mutable_subtotal_type import aggregate_typed_rows_to_grades
from src.challenges.sectional.section_test_data_types_pyspark import SectionDataSet, StudentSummary


def section_nospark_logic(
    data_set: SectionDataSet,
) -> Iterable[StudentSummary]:
    def read_file() -> Iterable[str]:
        with open(data_set.exec_params.source_data_file_path, "r") as fh:
            for line in fh:
                yield line

    parsed_iterable = map(parse_line_to_types, read_file())
    return aggregate_typed_rows_to_grades(parsed_iterable)


def section_py_only_single_threaded(
    data_set: SectionDataSet,
) -> list[StudentSummary]:
    return list(section_nospark_logic(data_set))
