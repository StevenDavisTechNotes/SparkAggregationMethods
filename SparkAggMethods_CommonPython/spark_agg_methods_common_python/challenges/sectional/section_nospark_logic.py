from typing import Iterable

from spark_agg_methods_common_python.challenges.sectional.domain_logic.section_data_parsers import parse_line_to_types
from spark_agg_methods_common_python.challenges.sectional.domain_logic.section_mutable_subtotal_type import (
    aggregate_typed_rows_to_grades,
)
from spark_agg_methods_common_python.challenges.sectional.section_test_data_types import (
    SectionDataSetDescription, StudentSummary, section_derive_source_test_data_file_path,
)


def section_nospark_logic(
        *,
        data_description: SectionDataSetDescription,
) -> Iterable[StudentSummary]:
    def read_file() -> Iterable[str]:
        with open(section_derive_source_test_data_file_path(data_description), "r") as fh:
            for line in fh:
                yield line

    parsed_iterable = map(parse_line_to_types, read_file())
    return aggregate_typed_rows_to_grades(parsed_iterable)
