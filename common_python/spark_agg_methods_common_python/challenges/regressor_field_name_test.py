import typing
from typing import Any

import pytest
from spark_agg_methods_common_python.challenges.bi_level.bi_level_test_data_types import BiLevelDataSetDescription
from spark_agg_methods_common_python.challenges.conditional.conditional_test_data_types import (
    ConditionalDataSetDescription,
)
from spark_agg_methods_common_python.challenges.deduplication.dedupe_test_data_types import DedupeDataSetDescription
from spark_agg_methods_common_python.challenges.sectional.section_test_data_types import SectionDataSetDescription
from spark_agg_methods_common_python.challenges.vanilla.vanilla_test_data_types import VanillaDataSetDescription


@pytest.mark.parametrize(
    "description_type",
    [
        BiLevelDataSetDescription,
        ConditionalDataSetDescription,
        DedupeDataSetDescription,
        SectionDataSetDescription,
        VanillaDataSetDescription,
    ],
)
def test_regressor_field_name_is_a_declared_field_of_the_description(description_type: Any) -> None:
    regressor_field_name = description_type.regressor_field_name()
    assert regressor_field_name in typing.get_type_hints(description_type)
