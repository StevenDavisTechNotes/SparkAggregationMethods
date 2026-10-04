import hashlib
import math
import os
import typing
from dataclasses import dataclass

from spark_agg_methods_common_python.perf_test_common import (
    LOCAL_TEST_DATA_FILE_LOCATION, DataSetDescriptionBase, ExecutionParametersBase,
)
from spark_agg_methods_common_python.utils.utils import always_true

MAX_EXPONENT = 5
DEDUPE_SOURCE_CODES: list[str] = ['A', 'B', 'C', 'D', 'E', 'F']


class DedupeDataSetDescription(DataSetDescriptionBase):
    num_people: int
    num_b_recs: int
    num_sources: int
    data_size_exponent: int

    def __init__(
            self,
            *,
            num_people: int,
            num_b_recs: int,
            num_sources: int,
    ):
        debugging_only = (num_people == 1)
        num_source_rows = (num_sources - 1) * num_people + num_b_recs
        logical_data_size = num_sources * num_people
        size_code = (
            str(logical_data_size)
            if logical_data_size < 1000 else
            f'{logical_data_size//1000}k'
        )
        super().__init__(
            debugging_only=debugging_only,
            num_source_rows=num_source_rows,
            size_code=size_code,
        )
        self.num_people = num_people
        self.num_b_recs = num_b_recs
        self.num_sources = num_sources
        self.data_size_exponent = round(math.log10(num_source_rows-num_b_recs))

    @classmethod
    def regressor_field_name(cls) -> str:
        regressor_field_name = "num_source_rows"
        assert regressor_field_name in typing.get_type_hints(cls)
        return regressor_field_name


@dataclass(frozen=True)
class DedupeDataSetBase:
    data_description: DedupeDataSetDescription


@dataclass(frozen=True)
class DedupeExecutionParametersBase(ExecutionParametersBase):
    in_cloud_mode: bool
    can_assume_no_dupes_per_partition: bool


DATA_SIZE_LIST_DEDUPE = [
    DedupeDataSetDescription(
        num_people=num_people,
        num_b_recs=num_b_recs,
        num_sources=num_sources,
    )
    for num_people in [10**x for x in range(0, MAX_EXPONENT + 1)]
    for num_sources in [2, 3, 6]
    if always_true(num_b_recs := max(1, 2 * num_people // 100))
]


def dedupe_derive_source_test_data_file_paths(
        data_description: DedupeDataSetDescription,
        *,
        temp_file: bool = False,
) -> dict[str, str]:
    root_path = os.path.join(
        LOCAL_TEST_DATA_FILE_LOCATION,
        "Dedupe_Test_Data",
    )
    num_people = data_description.num_people
    temp_postfix = "_temp" if temp_file else ""
    return {
        source_code: root_path
        + "/dedupe_field_%d" % (num_people)
        + "/dedupe_field_%s_%d%s.csv" % (source_code, num_people, temp_postfix)
        for source_code in DEDUPE_SOURCE_CODES
    }


def name_hash(
        i: int,
) -> str:
    return hashlib.sha512(str(i).encode('utf8')).hexdigest()
