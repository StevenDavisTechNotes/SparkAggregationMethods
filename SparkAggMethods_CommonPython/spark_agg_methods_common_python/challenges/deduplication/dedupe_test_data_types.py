import inspect
import math
from dataclasses import dataclass

from spark_agg_methods_common_python.perf_test_common import (
    DataSetDescriptionBase, ExecutionParametersBase)


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
        assert regressor_field_name in inspect.get_annotations(cls)
        return regressor_field_name


@dataclass(frozen=True)
class DedupeDataSetBase:
    data_description: DedupeDataSetDescription


@dataclass(frozen=True)
class DedupeExecutionParametersBase(ExecutionParametersBase):
    in_cloud_mode: bool
    can_assume_no_dupes_per_partition: bool
