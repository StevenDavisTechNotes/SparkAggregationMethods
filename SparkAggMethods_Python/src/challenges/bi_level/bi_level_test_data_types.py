import inspect

import pyspark.sql.types as DataTypes

from src.six_field_test_data.six_test_data_types import DataPointSchema, SixTestDataSetDescription

GROUP_BY_COLUMNS = ['grp']
AGGREGATION_COLUMNS = ['mean_of_C', 'max_of_D', 'avg_var_of_E', 'avg_var_of_E2']
RESULT_COLUMNS = GROUP_BY_COLUMNS + AGGREGATION_COLUMNS
postAggSchema = DataTypes.StructType(
    [x for x in DataPointSchema.fields if x.name in GROUP_BY_COLUMNS] +
    [DataTypes.StructField(name, DataTypes.DoubleType(), False) for name in AGGREGATION_COLUMNS])


class BiLevelDataSetDescription(SixTestDataSetDescription):
    # for DataSetDescriptionBase
    debugging_only: bool
    num_source_rows: int
    size_code: str
    # for SixTestDataSetDescription
    num_grp_1: int
    num_grp_2: int
    points_per_index: int
    relative_cardinality_between_groupings: int

    def __init__(
            self,
            *,
            num_grp_1: int,
            num_grp_2: int,
            points_per_index: int,
            size_code: str,
    ):
        debugging_only = size_code == '3_3_10'
        super().__init__(
            # for DataSetDescriptionBase
            debugging_only=debugging_only,
            num_grp_1=num_grp_1,
            num_grp_2=num_grp_2,
            # for SixTestDataSetDescription
            points_per_index=points_per_index,
            size_code=size_code,
        )

    @classmethod
    def regressor_field_name(cls) -> str:
        regressor_field_name = "relative_cardinality_between_groupings"
        assert regressor_field_name in inspect.get_annotations(cls)
        return regressor_field_name


DATA_SIZES_LIST_BI_LEVEL = [
    BiLevelDataSetDescription(
        size_code='3_3_10', num_grp_1=3, num_grp_2=3, points_per_index=10**1),
    BiLevelDataSetDescription(
        size_code='3_3_100k', num_grp_1=3, num_grp_2=3, points_per_index=10**5),
    BiLevelDataSetDescription(
        size_code='3_30_10k', num_grp_1=3, num_grp_2=3 * 10 ** 1, points_per_index=10**4),
    BiLevelDataSetDescription(
        size_code='3_300_1k', num_grp_1=3, num_grp_2=3 * 10 ** 2, points_per_index=10**3),
    BiLevelDataSetDescription(
        size_code='3_3k_100', num_grp_1=3, num_grp_2=3 * 10 ** 3, points_per_index=10**2),
]
