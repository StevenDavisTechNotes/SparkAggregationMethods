import typing

from spark_agg_methods_common_python.challenges.six_field_test_data.six_test_data_types import SixTestDataSetDescription

GROUP_BY_COLUMNS: list[str] = ['grp', 'subgrp']
AGGREGATION_COLUMNS_NON_NULL: list[str] = ['mean_of_C', 'max_of_D']
AGGREGATION_COLUMNS_NULLABLE: list[str] = ['var_of_E', 'var_of_E2']
AGGREGATION_COLUMNS: list[str] = AGGREGATION_COLUMNS_NON_NULL + AGGREGATION_COLUMNS_NULLABLE
VANILLA_RESULT_COLUMNS: list[str] = GROUP_BY_COLUMNS + AGGREGATION_COLUMNS


class VanillaDataSetDescription(SixTestDataSetDescription):
    pass

    def __init__(
            self,
            debugging_only: bool,
            num_grp_1: int,
            num_grp_2: int,
            points_per_index: int,
            size_code: str,
    ):
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
        regressor_field_name = "num_source_rows"
        assert regressor_field_name in typing.get_type_hints(cls)
        return regressor_field_name


DATA_SIZES_LIST_VANILLA = [
    VanillaDataSetDescription(
        size_code=code,
        num_grp_1=3,
        num_grp_2=3,
        points_per_index=scale,
        debugging_only=False,
    )
    for scale, code in [
        (10 ** 0, '3_3_1'),
        (10 ** 1, '3_3_10'),
        (10 ** 2, '3_3_100'),
        (10 ** 3, '3_3_1k'),
        (10 ** 4, '3_3_10k'),
        (10 ** 5, '3_3_100k'),
        (10 ** 6, '3_3_1m'),  # load data EOM ever since numpy.array is used for the answer generation
        (10 ** 7, '3_3_10m'),  # load data EOM ever since numpy.array is used for the answer generation
        (10 ** 8, '3_3_100m'),  # load data EOM ever since numpy.array is used for the answer generation
    ]
]
