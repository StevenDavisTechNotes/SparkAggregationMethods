import pyspark.sql.types as DataTypes

from src.six_field_test_data.six_test_data_types import (DataPointSchema,
                                                         DataSetDescription)

GROUP_BY_COLUMNS = ['grp']
AGGREGATION_COLUMNS = ['mean_of_C', 'max_of_D', 'avg_var_of_E', 'avg_var_of_E2']
RESULT_COLUMNS = GROUP_BY_COLUMNS + AGGREGATION_COLUMNS
postAggSchema = DataTypes.StructType(
    [x for x in DataPointSchema.fields if x.name in GROUP_BY_COLUMNS] +
    [DataTypes.StructField(name, DataTypes.DoubleType(), False) for name in AGGREGATION_COLUMNS])

DATA_SIZES_LIST_BI_LEVEL = [
    DataSetDescription(size_code='3_3_10', num_grp_1=3, num_grp_2=3, points_per_index=10**1),
    DataSetDescription(size_code='3_3_100k', num_grp_1=3, num_grp_2=3, points_per_index=10**5),
    DataSetDescription(size_code='3_30_10k', num_grp_1=3, num_grp_2=3 * 10**1, points_per_index=10**4),
    DataSetDescription(size_code='3_300_1k', num_grp_1=3, num_grp_2=3 * 10**2, points_per_index=10**3),
    DataSetDescription(size_code='3_3k_100', num_grp_1=3, num_grp_2=3 * 10**3, points_per_index=10**2),
]
