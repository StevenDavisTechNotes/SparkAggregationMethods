from typing import List, Tuple, Optional

import pandas as pd

from pyspark import RDD
from pyspark.sql import SparkSession, DataFrame as spark_DataFrame
import pyspark.sql.types as DataTypes

from Utils.SparkUtils import cast_from_pd_dataframe

from .VanillaTestData import (
    DataPoint, DataPointSchema,
    cast_data_points_to_tuples,
)


def vanilla_pandas(
    spark: SparkSession, pyData: List[DataPoint]
) -> Tuple[Optional[RDD], Optional[spark_DataFrame]]:
    df = spark.createDataFrame(
        cast_data_points_to_tuples(pyData),
        schema=DataPointSchema)

    groupby_columns = ['grp', 'subgrp']
    agg_columns = ['mean_of_C', 'max_of_D', 'var_of_E', 'var_of_E2']
    postAggSchema = DataTypes.StructType(
        [x for x in DataPointSchema.fields if x.name in groupby_columns] +
        [DataTypes.StructField(name, DataTypes.DoubleType(), False) for name in agg_columns])

    def inner_agg_method(dfPartition: pd.DataFrame) -> pd.DataFrame:
        group_key = dfPartition['grp'].iloc[0]
        subgroup_key = dfPartition['subgrp'].iloc[0]
        C = dfPartition['C']
        D = dfPartition['D']
        E = dfPartition['E']
        return pd.DataFrame([[
            group_key,
            subgroup_key,
            C.mean(),
            D.max(),
            E.var(),
            ((E * E).sum() - E.sum()**2/E.count())/(E.count()-1),
        ]], columns=groupby_columns + agg_columns)

    aggregates = (
        cast_from_pd_dataframe(
            df.groupby(df.grp, df.subgrp)
        )        .applyInPandas(inner_agg_method, postAggSchema))
    return None, aggregates
