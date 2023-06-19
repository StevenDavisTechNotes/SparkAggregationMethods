from typing import List, Tuple, Optional

import pandas as pd
import numpy

from pyspark import RDD
from pyspark.sql import SparkSession, DataFrame as spark_DataFrame
import pyspark.sql.types as DataTypes

from Utils.SparkUtils import cast_from_pd_dataframe

from ..VanillaTestData import DataPointAsTuple, DataPointSchema


def vanilla_pandas_numpy(
    spark: SparkSession, pyData: List[DataPointAsTuple]
) -> Tuple[Optional[RDD], Optional[spark_DataFrame]]:
    df = spark.createDataFrame(pyData, schema=DataPointSchema)

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
            numpy.mean(C),
            numpy.max(D),
            numpy.var(E),
            (numpy.inner(E, E) - numpy.sum(E)**2/E.count())/(E.count()-1),
        ]], columns=groupby_columns + agg_columns)

    aggregates = (
        cast_from_pd_dataframe(
            df.groupby(df.grp, df.subgrp))
        .applyInPandas(
            inner_agg_method, postAggSchema)
    )
    return None, aggregates
