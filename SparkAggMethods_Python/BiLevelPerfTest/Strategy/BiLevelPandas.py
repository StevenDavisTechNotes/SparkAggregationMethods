from typing import List, Optional, Tuple

import pandas as pd
import pyspark.sql.types as DataTypes
from pyspark import RDD
from pyspark.sql import DataFrame as spark_DataFrame
from pyspark.sql.pandas.functions import PandasUDFType, pandas_udf

from Utils.SparkUtils import TidySparkSession

from ..BiLevelTestData import DataPoint, DataPointSchema


def bi_pandas(
    spark_session: TidySparkSession, pyData: List[DataPoint]
) -> Tuple[Optional[RDD], Optional[spark_DataFrame]]:
    spark = spark_session.spark
    # df = spark.createDataFrame(
    #     map(lambda x: astuple(x), pyData), schema=DataPointSchema)
    groupby_columns = ['grp']
    agg_columns = ['mean_of_C', 'max_of_D', 'avg_var_of_E', 'avg_var_of_E2']
    df = spark.createDataFrame(pyData)
    postAggSchema = DataTypes.StructType(
        [x for x in DataPointSchema.fields if x.name in groupby_columns] +
        [DataTypes.StructField(name, DataTypes.DoubleType(), False) for name in agg_columns])
    #

    @pandas_udf(postAggSchema, PandasUDFType.GROUPED_MAP)
    def inner_agg_method(dfPartition):
        group_key = dfPartition['grp'].iloc[0]
        C = dfPartition['C']
        D = dfPartition['D']
        E = dfPartition['E']
        subgroupedE = dfPartition.groupby('subgrp')['E']
        return pd.DataFrame([[
            group_key,
            C.mean(),
            D.max(),
            subgroupedE.var().mean(),
            subgroupedE
            .agg(lambda E:
                 ((E * E).sum() -
                     E.sum()**2/E.count())/(E.count()-1))
            .mean(),
        ]], columns=groupby_columns + agg_columns)
    #
    aggregates = df.groupby(df.grp).apply(inner_agg_method)
    return None, aggregates
