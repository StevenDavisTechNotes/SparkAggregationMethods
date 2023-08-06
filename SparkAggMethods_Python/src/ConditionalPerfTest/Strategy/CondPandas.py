from typing import Tuple

import pandas as pd
from pyspark import RDD
from pyspark.sql import DataFrame as spark_DataFrame

from SixFieldCommon.SixFieldTestData import DataSet, ExecutionParameters
from Utils.SparkUtils import TidySparkSession

from ConditionalPerfTest.CondDataTypes import agg_columns_4, groupby_columns, postAggSchema_4


def cond_pandas(
    spark_session: TidySparkSession,
    _exec_params: ExecutionParameters,
    data_set: DataSet,
) -> Tuple[RDD | None, spark_DataFrame | None]:
    df = data_set.data.dfSrc

    def my_var(E: pd.Series):
        return (
            (E * E).sum() / E.count() -
            (E.sum() / E.count())**2
        )

    def inner_agg_method(dfPartition: pd.DataFrame):
        group_key = dfPartition['grp'].iloc[0]
        subgroup_key = dfPartition['subgrp'].iloc[0]
        C = dfPartition['C']
        D = dfPartition['D']
        negE = dfPartition[dfPartition.E < 0]['E']
        return pd.DataFrame([[
            group_key,
            subgroup_key,
            C.mean(),
            D.max(),
            negE.var(ddof=0),
            negE.agg(my_var),
        ]], columns=groupby_columns + agg_columns_4)

    df = (
        df
        .groupBy(df.grp, df.subgrp)
        .applyInPandas(inner_agg_method, postAggSchema_4)
    )
    df = df.orderBy(df.grp, df.subgrp)
    return None, df
