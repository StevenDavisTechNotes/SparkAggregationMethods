from typing import Optional, Tuple

import pandas as pd
from pyspark import RDD
from pyspark.sql import DataFrame as spark_DataFrame

from BiLevelPerfTest.BiLevelDataTypes import postAggSchema, result_columns
from SixFieldCommon.PySpark_SixFieldTestData import PysparkDataSet
from SixFieldCommon.SixFieldTestData import ExecutionParameters
from Utils.TidySparkSession import TidySparkSession


def bi_pandas(
        _spark_session: TidySparkSession,
        _exec_params: ExecutionParameters,
        data_set: PysparkDataSet
) -> Tuple[Optional[RDD], Optional[spark_DataFrame]]:
    df = data_set.data.dfSrc

    df = (
        df
        .groupBy(df.grp)
        .applyInPandas(inner_agg_method, postAggSchema)
    )
    df = df.orderBy(df.grp)
    return None, df


def inner_agg_method(
        dfPartition: pd.DataFrame
) -> pd.DataFrame:
    group_key = dfPartition['grp'].iloc[0]
    C = dfPartition['C']
    D = dfPartition['D']
    subgroupedE = dfPartition.groupby('subgrp')['E']
    return pd.DataFrame([[
        group_key,
        C.mean(),
        D.max(),
        subgroupedE.var(ddof=0).mean(),
        subgroupedE
        .agg(lambda E:
             ((E * E).sum() / E.count() -
              (E.sum() / E.count())**2))
        .mean(),
    ]], columns=result_columns)
