from typing import List, Tuple, Optional

from dataclasses import astuple

import numpy
import pandas as pd

from pyspark import RDD
from pyspark.sql import DataFrame as spark_DataFrame

from Utils.SparkUtils import cast_from_pd_dataframe, TidySparkSession

from ..VanillaTestData import DataPoint, DataPointSchema, groupby_columns, agg_columns, postAggSchema


def vanilla_pandas(
    spark_session: TidySparkSession, pyData: List[DataPoint]
) -> Tuple[Optional[RDD], Optional[spark_DataFrame]]:

    def inner_agg_method(dfPartition: pd.DataFrame) -> pd.DataFrame:
        group_key = dfPartition['grp'].iloc[0]
        subgroup_key = dfPartition['subgrp'].iloc[0]
        C = dfPartition['C']
        D = dfPartition['D']
        E = dfPartition['E']
        var_of_E2 = (
            ((E * E).sum() - E.sum()**2/E.count())/(E.count()-1)
            if E.count() > 1 else numpy.nan)
        return pd.DataFrame([[
            group_key,
            subgroup_key,
            C.mean(),
            D.max(),
            E.var(),
            var_of_E2,
        ]], columns=groupby_columns + agg_columns)

    df = spark_session.spark.createDataFrame(
        map(lambda x: astuple(x), pyData), schema=DataPointSchema)
    aggregates = (
        cast_from_pd_dataframe(
            df.groupby(df.grp, df.subgrp)
        ).applyInPandas(inner_agg_method, postAggSchema))
    return None, aggregates
