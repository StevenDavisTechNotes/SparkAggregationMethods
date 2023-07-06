from typing import Optional, Tuple

import numpy
import pandas as pd
from pyspark import RDD
from pyspark.sql import DataFrame as spark_DataFrame

from SixFieldTestData import DataSet, ExecutionParameters
from Utils.SparkUtils import TidySparkSession

from ..VanillaDataTypes import agg_columns, groupby_columns, postAggSchema


def vanilla_pandas(
    spark_session: TidySparkSession,
    _exec_params: ExecutionParameters,
    data_set: DataSet
) -> Tuple[Optional[RDD], Optional[spark_DataFrame]]:
    df = data_set.dfSrc

    def inner_agg_method(dfPartition: pd.DataFrame) -> pd.DataFrame:
        group_key = dfPartition['grp'].iloc[0]
        subgroup_key = dfPartition['subgrp'].iloc[0]
        C = dfPartition['C']
        D = dfPartition['D']
        E = dfPartition['E']
        var_of_E2 = (
            ((E * E).sum() - E.sum()**2 / E.count()) / (E.count() - 1)
            if E.count() > 1 else numpy.nan)
        return pd.DataFrame([[
            group_key,
            subgroup_key,
            C.mean(),
            D.max(),
            E.var(),
            var_of_E2,
        ]], columns=groupby_columns + agg_columns)

    aggregates = (
        df
        .groupby(df.grp, df.subgrp)
        .applyInPandas(inner_agg_method, postAggSchema)
    )
    return None, aggregates
