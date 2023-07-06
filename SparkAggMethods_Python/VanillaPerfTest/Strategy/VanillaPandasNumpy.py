from typing import Optional, Tuple

import numpy
import pandas as pd
from pyspark import RDD
from pyspark.sql import DataFrame as spark_DataFrame

from SixFieldTestData import DataSet, ExecutionParameters
from Utils.SparkUtils import TidySparkSession

from ..VanillaDataTypes import agg_columns, groupby_columns, postAggSchema


def vanilla_pandas_numpy(
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
        return pd.DataFrame([[
            group_key,
            subgroup_key,
            numpy.mean(C),
            numpy.max(D),
            numpy.var(E),
            (numpy.inner(E, E) - numpy.sum(E)**2 / E.count()) / (E.count() - 1)
            if E.count() > 1 else numpy.nan,
        ]], columns=groupby_columns + agg_columns)

    df = data_set.dfSrc
    aggregates = (
        df.groupby(df.grp, df.subgrp)
        .applyInPandas(inner_agg_method, postAggSchema)
    )
    return None, aggregates
