from typing import cast

import pandas as pd

from ConditionalPerfTest.CondDataTypes import (agg_columns_4, groupby_columns,
                                               postAggSchema_4)
from SixFieldCommon.PySpark_SixFieldTestData import (
    PysparkDataSet, PysparkPythonPendingAnswerSet)
from SixFieldCommon.SixFieldTestData import ExecutionParameters
from Utils.PandasHelpers import PandasSeriesOfFloat, PandasSeriesOfInt
from Utils.TidySparkSession import TidySparkSession


def cond_pandas(
        spark_session: TidySparkSession,
        _exec_params: ExecutionParameters,
        data_set: PysparkDataSet,
) -> PysparkPythonPendingAnswerSet:
    df = data_set.data.dfSrc

    df = (
        df
        .groupBy(df.grp, df.subgrp)
        .applyInPandas(inner_agg_method, postAggSchema_4)
    )
    df = df.orderBy(df.grp, df.subgrp)
    return PysparkPythonPendingAnswerSet(spark_df=df)


def my_var(
        E: PandasSeriesOfFloat
) -> float:
    return (
        (E * E).sum()  # type: ignore
        / E.count() -
        (
            E.sum()   # type: ignore
            / E.count())**2
    )


def inner_agg_method(
        dfPartition: pd.DataFrame,
) -> pd.DataFrame:
    group_key = cast(PandasSeriesOfInt, dfPartition['grp']).iloc[0]
    subgroup_key = cast(PandasSeriesOfInt, dfPartition['subgrp']).iloc[0]
    C = cast(PandasSeriesOfFloat, dfPartition['C'])
    D = cast(PandasSeriesOfFloat, dfPartition['D'])
    negE = cast(PandasSeriesOfFloat, dfPartition[dfPartition["E"] < 0]['E'])
    return pd.DataFrame([[
        group_key,
        subgroup_key,
        C.mean(),  # type: ignore
        D.max(),  # type: ignore
        negE.var(ddof=0),  # type: ignore
        negE.agg(my_var),  # type: ignore
    ]], columns=groupby_columns + agg_columns_4)
