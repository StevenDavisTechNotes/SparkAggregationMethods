import pandas as pd

from BiLevelPerfTest.BiLevelDataTypes import postAggSchema, result_columns
from SixFieldCommon.PySpark_SixFieldTestData import (
    PysparkDataSet, PysparkPythonPendingAnswerSet)
from SixFieldCommon.SixFieldTestData import ExecutionParameters
from Utils.TidySparkSession import TidySparkSession


def bi_pandas(
        _spark_session: TidySparkSession,
        _exec_params: ExecutionParameters,
        data_set: PysparkDataSet
) -> PysparkPythonPendingAnswerSet:
    df = data_set.data.dfSrc

    df = (
        df
        .groupBy(df.grp)
        .applyInPandas(inner_agg_method, postAggSchema)
    )
    df = df.orderBy(df.grp)
    return PysparkPythonPendingAnswerSet(spark_df=df)


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
