import pandas as pd

from SixFieldCommon.PySpark_SixFieldTestData import (
    PysparkDataSet, PysparkPythonPendingAnswerSet)
from SixFieldCommon.SixFieldTestData import ExecutionParameters
from Utils.TidySparkSession import TidySparkSession
from VanillaPerfTest.VanillaDataTypes import (pyspark_post_agg_schema,
                                              result_columns)


def vanilla_pandas(
        spark_session: TidySparkSession,
        _exec_params: ExecutionParameters,
        data_set: PysparkDataSet
) -> PysparkPythonPendingAnswerSet:
    df = data_set.data.dfSrc

    df = (
        df
        .groupBy(df.grp, df.subgrp)
        .applyInPandas(inner_agg_method, pyspark_post_agg_schema)
    )
    df = df.orderBy(df.grp, df.subgrp)
    return PysparkPythonPendingAnswerSet(spark_df=df)


def inner_agg_method(
        dfPartition: pd.DataFrame,
) -> pd.DataFrame:
    group_key = dfPartition['grp'].iloc[0]
    subgroup_key = dfPartition['subgrp'].iloc[0]
    C = dfPartition['C']
    D = dfPartition['D']
    E = dfPartition['E']
    var_of_E2 = (
        (E * E).sum() / E.count()
        - (E.sum() / E.count())**2)
    return pd.DataFrame([[
        group_key,
        subgroup_key,
        C.mean(),
        D.max(),
        E.var(ddof=0),
        var_of_E2,
    ]], columns=result_columns)
