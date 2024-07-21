from typing import cast

from dask.dataframe.core import DataFrame as DaskDataFrame
from dask_sql import Context

from src.six_field_test_data.six_generate_test_data import (
    DataSetDask, TChallengeAnswerPythonDask)
from src.six_field_test_data.six_test_data_types import ExecutionParameters

# pyright: reportArgumentType=false
# pyright: reportCallIssue=false


def vanilla_dask_sql_no_gpu(
        exec_params: ExecutionParameters,
        data_set: DataSetDask,
) -> TChallengeAnswerPythonDask:
    df = data_set.data.df_src
    return inner(df, gpu=False)


def inner(df: DaskDataFrame, gpu: bool) -> DaskDataFrame:
    c = Context()
    df = cast(DaskDataFrame,
              df.rename(columns={
                  "A": "a",
                  "B": "b",
                  "C": "c",
                  "D": "d",
                  "E": "e",
                  "F": "f",
              }))
    c.create_table(
        gpu=gpu,
        input_table=df,
        persist=False,
        table_name="example_data",
    )
    result = c.sql(
        sql="""
        SELECT
            grp, subgrp, AVG(C) mean_of_C, MAX(D) max_of_D,
            VAR_POP(E) var_of_E,
            (
                SUM(E * E) / COUNT(E) -
                POWER(SUM(E) / COUNT(E), 2)
            ) var_of_E2
        FROM
            example_data
        GROUP BY grp, subgrp
        ORDER BY grp, subgrp
    """,
        gpu=gpu,
    )
    df = cast(DaskDataFrame, result.compute())
    df = cast(DaskDataFrame,
              df.rename(columns={
                  "mean_of_c": "mean_of_C",
                  "max_of_d": "max_of_D",
                  "var_of_e": "var_of_E",
                  "var_of_e2": "var_of_E2",
              }))
    return df
