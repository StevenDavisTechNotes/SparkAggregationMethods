from typing import cast

from dask.dataframe.core import DataFrame as DaskDataFrame
from dask_sql import Context
from spark_agg_methods_common_python.challenges.six_field_test_data.six_test_data_types import (
    SixTestExecutionParameters,
)

from src.challenges.six_field_test_data.six_test_data_for_dask import SixTestDataSetDask, TChallengeAnswerPythonDask

# pyright: reportArgumentType=false
# pyright: reportCallIssue=false


def bi_level_dask_sql_temp_join_no_gpu(
        exec_params: SixTestExecutionParameters,
        data_set: SixTestDataSetDask,
) -> TChallengeAnswerPythonDask:
    df = data_set.data.open_source_data_as_ddf()
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
    c.sql(
        sql="""
        CREATE TABLE level1 AS (
            SELECT
                grp, AVG(C) mean_of_C, MAX(D) max_of_D
            FROM
                example_data
            GROUP BY grp
        )
    """,
        gpu=gpu,
    )
    c.sql(
        sql="""
        CREATE TABLE level2 AS (
            SELECT
                grp,
                subgrp,
                VAR_POP(E) var_of_E,
                (SUM(E * E) /COUNT(E) -
                POWER(AVG(E), 2)) var_of_E2
            FROM
                example_data
            GROUP BY grp , subgrp
        )
    """,
        gpu=gpu,
    )
    result = c.sql(
        sql="""
            SELECT
                level1.grp,
                MAX(level1.mean_of_C) mean_of_C,
                MAX(level1.max_of_D) max_of_D,
                AVG(level2.var_of_E) avg_var_of_E,
                AVG(level2.var_of_E2) avg_var_of_E2
            FROM
                level1 LEFT JOIN level2 ON level1.grp = level2.grp
            GROUP BY level1.grp
            ORDER BY level1.grp
    """,
        gpu=gpu,
    )
    df = cast(DaskDataFrame, result.compute())
    df = cast(DaskDataFrame,
              df.rename(columns={
                  "mean_of_c": "mean_of_C",
                  "max_of_d": "max_of_D",
                  "avg_var_of_e": "avg_var_of_E",
                  "avg_var_of_e2": "avg_var_of_E2",
              }))
    return df
