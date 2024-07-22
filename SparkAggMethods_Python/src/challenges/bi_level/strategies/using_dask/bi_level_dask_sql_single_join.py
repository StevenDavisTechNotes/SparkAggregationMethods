from typing import cast

from dask.dataframe.core import DataFrame as DaskDataFrame
from dask_sql import Context

from src.six_field_test_data.six_generate_test_data import (
    DataSetDask, TChallengeAnswerPythonDask)
from src.six_field_test_data.six_test_data_types import ExecutionParameters

# pyright: reportArgumentType=false
# pyright: reportCallIssue=false


def bi_level_dask_sql_single_join_no_gpu(
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
                level1.grp,
                MAX(level1.mean_of_C) mean_of_C,
                MAX(level1.max_of_D) max_of_D,
                AVG(level2.var_of_E) avg_var_of_E,
                AVG(level2.var_of_E2) avg_var_of_E2
            FROM
                (SELECT
                    grp, AVG(C) mean_of_C, MAX(D) max_of_D
                FROM
                    example_data
                GROUP BY grp) AS level1
                    LEFT JOIN
                (SELECT
                        grp,
                        subgrp,
                        VAR_POP(E) var_of_E,
                        (SUM(E * E) /COUNT(E) -
                        POWER(AVG(E), 2)) var_of_E2
                    FROM
                        example_data
                    GROUP BY grp , subgrp
                ) AS level2
                    ON level1.grp = level2.grp
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
