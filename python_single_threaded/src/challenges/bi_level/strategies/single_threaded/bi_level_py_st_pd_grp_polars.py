import polars as pl
from spark_agg_methods_common_python.challenges.six_field_test_data.six_test_data_types import (
    SixTestExecutionParameters,
)
from src.challenges.six_field_test_data.six_test_data_for_py_st import SixDataSetPythonST, TChallengePythonSTAnswer


def bi_level_py_st_pd_grp_polars(
        exec_params: SixTestExecutionParameters,
        data_set: SixDataSetPythonST,
) -> TChallengePythonSTAnswer:
    if data_set.data_description.num_source_rows > 9 * 10**6:
        return "infeasible", "Unknown reason"
    df = pl.read_parquet(data_set.data.source_file_path_parquet)
    E = pl.col("E")
    df_group = (
        df
        .group_by("grp")
        .agg(
            mean_of_C=pl.col("C").mean(),
            max_of_D=pl.col("D").max(),
        )
    )
    df_subgroup = (
        df
        .group_by("grp", "subgrp")
        .agg(
            var_of_E=E.var(ddof=0),
            var_of_E2=(E * E).sum() / E.count() - (E.sum() / E.count()) ** 2,
        )
        .group_by("grp")
        .agg(
            avg_var_of_E=pl.col("var_of_E").mean(),
            avg_var_of_E2=pl.col("var_of_E2").mean(),
        )
    )
    df_result = (
        df_group
        .join(df_subgroup, on="grp", how="inner")
        .sort("grp")
    )
    return df_result
