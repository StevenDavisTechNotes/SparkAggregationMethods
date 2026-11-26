import polars as pl
from spark_agg_methods_common_python.challenges.six_field_test_data.six_test_data_types import (
    SixTestExecutionParameters,
)
from src.challenges.six_field_test_data.six_test_data_for_py_st import SixDataSetPythonST, TChallengePythonSTAnswer


def bilevel_py_st_pl_grp_scan(
        exec_params: SixTestExecutionParameters,
        data_set: SixDataSetPythonST,
) -> TChallengePythonSTAnswer:
    lf = pl.scan_parquet(data_set.data.source_file_path_parquet)
    E = pl.col("E")
    lf_group = (
        lf
        .group_by("grp")
        .agg(
            mean_of_C=pl.col("C").mean(),
            max_of_D=pl.col("D").max(),
        )
    )
    lf_subgroup = (
        lf
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
    result_df = (
        lf_group
        .join(lf_subgroup, on="grp", how="inner")
        .sort("grp")
        .collect()
    )
    return result_df
