import polars as pl
from spark_agg_methods_common_python.challenges.six_field_test_data.six_test_data_types import (
    SixTestExecutionParameters,
)
from src.challenges.six_field_test_data.six_test_data_for_py_st import SixDataSetPythonST, TChallengePythonSTAnswer


def cond_py_st_pl_grp_scan(
        exec_params: SixTestExecutionParameters,
        data_set: SixDataSetPythonST,
) -> TChallengePythonSTAnswer:
    lf = pl.scan_parquet(data_set.data.source_file_path_parquet)
    neg_e = pl.col("E").filter(pl.col("E") < 0)
    df_result = (
        lf
        .group_by("grp", "subgrp")
        .agg(
            mean_of_C=pl.col("C").mean(),
            max_of_D=pl.col("D").max(),
            cond_var_of_E=neg_e.var(ddof=0),
            cond_var_of_E2=(
                (neg_e * neg_e).sum() / neg_e.count()
                - (neg_e.sum() / neg_e.count()) ** 2
            ),
        )
        .sort("grp", "subgrp")
        .collect()
    )
    return df_result
