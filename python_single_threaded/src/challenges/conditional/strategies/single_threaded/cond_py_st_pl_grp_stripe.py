import polars as pl
from spark_agg_methods_common_python.challenges.six_field_test_data.six_test_data_types import (
    SixTestExecutionParameters,
)
from src.challenges.six_field_test_data.six_test_data_for_py_st import SixDataSetPythonST, TChallengePythonSTAnswer


def cond_py_st_pl_grp_stripe(
        exec_params: SixTestExecutionParameters,
        data_set: SixDataSetPythonST,
) -> TChallengePythonSTAnswer:
    neg_e = pl.col("E").filter(pl.col("E") < 0)
    stripes = [
        (
            pl.scan_parquet(data_set.data.source_file_path_parquet)
            .filter(pl.col("grp") == grp)
            .group_by("grp", "subgrp")
            .agg(
                mean_of_C=pl.col("C").mean(),
                max_of_D=pl.col("D").max(),
                cond_var_of_E=neg_e.var(ddof=0),
                cond_var_of_E2=(neg_e * neg_e).sum() / neg_e.count() - (neg_e.sum() / neg_e.count()) ** 2,
            )
            .sort("grp", "subgrp")
            .collect()
        )
        for grp in range(data_set.data_description.num_grp_1)
    ]
    return pl.concat(stripes)
