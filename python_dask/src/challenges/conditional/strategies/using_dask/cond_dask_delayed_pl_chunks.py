import polars as pl
from dask.base import compute
from dask.delayed import delayed
from spark_agg_methods_common_python.challenges.six_field_test_data.six_test_data_types import (
    SixTestExecutionParameters,
)
from spark_agg_methods_common_python.perf_test_common import TARGET_ROWS_PER_PARTITION
from src.challenges.six_field_test_data.six_test_data_for_dask import SixTestDataSetDask, TChallengeAnswerPythonDask

# Each Dask task aggregates one slice of the Parquet file with Polars; the small partial results are merged locally.
KEYS = ["grp", "subgrp"]


@delayed
def aggregate_chunk(file_path: str, offset: int, length: int) -> pl.DataFrame:
    negE = pl.col("E").filter(pl.col("E") < 0)
    return (
        pl.scan_parquet(file_path)
        .slice(offset, length)
        .group_by(KEYS)
        .agg(
            n=pl.len(),
            sum_C=pl.col("C").sum(),
            max_D=pl.col("D").max(),
            n_neg=negE.count(),
            sum_negE=negE.sum(),
            sum_negE2=(negE * negE).sum(),
            # sum of squared deviations from the chunk mean; null (no negative E in this chunk) counts as 0
            m2_negE=(negE.var(ddof=0) * negE.count()).fill_null(0.0),
        )
        .collect()
    )


def cond_dask_delayed_pl_chunks(
        exec_params: SixTestExecutionParameters,
        data_set: SixTestDataSetDask,
) -> TChallengeAnswerPythonDask:
    file_path = data_set.data.source_file_path_parquet
    num_source_rows = data_set.data_description.num_source_rows
    tasks = [
        aggregate_chunk(
            file_path=file_path,
            offset=offset,
            length=TARGET_ROWS_PER_PARTITION,
        )
        for offset in range(0, num_source_rows, TARGET_ROWS_PER_PARTITION)
    ]
    partials = pl.concat(compute(*tasks))
    # Chan et al.: pooled M2 = sum of chunk M2 plus the spread of chunk means around the pooled mean
    pooled_mean_negE = pl.col("sum_negE").sum().over(KEYS) / pl.col("n_neg").sum().over(KEYS)
    has_neg = pl.col("n_neg") > 0
    df_result = (
        partials
        .with_columns(
            spread=pl.when(has_neg)
            .then(pl.col("n_neg") * (pl.col("sum_negE") / pl.col("n_neg") - pooled_mean_negE) ** 2)
            .otherwise(0.0)
        )
        .group_by(KEYS)
        .agg(
            n=pl.col("n").sum(),
            sum_C=pl.col("sum_C").sum(),
            max_D=pl.col("max_D").max(),
            n_neg=pl.col("n_neg").sum(),
            sum_negE=pl.col("sum_negE").sum(),
            sum_negE2=pl.col("sum_negE2").sum(),
            m2_negE=(pl.col("m2_negE") + pl.col("spread")).sum(),
        )
        .select(
            *KEYS,
            mean_of_C=pl.col("sum_C") / pl.col("n"),
            max_of_D=pl.col("max_D"),
            # groups without any negative E have no variance
            cond_var_of_E=pl.when(has_neg).then(pl.col("m2_negE") / pl.col("n_neg")),
            cond_var_of_E2=pl.when(has_neg).then(
                pl.col("sum_negE2") / pl.col("n_neg") - (pl.col("sum_negE") / pl.col("n_neg")) ** 2
            ),
        )
        .sort(KEYS)
    )
    return df_result.to_pandas()
