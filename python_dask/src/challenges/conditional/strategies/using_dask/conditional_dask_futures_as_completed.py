import polars as pl
from distributed import as_completed
from spark_agg_methods_common_python.challenges.six_field_test_data.six_test_data_types import (
    SixTestExecutionParameters,
)
from spark_agg_methods_common_python.perf_test_common import TARGET_ROWS_PER_PARTITION
from src.challenges.six_field_test_data.dask_cluster import get_dask_client
from src.challenges.six_field_test_data.six_test_data_for_dask import SixTestDataSetDask, TChallengeAnswerPythonDask

# Each future aggregates one slice of the Parquet file on a worker; the client folds the small partial
# results into a running total as soon as each future completes, rather than waiting for all of them.
KEYS = ["grp", "subgrp"]


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


def merge_partials(partials: pl.DataFrame) -> pl.DataFrame:
    """Merge partial aggregates that may repeat a key; the output has the same columns as the input."""
    # Chan et al.: pooled M2 = sum of chunk M2 plus the spread of chunk means around the pooled mean
    pooled_mean_negE = pl.col("sum_negE").sum().over(KEYS) / pl.col("n_neg").sum().over(KEYS)
    return (
        partials
        .with_columns(
            spread=pl.when(pl.col("n_neg") > 0)
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
    )


def conditional_dask_futures_as_completed(
        exec_params: SixTestExecutionParameters,
        data_set: SixTestDataSetDask,
) -> TChallengeAnswerPythonDask:
    client = get_dask_client(exec_params)
    file_path = data_set.data.source_file_path_parquet
    offsets = list(range(0, data_set.data_description.num_source_rows, TARGET_ROWS_PER_PARTITION))
    futures = client.map(
        aggregate_chunk,
        [file_path] * len(offsets), offsets,
        [TARGET_ROWS_PER_PARTITION] * len(offsets),
    )
    acc: pl.DataFrame | None = None
    for future in as_completed(futures, loop=client.loop):
        partial = future.result()
        acc = merge_partials(
            partial if acc is None else pl.concat([acc, partial])
        )
    assert acc is not None
    has_neg = pl.col("n_neg") > 0
    result_df = (
        acc
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
    return result_df.to_pandas()
