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
    E = pl.col("E")
    return (
        pl.scan_parquet(file_path)
        .slice(offset, length)
        .group_by(KEYS)
        .agg(
            n=pl.len(),
            sum_C=pl.col("C").sum(),
            max_D=pl.col("D").max(),
            sum_E=E.sum(),
            sum_E2=(E * E).sum(),
            m2_E=E.var(ddof=0) * pl.len(),  # sum of squared deviations from the chunk mean
        )
        .collect()
    )


def bi_level_dask_delayed_pl_chunks(
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
    pooled_mean_E = pl.col("sum_E").sum().over(KEYS) / pl.col("n").sum().over(KEYS)
    df_subgroup = (
        partials
        .with_columns(spread=pl.col("n") * (pl.col("sum_E") / pl.col("n") - pooled_mean_E) ** 2)
        .group_by(KEYS)
        .agg(
            n=pl.col("n").sum(),
            sum_C=pl.col("sum_C").sum(),
            max_D=pl.col("max_D").max(),
            sum_E=pl.col("sum_E").sum(),
            sum_E2=pl.col("sum_E2").sum(),
            m2_E=(pl.col("m2_E") + pl.col("spread")).sum(),
        )
    )
    df_result = (
        df_subgroup
        .select(
            "grp",
            "n",
            "sum_C",
            "max_D",
            var_of_E=pl.col("m2_E") / pl.col("n"),
            var_of_E2=pl.col("sum_E2") / pl.col("n") - (pl.col("sum_E") / pl.col("n")) ** 2,
        )
        .group_by("grp")
        .agg(
            mean_of_C=pl.col("sum_C").sum() / pl.col("n").sum(),
            max_of_D=pl.col("max_D").max(),
            avg_var_of_E=pl.col("var_of_E").mean(),
            avg_var_of_E2=pl.col("var_of_E2").mean(),
        )
        .sort("grp")
    )
    return df_result.to_pandas()
