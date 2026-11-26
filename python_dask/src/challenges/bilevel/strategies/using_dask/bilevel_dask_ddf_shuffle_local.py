import dask.dataframe
import pandas as pd
import polars as pl
from spark_agg_methods_common_python.challenges.six_field_test_data.six_test_data_types import (
    BILEVEL_RESULT_PANDAS_SCHEMA, SixTestExecutionParameters,
)
from src.challenges.six_field_test_data.dask_cluster import get_dask_client
from src.challenges.six_field_test_data.six_test_data_for_dask import (
    SHUFFLE_SRC_ROW_GROUPS_PER_PARTITION, SixTestDataSetDask, TChallengeAnswerPythonDask,
    pick_shuffle_tgt_num_partitions_dask,
)

# Hash-shuffle (p2p) the rows on BOTH keys (grp alone has too few values to spread across workers) so every
# (grp, subgrp) lives in exactly one partition, aggregate each partition with Polars, then fold the small
# per-subgroup results into per-group results on the client.
KEYS = ["grp", "subgrp"]


def aggregate_partition(pdf: pd.DataFrame) -> pd.DataFrame:
    E = pl.col("E")
    return (
        pl.from_pandas(pdf)
        .group_by(KEYS)
        .agg(
            n=pl.len().cast(pl.Float64),
            sum_C=pl.col("C").sum(),
            max_D=pl.col("D").max(),
            var_of_E=E.var(ddof=0),
            var_of_E2=(E * E).sum() / E.count() - (E.sum() / E.count()) ** 2,
        )
        .to_pandas()
        .astype(BILEVEL_RESULT_PANDAS_SCHEMA.dtypes.to_dict())
    )


def bilevel_dask_ddf_shuffle_local(
        exec_params: SixTestExecutionParameters,
        data_set: SixTestDataSetDask,
) -> TChallengeAnswerPythonDask:
    client = get_dask_client(exec_params)
    ddf = dask.dataframe.read_parquet(
        data_set.data.source_file_path_parquet,
        engine="pyarrow",
        columns=["grp", "subgrp", "C", "D", "E"],
        split_row_groups=SHUFFLE_SRC_ROW_GROUPS_PER_PARTITION,  # pyright: ignore[reportArgumentType]
    )
    shuffled = ddf.shuffle(
        on=KEYS,
        npartitions=pick_shuffle_tgt_num_partitions_dask(
            exec_params=exec_params,
            data_description=data_set.data_description,
        ),
        shuffle_method="p2p",
    )
    subgroup_df: pd.DataFrame = (
        client.compute(
            shuffled.map_partitions(aggregate_partition, meta=BILEVEL_RESULT_PANDAS_SCHEMA)
        )
        .result()  # pyright: ignore[reportAttributeAccessIssue]
    )
    return (
        pl.from_pandas(subgroup_df)
        .group_by("grp")
        .agg(
            mean_of_C=pl.col("sum_C").sum() / pl.col("n").sum(),
            max_of_D=pl.col("max_D").max(),
            avg_var_of_E=pl.col("var_of_E").mean(),
            avg_var_of_E2=pl.col("var_of_E2").mean(),
        )
        .sort("grp")
        .to_pandas()
    )
