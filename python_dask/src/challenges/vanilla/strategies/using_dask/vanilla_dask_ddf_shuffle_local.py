import dask.dataframe
import pandas as pd
import polars as pl
from spark_agg_methods_common_python.challenges.six_field_test_data.six_test_data_types import (
    VANILLA_RESULT_PANDAS_SCHEMA, SixTestExecutionParameters,
)
from src.challenges.six_field_test_data.dask_cluster import get_dask_client
from src.challenges.six_field_test_data.six_test_data_for_dask import (
    SHUFFLE_SRC_ROW_GROUPS_PER_PARTITION, SixTestDataSetDask, TChallengeAnswerPythonDask,
    pick_shuffle_tgt_num_partitions_dask,
)

# Hash-shuffle (p2p) the rows between workers so every (grp, subgrp) lives in exactly one partition,
# then aggregate each partition independently with Polars. No cross-partition merge is needed.
KEYS = ["grp", "subgrp"]


def aggregate_partition(pdf: pd.DataFrame) -> pd.DataFrame:
    E = pl.col("E")
    return (
        pl.from_pandas(pdf)
        .group_by(KEYS)
        .agg(
            mean_of_C=pl.col("C").mean(),
            max_of_D=pl.col("D").max(),
            var_of_E=E.var(ddof=0),
            var_of_E2=(E * E).sum() / E.count() - (E.sum() / E.count()) ** 2,
        )
        .to_pandas()
        .astype(VANILLA_RESULT_PANDAS_SCHEMA.dtypes.to_dict())
    )


def vanilla_dask_ddf_shuffle_local(
        exec_params: SixTestExecutionParameters,
        data_set: SixTestDataSetDask,
) -> TChallengeAnswerPythonDask:
    if data_set.data_description.points_per_index > 10**7:
        # Only num_grp_1 * num_grp_2 output partitions exist, so each holds ~points_per_index rows (OOM at 3_3_100m)
        return "infeasible", "Each key's rows must fit in one worker's memory"
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
    df = (
        client.compute(shuffled.map_partitions(aggregate_partition, meta=VANILLA_RESULT_PANDAS_SCHEMA))
        .result()  # pyright: ignore[reportAttributeAccessIssue]
    )
    return df.sort_values(KEYS).reset_index(drop=True)
