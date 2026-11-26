from typing import cast

import polars as pl
from spark_agg_methods_common_python.challenges.six_field_test_data.six_test_data_types import (
    SixTestExecutionParameters,
)
from spark_agg_methods_common_python.perf_test_common import TARGET_ROWS_PER_PARTITION
from src.challenges.six_field_test_data.dask_cluster import get_dask_client
from src.challenges.six_field_test_data.six_test_data_for_dask import SixTestDataSetDask, TChallengeAnswerPythonDask

# Chunk tasks aggregate one slice of the Parquet file on a worker and push the partial result to one actor per
# grp value; each actor folds what it receives into a running aggregate. The actors' states are read back at the end.
KEYS = ["grp", "subgrp"]

# From Claude


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


class PartialMerger:
    """Actor owning the running aggregate of one grp value; it lives on a worker and is fed by chunk tasks."""

    def __init__(self) -> None:
        self.state: pl.DataFrame | None = None

    def add(self, partial: pl.DataFrame) -> int:
        self.state = merge_partials(partial if self.state is None else pl.concat([self.state, partial]))
        return self.state.height

    def snapshot(self) -> pl.DataFrame | None:  # not named `result`: that clashes with the ActorFuture API
        return self.state


def aggregate_and_push(file_path: str, offset: int, length: int, *actors) -> int:
    """Aggregate one chunk, then hand each grp's rows to that grp's actor (actor i owns grp == i)."""
    partial = aggregate_chunk(
        file_path=file_path,
        offset=offset,
        length=length,
    )
    for grp, actor in enumerate(actors):
        part = partial.filter(pl.col("grp") == grp)
        if part.height > 0:
            actor.add(part).result()
    return partial.height


def read_state(actor) -> pl.DataFrame | None:
    # Actor methods can only be called from worker tasks here: the client is not the default client
    return actor.snapshot().result()


def conditional_dask_actors_accumulate(
        exec_params: SixTestExecutionParameters,
        data_set: SixTestDataSetDask,
) -> TChallengeAnswerPythonDask:
    client = get_dask_client(exec_params)
    file_path = data_set.data.source_file_path_parquet
    offsets = range(0, data_set.data_description.num_source_rows, TARGET_ROWS_PER_PARTITION)
    actor_futures = [client.submit(PartialMerger, actor=True) for _ in range(data_set.data_description.num_grp_1)]
    try:
        for actor_future in actor_futures:
            actor_future.result()
        # Actor futures must be passed positionally (not in a list, not .result()ed) so tasks receive live actors
        chunk_futures = [
            client.submit(aggregate_and_push, file_path, offset, TARGET_ROWS_PER_PARTITION, *actor_futures)
            for offset in offsets
        ]
        client.gather(chunk_futures)
        states = cast(
            list[pl.DataFrame | None],
            client.gather([client.submit(read_state, actor_future) for actor_future in actor_futures]),
        )
    finally:
        client.cancel(actor_futures)
    # Keys are disjoint across actors (one actor per grp), so concatenation is the full merged aggregate
    acc = pl.concat([state for state in states if state is not None])
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
