from typing import cast

import polars as pl
from distributed.client import Future
from spark_agg_methods_common_python.challenges.six_field_test_data.six_test_data_types import (
    SixTestExecutionParameters,
)
from spark_agg_methods_common_python.perf_test_common import TARGET_ROWS_PER_PARTITION
from src.challenges.six_field_test_data.dask_cluster import get_dask_client
from src.challenges.six_field_test_data.six_test_data_for_dask import SixTestDataSetDask, TChallengeAnswerPythonDask

# Strategy by Claude

# Chunk tasks aggregate one slice of the Parquet file on a worker and push the partial result to one actor per
# grp value; each actor folds what it receives into a running aggregate. The actors' states are read back at the end.
KEYS = ["grp", "subgrp"]


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


def merge_partials(partials: pl.DataFrame) -> pl.DataFrame:
    """Merge partial aggregates that may repeat a key; the output has the same columns as the input."""
    # Chan et al.: pooled M2 = sum of chunk M2 plus the spread of chunk means around the pooled mean
    pooled_mean_E = pl.col("sum_E").sum().over(KEYS) / pl.col("n").sum().over(KEYS)
    return (
        partials
        .with_columns(
            spread=pl.col("n")
            * (pl.col("sum_E") / pl.col("n") - pooled_mean_E) ** 2
        )
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
    # Actor methods can only be called from worker tasks here
    # the client is not the default client
    return actor.snapshot().result()


def bi_level_dask_actors_accumulate(
        exec_params: SixTestExecutionParameters,
        data_set: SixTestDataSetDask,
) -> TChallengeAnswerPythonDask:
    client = get_dask_client(exec_params)
    file_path = data_set.data.source_file_path_parquet
    offsets = range(0, data_set.data_description.num_source_rows, TARGET_ROWS_PER_PARTITION)
    actor_futures = [
        client.submit(PartialMerger, actor=True)
        for _ in range(data_set.data_description.num_grp_1)
    ]
    try:
        for actor_future in actor_futures:
            actor_future.result()
        chunk_futures = [
            client.submit(
                aggregate_and_push, file_path, offset, TARGET_ROWS_PER_PARTITION,
                *actor_futures  # Actor futures must be passed positionally
            )
            for offset in offsets
        ]
        client.gather(chunk_futures)
        futures: list[Future[pl.DataFrame | None]] = [
            client.submit(read_state, actor_future)
            for actor_future in actor_futures
        ]
        states = cast(list[pl.DataFrame | None], client.gather(futures))
    finally:
        client.cancel(actor_futures)
    # Keys are disjoint across actors (one actor per grp), so concatenation is the full merged aggregate
    acc = pl.concat([state for state in states if state is not None])
    df_result = (
        acc
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
