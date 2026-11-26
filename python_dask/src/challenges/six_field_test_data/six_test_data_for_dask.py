import logging
from dataclasses import dataclass
from typing import Literal, Protocol, cast

import dask.dataframe
import pandas as pd
from dask.bag.core import Bag as DaskBag
from dask.dataframe import DataFrame as DaskDataFrame
from spark_agg_methods_common_python.challenges.six_field_test_data.six_test_data_types import (
    TARGET_PARQUET_BATCH_SIZE, SixTestDataChallengeMethodRegistrationBase, SixTestDataSetDescription,
    SixTestExecutionParameters, six_derive_source_test_data_file_path,
)
from spark_agg_methods_common_python.perf_test_common import (
    TARGET_ROWS_PER_PARTITION, CalcEngine, Challenge, NumericalToleranceExpectations, SolutionInterfaceDask,
    SolutionLanguage,
)
from spark_agg_methods_common_python.utils.utils import int_divide_round_up

# Read side of the hash-shuffle strategies: how many ~TARGET_PARQUET_BATCH_SIZE-row Parquet row groups form one input
# partition, so that an input partition holds about TARGET_ROWS_PER_PARTITION rows. Left at Dask's default a 9M-row
# file is read as a single partition, which measured slower than this at 3_3_1m.
SHUFFLE_SRC_ROW_GROUPS_PER_PARTITION = TARGET_ROWS_PER_PARTITION // TARGET_PARQUET_BATCH_SIZE
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SixTestDataSetDataDask:
    src_num_partitions: int
    agg_tgt_num_partitions_1_level: int
    agg_tgt_num_partitions_2_level: int
    source_file_path_parquet: str

    def open_source_data_as_bag(self) -> DaskBag:
        return (
            self.open_source_data_as_ddf()
            .reset_index()
            .to_bag()
        )

    def open_source_data_as_ddf(self) -> DaskDataFrame:
        read_parquet = dask.dataframe.read_parquet
        src_df = cast(DaskDataFrame, read_parquet(
            self.source_file_path_parquet,
            engine="pyarrow",
        ))
        src_df = src_df.set_index("id", npartitions=self.src_num_partitions)
        return src_df


def pick_agg_tgt_num_partitions_dask(data: SixTestDataSetDataDask, challenge: Challenge) -> int:
    match challenge:
        case Challenge.BILEVEL | Challenge.CONDITIONAL:
            return data.agg_tgt_num_partitions_1_level
        case Challenge.VANILLA:
            return data.agg_tgt_num_partitions_2_level
        case _:
            raise KeyError(f"Unknown challenge {challenge}")


def pick_shuffle_tgt_num_partitions_dask(
        exec_params: SixTestExecutionParameters,
        data_description: SixTestDataSetDescription,
) -> int:
    """Output partition count for a hash shuffle on (grp, subgrp), derived from the data set.

    Every (grp, subgrp) key has points_per_index rows and lands whole in one partition, so a partition can
    never hold fewer than points_per_index rows and there are at most num_grp_1 * num_grp_2 non-empty partitions.
    Within that cap, use at least default_parallelism partitions, and more when the rows would exceed
    TARGET_ROWS_PER_PARTITION per partition.
    """
    num_keys = data_description.num_grp_1 * data_description.num_grp_2
    num_partitions_for_size = int_divide_round_up(
        data_description.num_source_rows,
        TARGET_ROWS_PER_PARTITION,
    )
    return min(num_keys, max(exec_params.default_parallelism, num_partitions_for_size))


@dataclass(frozen=True)
class SixTestDataSetDask:
    data_description: SixTestDataSetDescription
    data: SixTestDataSetDataDask


TChallengeAnswerPythonDask = (
    tuple[Literal["infeasible"], str]
    | DaskDataFrame
    | pd.DataFrame
)


class IChallengeMethodPythonDask(Protocol):
    def __call__(
        self,
        *,
        exec_params: SixTestExecutionParameters,
        data_set: SixTestDataSetDask,
    ) -> TChallengeAnswerPythonDask: ...


@dataclass(frozen=True)
class ChallengeMethodPythonDaskRegistration(
    SixTestDataChallengeMethodRegistrationBase[
        SolutionInterfaceDask, IChallengeMethodPythonDask
    ]
):
    strategy_name: str
    language: SolutionLanguage
    engine: CalcEngine
    interface: SolutionInterfaceDask
    numerical_tolerance: NumericalToleranceExpectations
    requires_gpu: bool
    delegate: IChallengeMethodPythonDask
    requires_cluster: bool = False


def six_prepare_data_set_dask(
        exec_params: SixTestExecutionParameters,
        data_description: SixTestDataSetDescription,
) -> SixTestDataSetDataDask:
    num_grp_1 = data_description.num_grp_1
    num_grp_2 = data_description.num_grp_2
    points_per_index = data_description.points_per_index
    num_source_rows = num_grp_1 * num_grp_2 * points_per_index

    source_file_paths = six_derive_source_test_data_file_path(
        data_description=data_description,
    )
    src_num_partitions = max(
        exec_params.default_parallelism,
        int_divide_round_up(
            num_source_rows,
            TARGET_ROWS_PER_PARTITION,
        )
    )
    return SixTestDataSetDataDask(
        src_num_partitions=src_num_partitions,
        agg_tgt_num_partitions_1_level=num_grp_1,
        agg_tgt_num_partitions_2_level=num_grp_1 * num_grp_2,
        source_file_path_parquet=str(source_file_paths.source_file_path_parquet_single_file),
    )
