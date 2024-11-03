import logging
from dataclasses import dataclass
from typing import Literal, Protocol, cast

import dask.dataframe
import pandas as pd
from dask.bag.core import Bag as DaskBag
from dask.dataframe.core import DataFrame as DaskDataFrame
from spark_agg_methods_common_python.challenges.six_field_test_data.six_test_data_types import (
    DataPointNT, SixTestDataChallengeMethodRegistrationBase, SixTestDataSetDescription, SixTestExecutionParameters,
    six_derive_source_test_data_file_path,
)
from spark_agg_methods_common_python.perf_test_common import (
    CalcEngine, Challenge, NumericalToleranceExpectations, SolutionInterfaceDask, SolutionLanguage,
)
from spark_agg_methods_common_python.utils.utils import int_divide_round_up

MAX_DATA_POINTS_PER_DASK_PARTITION = 1 * 10**5
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SixTestDataSetDataDask:
    src_num_partitions: int
    agg_tgt_num_partitions_1_level: int
    agg_tgt_num_partitions_2_level: int
    df_src: DaskDataFrame
    bag_src: DaskBag


def pick_agg_tgt_num_partitions_dask(data: SixTestDataSetDataDask, challenge: Challenge) -> int:
    match challenge:
        case Challenge.BI_LEVEL | Challenge.CONDITIONAL:
            return data.agg_tgt_num_partitions_1_level
        case Challenge.VANILLA:
            return data.agg_tgt_num_partitions_2_level
        case _:
            raise KeyError(f"Unknown challenge {challenge}")


@dataclass(frozen=True)
class SixTestDataSetDask:
    data_description: SixTestDataSetDescription
    data: SixTestDataSetDataDask


TChallengeAnswerPythonDask = Literal["infeasible"] | DaskDataFrame | pd.DataFrame


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


def six_populate_data_set_dask(
        exec_params: SixTestExecutionParameters,
        data_description: SixTestDataSetDescription,
) -> SixTestDataSetDataDask:
    num_grp_1 = data_description.num_grp_1
    num_grp_2 = data_description.num_grp_2
    points_per_index = data_description.points_per_index
    num_source_rows = num_grp_1 * num_grp_2 * points_per_index

    source_file_name_parquet, source_file_name_csv = six_derive_source_test_data_file_path(
        data_description=data_description,
    )
    max_data_points_per_partition = MAX_DATA_POINTS_PER_DASK_PARTITION
    src_num_partitions = (
        1 if max_data_points_per_partition < 0 else
        max(
            exec_params.default_parallelism,
            int_divide_round_up(
                num_source_rows,
                max_data_points_per_partition,
            )
        )
    )
    read_parquet = dask.dataframe.read_parquet  # type: ignore
    df_src = cast(DaskDataFrame, read_parquet(
        source_file_name_parquet,
        engine='pyarrow',
    ))
    df_src = df_src.set_index('id', npartitions=src_num_partitions)
    cnt, parts = len(df_src), len(df_src.divisions)
    logger.info("Found %i rows in %i parts ratio %.1f" % (cnt, parts, cnt / parts))
    assert cnt == num_source_rows

    bag_src: DaskBag = df_src.to_bag().map(lambda x: DataPointNT(*x))
    return SixTestDataSetDataDask(
        src_num_partitions=src_num_partitions,
        agg_tgt_num_partitions_1_level=num_grp_1,
        agg_tgt_num_partitions_2_level=num_grp_1 * num_grp_2,
        df_src=df_src,
        bag_src=bag_src,
    )
