from dataclasses import dataclass
from typing import Literal, Protocol

import pandas as pd
from dask.bag.core import Bag as DaskBag
from dask.dataframe.core import DataFrame as DaskDataFrame
from dask.dataframe.io.io import from_pandas

from src.perf_test_common import CalcEngine, ChallengeMethodRegistration
from src.six_field_test_data.six_generate_test_data.six_test_data_for_python_only import \
    NumericalToleranceExpectations
from src.six_field_test_data.six_test_data_types import (
    Challenge, DataSetAnswer, DataSetDescription, ExecutionParameters,
    populate_data_set_generic)

# region Dask version


@dataclass(frozen=True)
class DataSetDataDask:
    src_num_partitions: int
    agg_tgt_num_partitions_1_level: int
    agg_tgt_num_partitions_2_level: int
    df_src: DaskDataFrame


def pick_agg_tgt_num_partitions_dask(data: DataSetDataDask, challenge: Challenge) -> int:
    match challenge:
        case Challenge.BI_LEVEL | Challenge.CONDITIONAL:
            return data.agg_tgt_num_partitions_1_level
        case Challenge.VANILLA:
            return data.agg_tgt_num_partitions_2_level
        case _:
            raise KeyError(f"Unknown challenge {challenge}")


@dataclass(frozen=True)
class DataSetDask:
    data_size: DataSetDescription
    data: DataSetDataDask


@dataclass(frozen=True)
class DataSetDaskWithAnswer(DataSetDask):
    answer: DataSetAnswer


TChallengeAnswerPythonDask = Literal["infeasible"] | DaskBag | DaskDataFrame | pd.DataFrame


class IChallengeMethodPythonDask(Protocol):
    def __call__(
        self,
        *,
        exec_params: ExecutionParameters,
        data_set: DataSetDask,
    ) -> TChallengeAnswerPythonDask: ...


@dataclass(frozen=True)
class ChallengeMethodPythonDaskRegistration(ChallengeMethodRegistration):
    # strategy_name: str
    # language: str
    # interface: str
    numerical_tolerance: NumericalToleranceExpectations
    # requires_gpu: bool
    delegate: IChallengeMethodPythonDask

# endregion


def populate_data_set_dask(
        exec_params: ExecutionParameters,
        data_size: DataSetDescription,
) -> DataSetDaskWithAnswer:
    raw_data = populate_data_set_generic(
        CalcEngine.DASK, exec_params, data_size)
    df_src: DaskDataFrame = from_pandas(raw_data.df_src, npartitions=raw_data.src_num_partitions)
    cnt, parts = len(df_src), len(df_src.divisions)
    print("Found %i rows in %i parts ratio %.1f" % (cnt, parts, cnt / parts))
    assert cnt == raw_data.num_data_points
    return DataSetDaskWithAnswer(
        data_size=data_size,
        data=DataSetDataDask(
            src_num_partitions=raw_data.src_num_partitions,
            agg_tgt_num_partitions_1_level=raw_data.tgt_num_partitions_1_level,
            agg_tgt_num_partitions_2_level=raw_data.tgt_num_partitions_2_level,
            df_src=df_src,
        ),
        answer=DataSetAnswer(
            vanilla_answer=raw_data.vanilla_answer,
            bilevel_answer=raw_data.bilevel_answer,
            conditional_answer=raw_data.conditional_answer,
        ),
    )
