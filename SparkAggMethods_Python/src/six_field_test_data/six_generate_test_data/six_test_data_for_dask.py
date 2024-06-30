from dataclasses import dataclass
from typing import Literal, Protocol

import pandas as pd
from dask.bag.core import Bag as DaskBag
from dask.dataframe.core import DataFrame as DaskDataFrame
from dask.dataframe.io.io import from_pandas

from perf_test_common import CalcEngine
from six_field_test_data.six_test_data_types import (DataSetAnswer,
                                                     DataSetDescription,
                                                     ExecutionParameters,
                                                     populate_data_set_generic)

# region Dask version


@dataclass(frozen=True)
class DataSetDataDask:
    src_num_partitions: int
    agg_tgt_num_partitions: int
    df_src: DaskDataFrame


@dataclass(frozen=True)
class DataSetDask:
    description: DataSetDescription
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
class ChallengeMethodPythonDaskRegistration:
    strategy_name: str
    language: str
    interface: str
    requires_gpu: bool
    delegate: IChallengeMethodPythonDask

# endregion


def populate_data_set_dask(
        exec_params: ExecutionParameters,
        size_code: str,
        num_grp_1: int,
        num_grp_2: int,
        repetition: int,
) -> DataSetDaskWithAnswer:
    raw_data = populate_data_set_generic(
        CalcEngine.DASK, exec_params, num_grp_1, num_grp_2, repetition)
    df_src: DaskDataFrame = from_pandas(raw_data.dfSrc, npartitions=raw_data.src_num_partitions)
    cnt, parts = len(df_src), len(df_src.divisions)
    print("Found rdd %i rows in %i parts ratio %.1f" % (cnt, parts, cnt / parts))
    assert cnt == raw_data.num_data_points
    return DataSetDaskWithAnswer(
        description=DataSetDescription(
            NumDataPoints=num_grp_1 * num_grp_2 * repetition,
            NumGroups=num_grp_1,
            NumSubGroups=num_grp_2,
            SizeCode=size_code,
            RelativeCardinalityBetweenGroupings=num_grp_2 // num_grp_1,
        ),
        data=DataSetDataDask(
            src_num_partitions=raw_data.src_num_partitions,
            agg_tgt_num_partitions=raw_data.tgt_num_partitions,
            df_src=df_src,
        ),
        answer=DataSetAnswer(
            vanilla_answer=raw_data.vanilla_answer,
            bilevel_answer=raw_data.bilevel_answer,
            conditional_answer=raw_data.conditional_answer,
        ),
    )
