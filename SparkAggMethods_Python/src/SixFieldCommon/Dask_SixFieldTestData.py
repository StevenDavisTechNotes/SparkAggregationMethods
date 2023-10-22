from dataclasses import dataclass
from typing import Callable, Tuple

import pandas as pd
from dask.bag.core import Bag as dask_bag
from dask.dataframe.core import DataFrame as dask_dataframe
from dask.dataframe.io.io import from_pandas
from dask.distributed import Client as DaskClient

from SixFieldCommon.SixFieldTestData import (DataSetAnswer, DataSetDescription,
                                             ExecutionParameters,
                                             populate_data_set_generic)

# region Dask version


@dataclass(frozen=True)
class DaskDataSetData:
    SrcNumPartitions: int
    AggTgtNumPartitions: int
    dfSrc: dask_dataframe


@dataclass(frozen=True)
class DaskDataSet:
    description: DataSetDescription
    data: DaskDataSetData


@dataclass(frozen=True)
class DaskDataSetWithAnswer(DaskDataSet):
    answer: DataSetAnswer


@dataclass(frozen=True)
class DaskPythonTestMethod:
    strategy_name: str
    language: str
    interface: str
    delegate: Callable[
        [DaskClient, ExecutionParameters, DaskDataSet],
        Tuple[dask_bag | None, dask_dataframe | None, pd.DataFrame | None]]

# endregion


def populate_data_set_dask(
        exec_params: ExecutionParameters,
        size_code: str,
        num_grp_1: int,
        num_grp_2: int,
        repetition: int,
) -> DaskDataSetWithAnswer:
    num_data_points, tgt_num_partitions, src_num_partitions, df, \
        vanilla_answer, bilevel_answer, conditional_answer = populate_data_set_generic(
            exec_params, num_grp_1, num_grp_2, repetition)
    df_src: dask_dataframe = from_pandas(df, chunksize=src_num_partitions)
    cnt, parts = len(df_src), len(df_src.divisions)
    print("Found rdd %i rows in %i parts ratio %.1f" % (cnt, parts, cnt / parts))
    assert cnt == num_data_points
    del df
    return DaskDataSetWithAnswer(
        description=DataSetDescription(
            NumDataPoints=num_grp_1 * num_grp_2 * repetition,
            NumGroups=num_grp_1,
            NumSubGroups=num_grp_2,
            SizeCode=size_code,
            RelativeCardinalityBetweenGroupings=num_grp_2 // num_grp_1,
        ),
        data=DaskDataSetData(
            SrcNumPartitions=src_num_partitions,
            AggTgtNumPartitions=tgt_num_partitions,
            dfSrc=df_src,
        ),
        answer=DataSetAnswer(
            vanilla_answer=vanilla_answer,
            bilevel_answer=bilevel_answer,
            conditional_answer=conditional_answer,
        ),
    )
