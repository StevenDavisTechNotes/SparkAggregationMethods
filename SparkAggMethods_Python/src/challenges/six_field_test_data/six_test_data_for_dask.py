from dataclasses import dataclass
from typing import Literal, Protocol

import pandas as pd
from dask.bag.core import Bag as DaskBag
from dask.dataframe.core import DataFrame as DaskDataFrame
from dask.dataframe.io.io import from_pandas
from spark_agg_methods_common_python.challenges.six_field_test_data.six_test_data_types import (
    DataPointNT, SixTestDataChallengeMethodRegistrationBase, SixTestDataSetAnswers, SixTestDataSetDescription,
    SixTestExecutionParameters, populate_six_data_set_generic,
)
from spark_agg_methods_common_python.perf_test_common import (
    CalcEngine, Challenge, NumericalToleranceExpectations, SolutionInterfaceDask, SolutionLanguage,
)


@dataclass(frozen=True)
class DataSetDataDask:
    src_num_partitions: int
    agg_tgt_num_partitions_1_level: int
    agg_tgt_num_partitions_2_level: int
    df_src: DaskDataFrame
    bag_src: DaskBag


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
    data_description: SixTestDataSetDescription
    data: DataSetDataDask


@dataclass(frozen=True)
class DataSetDaskWithAnswer(DataSetDask):
    answer: SixTestDataSetAnswers


TChallengeAnswerPythonDask = Literal["infeasible"] | DaskDataFrame | pd.DataFrame


class IChallengeMethodPythonDask(Protocol):
    def __call__(
        self,
        *,
        exec_params: SixTestExecutionParameters,
        data_set: DataSetDask,
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


def populate_data_set_dask(
        exec_params: SixTestExecutionParameters,
        data_size: SixTestDataSetDescription,
) -> DataSetDaskWithAnswer:
    raw_data = populate_six_data_set_generic(
        CalcEngine.DASK, exec_params, data_size)
    df_src: DaskDataFrame = from_pandas(raw_data.df_src, npartitions=raw_data.src_num_partitions)
    bag_src: DaskBag = df_src.to_bag().map(lambda x: DataPointNT(*x))
    cnt, parts = len(df_src), len(df_src.divisions)
    print("Found %i rows in %i parts ratio %.1f" % (cnt, parts, cnt / parts))
    assert cnt == raw_data.num_source_rows
    return DataSetDaskWithAnswer(
        data_description=data_size,
        data=DataSetDataDask(
            src_num_partitions=raw_data.src_num_partitions,
            agg_tgt_num_partitions_1_level=raw_data.tgt_num_partitions_1_level,
            agg_tgt_num_partitions_2_level=raw_data.tgt_num_partitions_2_level,
            df_src=df_src,
            bag_src=bag_src,
        ),
        answer=SixTestDataSetAnswers(
            vanilla_answer=raw_data.vanilla_answer,
            bilevel_answer=raw_data.bilevel_answer,
            conditional_answer=raw_data.conditional_answer,
        ),
    )
