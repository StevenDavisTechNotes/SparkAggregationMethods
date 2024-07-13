from dataclasses import dataclass
from enum import Enum
from typing import Literal, Protocol

import pandas as pd

from src.perf_test_common import CalcEngine
from src.six_field_test_data.six_test_data_types import (
    Challenge, DataSetAnswer, DataSetDescription, ExecutionParameters,
    populate_data_set_generic)


class NumericalToleranceExpectations(Enum):
    NUMPY = 1e-12
    NUMBA = 1e-10

# region PythonOnly version


@dataclass(frozen=True)
class DataSetDataPythonOnly():
    src_num_partitions: int
    agg_tgt_num_partitions_1_level: int
    agg_tgt_num_partitions_2_level: int
    df_src: pd.DataFrame


def pick_agg_tgt_num_partitions_python_only(data: DataSetDataPythonOnly, challenge: Challenge) -> int:
    match challenge:
        case Challenge.BI_LEVEL | Challenge.CONDITIONAL:
            return data.agg_tgt_num_partitions_1_level
        case Challenge.VANILLA:
            return data.agg_tgt_num_partitions_2_level
        case _:
            raise KeyError(f"Unknown challenge {challenge}")


@dataclass(frozen=True)
class DataSetPythonOnly():
    data_size: DataSetDescription
    data: DataSetDataPythonOnly


@dataclass(frozen=True)
class DataSetPythonOnlyWithAnswer(DataSetPythonOnly):
    answer: DataSetAnswer


TChallengePythonOnlyAnswer = Literal["infeasible"] | pd.DataFrame


class IChallengeMethodPythonOnly(Protocol):
    def __call__(
        self,
        *,
        exec_params: ExecutionParameters,
        data_set: DataSetPythonOnly,
    ) -> TChallengePythonOnlyAnswer: ...


@dataclass(frozen=True)
class ChallengeMethodPythonOnlyRegistration:
    strategy_name: str
    language: str
    interface: str
    numerical_tolerance: NumericalToleranceExpectations
    requires_gpu: bool
    delegate: IChallengeMethodPythonOnly


# endregion


def populate_data_set_python_only(
        exec_params: ExecutionParameters,
        data_size: DataSetDescription,
) -> DataSetPythonOnlyWithAnswer:
    raw_data = populate_data_set_generic(
        CalcEngine.PYTHON_ONLY, exec_params, data_size)
    assert raw_data.num_data_points == data_size.num_data_points
    return DataSetPythonOnlyWithAnswer(
        data_size=data_size,
        data=DataSetDataPythonOnly(
            src_num_partitions=raw_data.src_num_partitions,
            agg_tgt_num_partitions_1_level=raw_data.tgt_num_partitions_1_level,
            agg_tgt_num_partitions_2_level=raw_data.tgt_num_partitions_2_level,
            df_src=raw_data.df_src,
        ),
        answer=DataSetAnswer(
            vanilla_answer=raw_data.vanilla_answer,
            bilevel_answer=raw_data.bilevel_answer,
            conditional_answer=raw_data.conditional_answer,
        ),
    )
