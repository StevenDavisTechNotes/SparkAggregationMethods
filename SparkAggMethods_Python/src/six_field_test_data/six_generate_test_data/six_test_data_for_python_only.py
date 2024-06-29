from dataclasses import dataclass
from typing import Literal, Protocol

import pandas as pd

from perf_test_common import CalcEngine
from six_field_test_data.six_test_data_types import (DataSetAnswer,
                                                     DataSetDescription,
                                                     ExecutionParameters,
                                                     populate_data_set_generic)

# region PythonOnly version


@dataclass(frozen=True)
class DataSetDataPythonOnly():
    SrcNumPartitions: int
    AggTgtNumPartitions: int
    dfSrc: pd.DataFrame


@dataclass(frozen=True)
class DataSetPythonOnly():
    description: DataSetDescription
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
    delegate: IChallengeMethodPythonOnly


# endregion


def populate_data_set_python_only(
        exec_params: ExecutionParameters,
        size_code: str,
        num_grp_1: int,
        num_grp_2: int,
        repetition: int,
) -> DataSetPythonOnlyWithAnswer:
    raw_data = populate_data_set_generic(
        CalcEngine.PYTHON_ONLY, exec_params, num_grp_1, num_grp_2, repetition)
    assert raw_data.num_data_points == num_grp_1 * num_grp_2 * repetition
    return DataSetPythonOnlyWithAnswer(
        description=DataSetDescription(
            NumDataPoints=num_grp_1 * num_grp_2 * repetition,
            NumGroups=num_grp_1,
            NumSubGroups=num_grp_2,
            SizeCode=size_code,
            RelativeCardinalityBetweenGroupings=num_grp_2 // num_grp_1,
        ),
        data=DataSetDataPythonOnly(
            SrcNumPartitions=raw_data.src_num_partitions,
            AggTgtNumPartitions=raw_data.tgt_num_partitions,
            dfSrc=raw_data.dfSrc,
        ),
        answer=DataSetAnswer(
            vanilla_answer=raw_data.vanilla_answer,
            bilevel_answer=raw_data.bilevel_answer,
            conditional_answer=raw_data.conditional_answer,
        ),
    )
