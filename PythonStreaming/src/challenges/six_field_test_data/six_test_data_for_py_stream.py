from dataclasses import dataclass
from typing import Literal, Protocol

import pandas as pd
from spark_agg_methods_common_python.challenges.six_field_test_data.six_test_data_types import (
    SixTestDataChallengeMethodRegistrationBase, SixTestDataSetDescription,
    SixTestExecutionParameters, six_derive_source_test_data_file_path,
)
from spark_agg_methods_common_python.perf_test_common import (
    CalcEngine, NumericalToleranceExpectations,
    SolutionInterfacePythonStreaming, SolutionLanguage,
)


@dataclass(frozen=True)
class SixDataSetDataPythonStreaming():
    source_file_path_parquet: str


@dataclass(frozen=True)
class SixDataSetPythonStreaming():
    data_description: SixTestDataSetDescription
    data: SixDataSetDataPythonStreaming


TChallengePythonStreamAnswer = (
    tuple[Literal["infeasible"], str]
    | pd.DataFrame
)


class IChallengeMethodPythonStreaming(Protocol):
    def __call__(
        self,
        *,
        exec_params: SixTestExecutionParameters,
        data_set: SixDataSetPythonStreaming,
    ) -> TChallengePythonStreamAnswer: ...


@dataclass(frozen=True)
class ChallengeMethodPythonStreamingRegistration(
    SixTestDataChallengeMethodRegistrationBase[
        SolutionInterfacePythonStreaming, IChallengeMethodPythonStreaming
    ]
):
    strategy_name_2018: str | None
    strategy_name: str
    language: SolutionLanguage
    engine: CalcEngine
    interface: SolutionInterfacePythonStreaming
    numerical_tolerance: NumericalToleranceExpectations
    requires_gpu: bool
    delegate: IChallengeMethodPythonStreaming


def six_prepare_data_set_python_streaming(
        exec_params: SixTestExecutionParameters,
        data_description: SixTestDataSetDescription,
) -> SixDataSetDataPythonStreaming:
    source_file_paths = six_derive_source_test_data_file_path(
        data_description=data_description,
    )
    return SixDataSetDataPythonStreaming(
        source_file_path_parquet=source_file_paths.source_file_path_parquet_single_file,
    )
