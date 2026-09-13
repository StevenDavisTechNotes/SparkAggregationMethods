from dataclasses import dataclass
from typing import Literal, Protocol

import pandas as pd
from spark_agg_methods_common_python.challenges.six_field_test_data.six_test_data_types import (
    SixTestDataChallengeMethodRegistrationBase, SixTestDataSetDescription,
    SixTestExecutionParameters, six_derive_source_test_data_file_path,
)
from spark_agg_methods_common_python.perf_test_common import (
    CalcEngine, NumericalToleranceExpectations, SolutionInterfacePythonST,
    SolutionLanguage,
)


@dataclass(frozen=True)
class SixDataSetDataPythonST():
    source_file_path_parquet: str


@dataclass(frozen=True)
class SixDataSetPythonST():
    data_description: SixTestDataSetDescription
    data: SixDataSetDataPythonST


TChallengePythonSTAnswer = (
    tuple[Literal["infeasible"], str]
    | pd.DataFrame
)


class IChallengeMethodPythonST(Protocol):
    def __call__(
        self,
        *,
        exec_params: SixTestExecutionParameters,
        data_set: SixDataSetPythonST,
    ) -> TChallengePythonSTAnswer: ...


@dataclass(frozen=True)
class ChallengeMethodPythonSingleThreadedRegistration(
    SixTestDataChallengeMethodRegistrationBase[
        SolutionInterfacePythonST, IChallengeMethodPythonST
    ]
):
    strategy_name_2018: str | None
    strategy_name: str
    language: SolutionLanguage
    engine: CalcEngine
    interface: SolutionInterfacePythonST
    numerical_tolerance: NumericalToleranceExpectations
    requires_gpu: bool
    delegate: IChallengeMethodPythonST


def six_prepare_data_set_python_single_threaded(
        exec_params: SixTestExecutionParameters,
        data_description: SixTestDataSetDescription,
) -> SixDataSetDataPythonST:
    source_file_paths = six_derive_source_test_data_file_path(
        data_description=data_description,
    )
    return SixDataSetDataPythonST(
        source_file_path_parquet=source_file_paths.source_file_path_parquet_single_file,
    )
