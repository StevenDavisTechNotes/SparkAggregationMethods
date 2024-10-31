from dataclasses import dataclass
from typing import Literal, Protocol

import pandas as pd
from spark_agg_methods_common_python.challenges.six_field_test_data.six_test_data_types import (
    SixTestDataChallengeMethodRegistrationBase, SixTestDataSetDescription, SixTestExecutionParameters,
    six_derive_source_test_data_file_path,
)
from spark_agg_methods_common_python.perf_test_common import (
    CalcEngine, NumericalToleranceExpectations, SolutionInterfacePythonOnly, SolutionLanguage,
)


@dataclass(frozen=True)
class DataSetDataPythonOnly():
    df_src: pd.DataFrame


@dataclass(frozen=True)
class SixDataSetPythonOnly():
    data_description: SixTestDataSetDescription
    data: DataSetDataPythonOnly


TChallengePythonOnlyAnswer = Literal["infeasible"] | pd.DataFrame


class IChallengeMethodPythonOnly(Protocol):
    def __call__(
        self,
        *,
        exec_params: SixTestExecutionParameters,
        data_set: SixDataSetPythonOnly,
    ) -> TChallengePythonOnlyAnswer: ...


@dataclass(frozen=True)
class ChallengeMethodPythonOnlyRegistration(
    SixTestDataChallengeMethodRegistrationBase[
        SolutionInterfacePythonOnly, IChallengeMethodPythonOnly
    ]
):
    strategy_name_2018: str | None
    strategy_name: str
    language: SolutionLanguage
    engine: CalcEngine
    interface: SolutionInterfacePythonOnly
    numerical_tolerance: NumericalToleranceExpectations
    requires_gpu: bool
    delegate: IChallengeMethodPythonOnly


def six_populate_data_set_python_only(
        exec_params: SixTestExecutionParameters,
        data_description: SixTestDataSetDescription,
) -> DataSetDataPythonOnly:
    num_grp_1 = data_description.num_grp_1
    num_grp_2 = data_description.num_grp_2
    points_per_index = data_description.points_per_index
    num_source_rows = num_grp_1 * num_grp_2 * points_per_index

    source_file_name_parquet, source_file_name_csv = six_derive_source_test_data_file_path(
        data_description=data_description,
    )
    df_src = pd.read_parquet(source_file_name_parquet)
    assert len(df_src) == num_source_rows
    return DataSetDataPythonOnly(
        df_src=df_src,
    )
