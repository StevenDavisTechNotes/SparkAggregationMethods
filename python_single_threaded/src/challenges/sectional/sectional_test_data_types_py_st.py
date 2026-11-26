from dataclasses import dataclass
from typing import Protocol

from spark_agg_methods_common_python.challenges.sectional.sectional_test_data_types import (
    SectionalChallengeMethodRegistrationBase, SectionalDataSetBase, SectionalExecutionParametersBase,
    TChallengePythonAnswer,
)
from spark_agg_methods_common_python.perf_test_common import SolutionInterfacePythonST


@dataclass(frozen=True)
class SectionalExecutionParametersPyOnly(SectionalExecutionParametersBase):
    pass


@dataclass(frozen=True)
class SectionalDataSetPyST(SectionalDataSetBase):
    pass


class ISectionalChallengeMethodPythonST(Protocol):
    def __call__(
        self,
        *,
        data_set: SectionalDataSetPyST,
        exec_params: SectionalExecutionParametersPyOnly,
    ) -> TChallengePythonAnswer: ...


@dataclass(frozen=True)
class SectionalChallengeMethodPythonSingleThreadedRegistration(
    SectionalChallengeMethodRegistrationBase[SolutionInterfacePythonST, ISectionalChallengeMethodPythonST]
):
    interface: SolutionInterfacePythonST
    delegate: ISectionalChallengeMethodPythonST
