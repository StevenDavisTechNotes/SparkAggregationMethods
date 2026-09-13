from dataclasses import dataclass
from typing import Protocol

from spark_agg_methods_common_python.challenges.sectional.section_test_data_types import (
    SectionChallengeMethodRegistrationBase, SectionDataSetBase,
    SectionExecutionParametersBase, TChallengePythonAnswer,
)
from spark_agg_methods_common_python.perf_test_common import (
    SolutionInterfacePythonST,
)


@dataclass(frozen=True)
class SectionExecutionParametersPyOnly(SectionExecutionParametersBase):
    pass


@dataclass(frozen=True)
class SectionDataSetPyST(SectionDataSetBase):
    pass


class ISectionChallengeMethodPythonST(Protocol):
    def __call__(
        self,
        *,
        data_set: SectionDataSetPyST,
        exec_params: SectionExecutionParametersPyOnly,
    ) -> TChallengePythonAnswer: ...


@dataclass(frozen=True)
class SectionChallengeMethodPythonSingleThreadedRegistration(
    SectionChallengeMethodRegistrationBase[SolutionInterfacePythonST, ISectionChallengeMethodPythonST]
):
    interface: SolutionInterfacePythonST
    delegate: ISectionChallengeMethodPythonST
