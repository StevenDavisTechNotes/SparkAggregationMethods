from dataclasses import dataclass
from typing import Protocol

from spark_agg_methods_common_python.challenges.sectional.section_test_data_types import (
    SectionChallengeMethodRegistrationBase, SectionDataSetBase, SectionExecutionParametersBase, TChallengePythonAnswer,
)
from spark_agg_methods_common_python.perf_test_common import SolutionInterfacePythonOnly


@dataclass(frozen=True)
class SectionExecutionParametersPyOnly(SectionExecutionParametersBase):
    pass


@dataclass(frozen=True)
class SectionDataSetPyOnly(SectionDataSetBase):
    pass


class ISectionChallengeMethodPythonOnly(Protocol):
    def __call__(
        self,
        *,
        data_set: SectionDataSetPyOnly,
        exec_params: SectionExecutionParametersPyOnly,
    ) -> TChallengePythonAnswer: ...


@dataclass(frozen=True)
class SectionChallengeMethodPythonOnlyRegistration(
    SectionChallengeMethodRegistrationBase[SolutionInterfacePythonOnly, ISectionChallengeMethodPythonOnly]
):
    interface: SolutionInterfacePythonOnly
    delegate: ISectionChallengeMethodPythonOnly
