from dataclasses import dataclass
from typing import Protocol

from spark_agg_methods_common_python.challenges.sectional.section_test_data_types import (
    SectionChallengeMethodRegistrationBase, SectionDataSetBase, SectionExecutionParametersBase, TChallengePythonAnswer,
)
from spark_agg_methods_common_python.perf_test_common import SolutionInterfaceDask


@dataclass(frozen=True)
class SectionExecutionParametersDask(SectionExecutionParametersBase):
    pass


@dataclass(frozen=True)
class SectionDataSetDask(SectionDataSetBase):
    pass


class ISectionChallengeMethodPythonDask(Protocol):
    def __call__(
        self,
        *,
        # dask_client: DaskClient | None,
        data_set: SectionDataSetDask,
    ) -> TChallengePythonAnswer: ...


@dataclass(frozen=True)
class SectionChallengeMethodDaskRegistration(
    SectionChallengeMethodRegistrationBase[
        SolutionInterfaceDask, ISectionChallengeMethodPythonDask
    ]
):
    interface: SolutionInterfaceDask
    delegate: ISectionChallengeMethodPythonDask
