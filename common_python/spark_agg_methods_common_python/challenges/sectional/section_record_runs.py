import os
from dataclasses import dataclass

from spark_agg_methods_common_python.perf_test_common import (
    CalcEngine, ChallengeMethodRegistrationBase, PersistedRunResultBase, RunResultBase, RunResultFileWriterBase,
    SolutionInterface, SolutionLanguage,
)


@dataclass(frozen=True)
class SectionRunResult(RunResultBase):
    status: str
    section_maximum: int


@dataclass(frozen=True)
class SectionPersistedRunResult(PersistedRunResultBase[SolutionInterface], SectionRunResult):
    num_students: int


def regressor_from_run_result(
        result: SectionPersistedRunResult,
) -> int:
    return result.num_students


class SectionPythonRunResultFileWriter(RunResultFileWriterBase):

    def __init__(
            self,
            engine: CalcEngine,
            rel_log_file_path: str,
    ):
        super().__init__(
            log_file_path=os.path.abspath(rel_log_file_path),
            language=SolutionLanguage.PYTHON,
            engine=engine,
            persisted_row_type=SectionPersistedRunResult,
        )

    def __enter__(self) -> 'SectionPythonRunResultFileWriter':
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def write_run_result(
            self,
            challenge_method_registration: ChallengeMethodRegistrationBase,
            run_result: RunResultBase,
    ) -> None:
        assert isinstance(run_result, SectionRunResult)
        assert self.engine == challenge_method_registration.engine
        self._persist_run_result(SectionPersistedRunResult(
            num_source_rows=run_result.num_source_rows,
            elapsed_time=run_result.elapsed_time,
            num_output_rows=run_result.num_output_rows,
            finished_at=run_result.finished_at,

            language=self.language,
            engine=self.engine,
            interface=challenge_method_registration.interface,
            strategy_name=challenge_method_registration.strategy_name,

            status=run_result.status,
            section_maximum=run_result.section_maximum,

            num_students=run_result.num_output_rows,

        ))
