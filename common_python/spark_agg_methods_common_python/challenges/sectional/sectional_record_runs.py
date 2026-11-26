from dataclasses import dataclass

from spark_agg_methods_common_python.perf_test_common import (
    CalcEngine, ChallengeMethodRegistrationBase, PersistedRunResultBase, RunResultBase, RunResultFileWriterBase,
    SolutionInterface, SolutionLanguage,
)


@dataclass(frozen=True)
class SectionalRunResult(RunResultBase):
    status: str
    section_maximum: int


@dataclass(frozen=True)
class SectionalPersistedRunResult(PersistedRunResultBase[SolutionInterface], SectionalRunResult):
    num_students: int


def regressor_from_run_result(
        result: SectionalPersistedRunResult,
) -> int:
    return result.num_students


class SectionalPythonRunResultFileWriter(RunResultFileWriterBase):

    def __init__(
            self,
            *,
            engine: CalcEngine,
            log_file_name: str,
    ):
        super().__init__(
            log_file_name=log_file_name,
            language=SolutionLanguage.PYTHON,
            engine=engine,
            persisted_row_type=SectionalPersistedRunResult,
        )

    def __enter__(self) -> "SectionalPythonRunResultFileWriter":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def write_run_result(
            self,
            challenge_method_registration: ChallengeMethodRegistrationBase,
            run_result: RunResultBase,
    ) -> None:
        assert isinstance(run_result, SectionalRunResult)
        if self.engine != challenge_method_registration.engine:
            raise ValueError(
                f"Writer for engine {self.engine} cannot record a result for engine "
                f"{challenge_method_registration.engine}"
            )
        self._persist_run_result(SectionalPersistedRunResult(
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
