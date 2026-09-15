import os
from dataclasses import dataclass

from spark_agg_methods_common_python.perf_test_common import (
    CalcEngine, ChallengeMethodRegistrationBase, PersistedRunResultBase,
    RunResultBase, RunResultFileWriterBase, SolutionInterfacePython,
    SolutionLanguage,
)


@dataclass(frozen=True)
class DedupeRunResult(RunResultBase):
    status: str
    num_sources: int
    num_people_actual: int
    data_size_exponent: int
    num_people_found: int
    in_cloud_mode: bool
    can_assume_no_duplicates_per_partition: bool


@dataclass(frozen=True)
class DedupePersistedRunResult(PersistedRunResultBase[SolutionInterfacePython], DedupeRunResult):
    pass


def regressor_from_run_result(
        result: DedupePersistedRunResult,
) -> int:
    return result.num_source_rows


class DedupePythonRunResultFileWriter(RunResultFileWriterBase):

    def __init__(
            self,
            engine: CalcEngine,
            rel_log_file_path: str,
    ):
        super().__init__(
            log_file_path=os.path.abspath(rel_log_file_path),
            language=SolutionLanguage.PYTHON,
            engine=engine,
            persisted_row_type=DedupePersistedRunResult,
        )

    def __enter__(self) -> 'DedupePythonRunResultFileWriter':
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def write_run_result(
            self,
            challenge_method_registration: ChallengeMethodRegistrationBase,
            run_result: RunResultBase,
    ) -> None:
        assert isinstance(run_result, DedupeRunResult)
        assert self.engine == challenge_method_registration.engine
        self._persist_run_result(DedupePersistedRunResult(
            num_source_rows=run_result.num_source_rows,
            elapsed_time=run_result.elapsed_time,
            num_output_rows=run_result.num_output_rows,
            finished_at=run_result.finished_at,

            language=self.language,
            engine=self.engine,
            interface=challenge_method_registration.interface,
            strategy_name=challenge_method_registration.strategy_name,

            status=run_result.status,
            num_sources=run_result.num_sources,
            num_people_actual=run_result.num_people_actual,
            data_size_exponent=run_result.data_size_exponent,
            num_people_found=run_result.num_people_found,
            in_cloud_mode=run_result.in_cloud_mode,
            can_assume_no_duplicates_per_partition=run_result.can_assume_no_duplicates_per_partition,
        ))
