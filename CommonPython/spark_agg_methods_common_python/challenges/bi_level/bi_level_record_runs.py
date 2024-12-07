import datetime as dt
import os
from dataclasses import dataclass

from spark_agg_methods_common_python.perf_test_common import (
    CalcEngine, ChallengeMethodRegistrationBase, PersistedRunResultBase,
    RunResultBase, RunResultFileWriterBase, SolutionInterfacePython,
    SolutionLanguage,
)


@dataclass(frozen=True)
class BiLevelRunResult(RunResultBase):
    relative_cardinality_between_groupings: int


@dataclass(frozen=True)
class BiLevelPersistedRunResult(PersistedRunResultBase[SolutionInterfacePython], BiLevelRunResult):
    pass


def regressor_from_run_result(
        result: PersistedRunResultBase,
) -> int:
    assert isinstance(result, BiLevelPersistedRunResult)
    return result.relative_cardinality_between_groupings


class BiLevelPythonRunResultFileWriter(RunResultFileWriterBase):

    def __init__(
            self,
            engine: CalcEngine,
            rel_log_file_path: str,
    ):
        super().__init__(
            log_file_path=os.path.abspath(rel_log_file_path),
            language=SolutionLanguage.PYTHON,
            engine=engine,
            persisted_row_type=BiLevelPersistedRunResult,
        )

    def __enter__(self) -> 'BiLevelPythonRunResultFileWriter':
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def write_run_result(
            self,
            challenge_method_registration: ChallengeMethodRegistrationBase,
            run_result: RunResultBase,
    ) -> None:
        assert isinstance(run_result, BiLevelRunResult)
        assert self.engine == challenge_method_registration.engine
        self._persist_run_result(BiLevelPersistedRunResult(
            num_source_rows=run_result.num_source_rows,
            elapsed_time=run_result.elapsed_time,
            num_output_rows=run_result.num_output_rows,
            finished_at=dt.datetime.now().isoformat(),

            relative_cardinality_between_groupings=run_result.relative_cardinality_between_groupings,

            language=SolutionLanguage.PYTHON,
            engine=challenge_method_registration.engine,
            interface=challenge_method_registration.interface,
            strategy_name=challenge_method_registration.strategy_name,
        ))
