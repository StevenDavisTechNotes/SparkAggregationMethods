import datetime as dt
import os
from dataclasses import dataclass

from spark_agg_methods_common_python.perf_test_common import (
    CalcEngine, ChallengeMethodRegistrationBase, PersistedRunResultBase, RunResultBase, RunResultFileWriterBase,
    SolutionLanguage, TSolutionInterface,
)


@dataclass(frozen=True)
class ConditionalRunResult(RunResultBase):
    pass


@dataclass(frozen=True)
class ConditionalPersistedRunResult(PersistedRunResultBase[TSolutionInterface], ConditionalRunResult):
    pass


def regressor_from_run_result(
        result: PersistedRunResultBase,
) -> int:
    assert isinstance(result, ConditionalPersistedRunResult)
    return result.num_source_rows


class ConditionalPythonRunResultFileWriter(RunResultFileWriterBase):

    def __init__(
            self,
            engine: CalcEngine,
            rel_log_file_path: str,
    ):
        super().__init__(
            log_file_path=os.path.abspath(rel_log_file_path),
            language=SolutionLanguage.PYTHON,
            engine=engine,
            persisted_row_type=ConditionalPersistedRunResult,
        )

    def __enter__(self) -> 'ConditionalPythonRunResultFileWriter':
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def write_run_result(
            self,
            challenge_method_registration: ChallengeMethodRegistrationBase,
            run_result: RunResultBase,
    ) -> None:
        assert isinstance(run_result, ConditionalRunResult)
        assert self.engine == challenge_method_registration.engine
        self._persist_run_result(ConditionalPersistedRunResult(
            num_source_rows=run_result.num_source_rows,
            elapsed_time=run_result.elapsed_time,
            num_output_rows=run_result.num_output_rows,
            finished_at=dt.datetime.now().isoformat(),

            language=SolutionLanguage.PYTHON,
            engine=challenge_method_registration.engine,
            interface=challenge_method_registration.interface,
            strategy_name=challenge_method_registration.strategy_name,
        ))
