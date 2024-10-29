import datetime as dt
import os
from dataclasses import dataclass

from spark_agg_methods_common_python.perf_test_common import (
    CalcEngine, ChallengeMethodRegistrationBase, PersistedRunResultBase,
    PersistedRunResultLog, RunResultBase, RunResultFileWriterBase,
    SolutionLanguage, TSolutionInterface, parse_interface_python)


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


class ConditionalPersistedRunResultLog(PersistedRunResultLog[ConditionalPersistedRunResult]):

    def __init__(
            self,
            engine: CalcEngine,
            language: SolutionLanguage,
            rel_log_file_path: str,
    ):
        super().__init__(
            engine=engine,
            language=language,
            log_file_path=os.path.abspath(rel_log_file_path),
        )

    def result_looks_valid(
            self,
            result: PersistedRunResultBase,
    ) -> bool:
        assert isinstance(result, ConditionalPersistedRunResult)
        return result.num_output_rows == 9


class ConditionalPythonPersistedRunResultLog(ConditionalPersistedRunResultLog):

    def __init__(
            self,
            engine: CalcEngine,
            rel_log_file_path: str,
    ):
        super().__init__(
            engine=engine,
            language=SolutionLanguage.PYTHON,
            rel_log_file_path=rel_log_file_path,
        )

    def read_line_from_log_file(
            self,
            i_line: int,
            line: str,
            last_header_line: list[str],
            fields: list[str],
    ) -> ConditionalPersistedRunResult | None:
        strategy_name, interface, num_source_rows, \
            elapsed_time, num_output_rows, \
            finished_at, engine  \
            = tuple(fields)
        assert engine == self.engine.value
        result = ConditionalPersistedRunResult(
            strategy_name=strategy_name,
            language=self.language,
            engine=self.engine,
            interface=parse_interface_python(interface, self.engine),
            num_source_rows=int(num_source_rows),
            elapsed_time=float(elapsed_time),
            num_output_rows=int(num_output_rows),
            finished_at=finished_at,
        )
        return result


class ConditionalScalaPersistedRunResultLog(ConditionalPersistedRunResultLog):

    def __init__(
            self,
            engine: CalcEngine,
            rel_log_file_path: str,
    ):
        super().__init__(
            engine=engine,
            language=SolutionLanguage.SCALA,
            rel_log_file_path=rel_log_file_path,
        )

    def read_line_from_log_file(
            self,
            i_line: int,
            line: str,
            last_header_line: list[str],
            fields: list[str],
    ) -> ConditionalPersistedRunResult | None:
        outcome, strategy_name, interface, expected_size, returnedSize, elapsed_time = tuple(
            fields)
        if outcome != 'success':
            print("Excluding line: " + line)
            return None
        if returnedSize != '9':
            print("Excluding line: " + line)
            return None
        result = ConditionalPersistedRunResult(
            strategy_name=strategy_name,
            engine=self.engine,
            language=self.language,
            interface=parse_interface_python(interface, self.engine),
            num_source_rows=int(expected_size),
            elapsed_time=float(elapsed_time),
            num_output_rows=-1,
            finished_at=None,
        )
        return result


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
        self.file.flush()
