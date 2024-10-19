import datetime as dt
import os
from dataclasses import dataclass

from src.perf_test_common import (
    CalcEngine, ChallengeMethodRegistrationBase, PersistedRunResultBase, PersistedRunResultLog, RunResultBase,
    RunResultFileWriterBase, SolutionLanguage, TSolutionInterface, parse_interface_python,
)

PYTHON_PYSPARK_RUN_LOG_FILE_PATH = 'results/conditional_pyspark_runs.csv'
PYTHON_DASK_RUN_LOG_FILE_PATH = 'results/conditional_dask_runs.csv'
FINAL_REPORT_FILE_PATH = 'results/cond_results.csv'
EXPECTED_SIZES = [3 * 3 * 10**x for x in range(1, 5 + 2)]


@dataclass(frozen=True)
class ConditionalRunResult(RunResultBase):
    # for RunResultBase
    num_source_rows: int
    elapsed_time: float
    num_output_rows: int
    finished_at: str | None


@dataclass(frozen=True)
class ConditionalPersistedRunResult(PersistedRunResultBase[TSolutionInterface], ConditionalRunResult):
    # for RunResultBase
    num_source_rows: int
    elapsed_time: float
    num_output_rows: int
    finished_at: str | None
    # for PersistedRunResultBase
    language: SolutionLanguage
    engine: CalcEngine
    interface: TSolutionInterface
    strategy_name: str


def derive_run_log_file_path(
        engine: CalcEngine,
) -> str:
    match engine:
        case  CalcEngine.PYSPARK:
            run_log = PYTHON_PYSPARK_RUN_LOG_FILE_PATH
        case CalcEngine.DASK:
            run_log = PYTHON_DASK_RUN_LOG_FILE_PATH
        case _:
            raise ValueError(f"Unknown engine: {engine}")
    return os.path.abspath(run_log)


def regressor_from_run_result(
        result: PersistedRunResultBase,
) -> int:
    assert isinstance(result, ConditionalPersistedRunResult)
    return result.num_source_rows


class ConditionalPersistedRunResultLog(PersistedRunResultLog[ConditionalPersistedRunResult]):
    def __init__(
            self,
            engine: CalcEngine,
    ):
        self.engine = engine
        match engine:
            case CalcEngine.DASK | CalcEngine.PYSPARK | CalcEngine.PYTHON_ONLY:
                language = SolutionLanguage.PYTHON
            case CalcEngine.SCALA_SPARK:
                language = SolutionLanguage.SCALA
            case _:
                raise ValueError(f"Unknown engine: {engine}")
        super().__init__(
            engine=engine,
            language=language,
        )

    def derive_run_log_file_path(
            self,
    ) -> str | None:
        return derive_run_log_file_path(self.engine)

    def result_looks_valid(
            self,
            result: PersistedRunResultBase,
    ) -> bool:
        assert isinstance(result, ConditionalPersistedRunResult)
        return result.num_output_rows == 9

    def read_regular_line_from_log_file(
            self,
            line: str,
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

    def read_scala_line_from_log_file(
            self,
            line: str,
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

    def read_line_from_log_file(
            self,
            i_line: int,
            line: str,
            last_header_line: list[str],
            fields: list[str],
    ) -> ConditionalPersistedRunResult | None:
        match self.engine:
            case CalcEngine.DASK | CalcEngine.PYSPARK | CalcEngine.PYTHON_ONLY:
                return self.read_regular_line_from_log_file(line, fields)
            case CalcEngine.SCALA_SPARK:
                return self.read_scala_line_from_log_file(line, fields)


class ConditionalPythonRunResultFileWriter(RunResultFileWriterBase):

    def __init__(
            self,
            engine: CalcEngine,
    ):
        super().__init__(
            file_name=derive_run_log_file_path(engine),
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
