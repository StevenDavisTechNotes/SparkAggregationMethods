import datetime as dt
import os
from dataclasses import dataclass

from spark_agg_methods_common_python.perf_test_common import (
    CalcEngine, ChallengeMethodRegistrationBase, PersistedRunResultBase, PersistedRunResultLog, RunResultBase,
    RunResultFileWriterBase, SolutionInterface, SolutionInterfaceScalaSpark, SolutionLanguage, parse_interface_python,
)

PYTHON_DASK_RUN_LOG_FILE_PATH = 'results/vanilla_dask_runs.csv'
PYTHON_PYSPARK_RUN_LOG_FILE_PATH = 'results/vanilla_pyspark_runs.csv'
PYTHON_ONLY_RUN_LOG_FILE_PATH = 'results/vanilla_python_only_runs.csv'
SCALA_RUN_LOG_FILE_PATH = '../results/Scala/vanilla_runs_scala.csv'


@dataclass(frozen=True)
class VanillaRunResult(RunResultBase):
    pass


@dataclass(frozen=True)
class VanillaPersistedRunResult(PersistedRunResultBase[SolutionInterface], VanillaRunResult):
    pass


def regressor_from_run_result(
        result: PersistedRunResultBase,
) -> int:
    assert isinstance(result, VanillaPersistedRunResult)
    return result.num_source_rows


class VanillaPersistedRunResultLog(PersistedRunResultLog[VanillaPersistedRunResult]):

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
            result: VanillaPersistedRunResult,
    ) -> bool:
        assert isinstance(result, VanillaPersistedRunResult)
        return result.num_output_rows == 9


class VanillaPythonPersistedRunResultLog(VanillaPersistedRunResultLog):

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
    ) -> VanillaPersistedRunResult | None:
        strategy_name, interface, num_source_rows, \
            elapsed_time, num_output_rows, \
            finished_at, result_engine  \
            = tuple(fields)
        result = VanillaPersistedRunResult(
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


class VanillaScalaPersistedRunResultLog(VanillaPersistedRunResultLog):

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
    ) -> VanillaPersistedRunResult | None:
        outcome, strategy_name, interface, expected_size, returnedSize, elapsed_time = tuple(
            fields)
        if outcome != 'success':
            print("Excluding line: " + line)
            return None
        if returnedSize != '9':
            print("Excluding line: " + line)
            return None
        result = VanillaPersistedRunResult(
            strategy_name=strategy_name,
            engine=self.engine,
            language=self.language,
            interface=SolutionInterfaceScalaSpark(interface),
            num_source_rows=int(expected_size),
            elapsed_time=float(elapsed_time),
            num_output_rows=-1,
            finished_at=None,
        )
        return result


class VanillaPythonRunResultFileWriter(RunResultFileWriterBase):

    def __init__(
            self,
            engine: CalcEngine,
            rel_log_file_path: str,
    ):
        super().__init__(
            log_file_path=os.path.abspath(rel_log_file_path),
            language=SolutionLanguage.PYTHON,
            engine=engine,
            persisted_row_type=VanillaPersistedRunResult,
        )

    def __enter__(self) -> 'VanillaPythonRunResultFileWriter':
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def write_run_result(
            self,
            challenge_method_registration: ChallengeMethodRegistrationBase,
            run_result: RunResultBase,
    ) -> None:
        assert isinstance(run_result, VanillaRunResult)
        assert self.engine == challenge_method_registration.engine
        self._persist_run_result(VanillaPersistedRunResult(
            num_source_rows=run_result.num_source_rows,
            elapsed_time=run_result.elapsed_time,
            num_output_rows=run_result.num_output_rows,
            finished_at=dt.datetime.now().isoformat(),

            language=SolutionLanguage.PYTHON,
            engine=challenge_method_registration.engine,
            interface=challenge_method_registration.interface,
            strategy_name=challenge_method_registration.strategy_name,
        ))
