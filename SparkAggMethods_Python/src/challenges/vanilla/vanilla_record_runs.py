import datetime as dt
import os
from dataclasses import dataclass

from src.perf_test_common import (
    CalcEngine, ChallengeMethodRegistrationBase, PersistedRunResultBase, PersistedRunResultLog, RunResultBase,
    RunResultFileWriterBase, SolutionInterface, SolutionInterfaceScalaSpark, SolutionLanguage, parse_interface_python,
)
from src.utils.utils import root_folder_abs_path

PYTHON_DASK_RUN_LOG_FILE_PATH = 'results/vanilla_dask_runs.csv'
PYTHON_PYSPARK_RUN_LOG_FILE_PATH = 'results/vanilla_pyspark_runs.csv'
PYTHON_ONLY_RUN_LOG_FILE_PATH = 'results/vanilla_python_only_runs.csv'
SCALA_RUN_LOG_FILE_PATH = '../results/Scala/vanilla_runs_scala.csv'
FINAL_REPORT_FILE_PATH = 'results/vanilla_results.csv'
EXPECTED_SIZES = [3 * 3 * 10**x for x in range(0, 6 + 1)]


@dataclass(frozen=True)
class VanillaRunResult(RunResultBase):
    # for RunResultBase
    num_source_rows: int
    elapsed_time: float
    num_output_rows: int
    finished_at: str | None


@dataclass(frozen=True)
class VanillaPersistedRunResult(PersistedRunResultBase[SolutionInterface], VanillaRunResult):
    # for RunResultBase
    num_source_rows: int
    elapsed_time: float
    num_output_rows: int
    finished_at: str | None
    # for PersistedRunResultBase
    language: SolutionLanguage
    engine: CalcEngine
    interface: SolutionInterface
    strategy_name: str


def derive_run_log_file_path_for_recording(
    engine: CalcEngine,
) -> str:
    match engine:
        case CalcEngine.DASK:
            run_log = PYTHON_DASK_RUN_LOG_FILE_PATH
        case CalcEngine.PYSPARK:
            run_log = PYTHON_PYSPARK_RUN_LOG_FILE_PATH
        case CalcEngine.PYTHON_ONLY:
            run_log = PYTHON_ONLY_RUN_LOG_FILE_PATH
        case CalcEngine.SCALA_SPARK:
            run_log = SCALA_RUN_LOG_FILE_PATH
        case _:
            raise ValueError(f"Unknown engine: {engine}")
    return os.path.join(
        root_folder_abs_path(),
        run_log)


class VanillaPersistedRunResultLog(PersistedRunResultLog[VanillaPersistedRunResult]):
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
        match self.engine:
            case CalcEngine.DASK | CalcEngine.PYSPARK | CalcEngine.PYTHON_ONLY:
                return os.path.join(
                    root_folder_abs_path(),
                    derive_run_log_file_path_for_recording(self.engine),
                )
            case CalcEngine.SCALA_SPARK:
                return None
            case _:
                raise ValueError(f"Unknown engine: {self.engine}")

    def result_looks_valid(
            self,
            result: VanillaPersistedRunResult,
    ) -> bool:
        assert isinstance(result, VanillaPersistedRunResult)
        return result.num_output_rows == 9

    def read_regular_line_from_log_file(
            self,
            line: str,
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

    def read_scala_line_from_log_file(
            self,
            line: str,
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

    def read_line_from_log_file(
            self,
            i_line: int,
            line: str,
            last_header_line: list[str],
            fields: list[str],
    ) -> VanillaPersistedRunResult | None:
        match self.engine:
            case CalcEngine.DASK | CalcEngine.PYSPARK | CalcEngine.PYTHON_ONLY:
                return self.read_regular_line_from_log_file(line, fields)
            case CalcEngine.SCALA_SPARK:
                return self.read_scala_line_from_log_file(line, fields)


def regressor_from_run_result(
        result: PersistedRunResultBase,
) -> int:
    assert isinstance(result, VanillaPersistedRunResult)
    return result.num_source_rows


class VanillaPythonRunResultFileWriter(RunResultFileWriterBase):

    def __init__(
            self,
            engine: CalcEngine,
    ):
        super().__init__(
            file_name=__class__.derive_run_log_file_path_for_recording(engine),
            language=SolutionLanguage.PYTHON,
            engine=engine,
            persisted_row_type=VanillaPersistedRunResult,
        )

    def __enter__(self) -> 'VanillaPythonRunResultFileWriter':
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @staticmethod
    def derive_run_log_file_path_for_recording(
            engine: CalcEngine,
    ) -> str:
        return os.path.join(
            root_folder_abs_path(),
            derive_run_log_file_path_for_recording(engine),
        )

    def write_run_result(
            self,
            challenge_method_registration: ChallengeMethodRegistrationBase,
            run_result: RunResultBase,
    ) -> None:
        assert isinstance(run_result, VanillaRunResult)
        assert self.engine == challenge_method_registration.engine
        # print("%s,%s,%d,%f,%d,%s,%s," % (
        #     challenge_method_registration.strategy_name,
        #     challenge_method_registration.interface,
        #     result.num_source_rows, result.elapsed_time, result.num_output_rows,
        #     challenge_method_registration.engine.value,
        #     dt.datetime.now().isoformat(),
        # ), file=self.file)
        # self.file.flush()
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
        self.file.flush()
