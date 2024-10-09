import os
from abc import ABC, abstractmethod
from dataclasses import dataclass

from src.perf_test_common import CalcEngine, SolutionLanguage


@dataclass(frozen=True)
class PersistedRunResultBase:
    strategy_name: str
    language: str
    engine: CalcEngine
    strategy_w_language_name: str
    interface: str
    # data_size: int
    elapsed_time: float
    # record_count: int


class PersistedRunResultLog(ABC):
    engine: CalcEngine
    language: SolutionLanguage

    def __init__(
            self,
            engine: CalcEngine,
            language: SolutionLanguage,
    ):
        self.engine = engine
        self.language = language

    @abstractmethod
    def derive_run_log_file_path_for_reading(self) -> str:
        pass

    @abstractmethod
    def read_line_from_log_file(
            self,
            line: str,
            last_header_line: list[str],
            fields: list[str],
    ) -> PersistedRunResultBase | None:
        pass

    @abstractmethod
    def result_looks_valid(
            self,
            result: PersistedRunResultBase,
    ) -> bool:
        pass

    def read_run_result_file(
            self,
    ) -> list[PersistedRunResultBase]:
        log_file_path = self.derive_run_log_file_path_for_reading()
        # language = engine_implied_language(engine)
        if log_file_path is None or not os.path.exists(log_file_path):
            return []
        test_runs: list[PersistedRunResultBase] = []
        last_header_line: list[str] | None = None
        with open(log_file_path, 'r') as f:
            for i_line, line in enumerate(f):
                line = line.rstrip()
                if line.startswith('#'):
                    print("Excluding line: " + line)
                    continue
                if line.startswith(' '):
                    last_header_line = line.strip().split(',')
                    continue
                if line.find(',') < 0:
                    print("Excluding line: " + line)
                    continue
                fields = line.rstrip().split(',')
                if fields[-1] == '':
                    fields = fields[:-1]
                assert last_header_line is not None
                result = self.read_line_from_log_file(line, last_header_line, fields)
                if result is None:
                    print(f"Excluding line {i_line+1}: {line}")
                    continue
                if not self.result_looks_valid(result):
                    print(f"Excluding line {i_line+1}: {line}")
                    continue
                test_runs.append(result)
        return test_runs

    # def _read_scala_file(
    #         self,
    #         engine: CalcEngine,
    #         qualifier: Callable[[PersistedRunResultBase], bool],
    # ) -> list[PersistedRunResultBase]:
    #     log_file_path = derive_run_log_file_path_for_reading(engine)
    #     language = engine_implied_language(engine)
    #     if log_file_path is None or not os.path.exists(log_file_path):
    #         return []
    #     test_runs: list[PersistedRunResultBase] = []
    #     with open(log_file_path, 'r') as f:
    #         for i_line, line in enumerate(f):
    #             line = line.rstrip()
    #             if line.startswith('#'):
    #                 print("Excluding line: " + line)
    #                 continue
    #             if line.startswith(' '):
    #                 print("Excluding line: " + line)
    #                 continue
    #             if line.find(',') < 0:
    #                 print("Excluding line: " + line)
    #                 continue
    #             fields = line.rstrip().split(',')
    #             outcome, strategy_name, interface, expected_size, returnedSize, elapsed_time = tuple(
    #                 fields)
    #             if outcome != 'success':
    #                 print("Excluding line: " + line)
    #                 continue
    #             if returnedSize != '9':
    #                 print("Excluding line: " + line)
    #                 continue
    #             # language = 'scala'
    #             result = PersistedRunResultBase(
    #                 strategy_name=strategy_name,
    #                 engine=engine,
    #                 language=language,
    #                 strategy_w_language_name=f"{strategy_name}_{language}",
    #                 interface=interface,
    #                 data_size=int(expected_size),
    #                 elapsed_time=float(elapsed_time),
    #                 record_count=-1)
    #             if not qualifier(result):
    #                 print(f"Excluding line {i_line+1}: {line}")
    #                 continue
    #             test_runs.append(result)
    #     return test_runs

    # def read_run_result_file(
    #         self,
    #         engine: CalcEngine,
    #         qualifier: Callable[[PersistedRunResultBase], bool],
    # ) -> list[PersistedRunResultBase]:
    #     if engine == CalcEngine.SCALA_SPARK:
    #         return _read_scala_file(engine, qualifier)
    #     else:
    #         return _read_python_file(engine, qualifier)
