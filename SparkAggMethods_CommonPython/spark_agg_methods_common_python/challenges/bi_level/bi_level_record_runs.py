import datetime as dt
import os
from dataclasses import dataclass

from spark_agg_methods_common_python.perf_test_common import (
    CalcEngine, ChallengeMethodRegistrationBase, PersistedRunResultBase,
    PersistedRunResultLog, RunResultBase, RunResultFileWriterBase,
    SolutionInterfacePython, SolutionLanguage, parse_interface_python)


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


class BiLevelPersistedRunResultLog(PersistedRunResultLog[BiLevelPersistedRunResult]):

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
        assert isinstance(result, BiLevelPersistedRunResult)
        return result.num_output_rows == 3

    def read_line_from_log_file(
            self,
            i_line: int,
            line: str,
            last_header_line: list[str],
            fields: list[str],
    ) -> BiLevelPersistedRunResult | None:
        if len(fields) < 6:
            fields.append('3')
        fields_as_dict = dict(zip(last_header_line, fields))
        strategy_name = fields_as_dict['strategy_name']
        interface = fields_as_dict['interface']
        num_source_rows = int(fields_as_dict['num_source_rows'])
        relative_cardinality = int(fields_as_dict['relative_cardinality'])
        elapsed_time = float(fields_as_dict['elapsed_time'])
        num_output_rows = int(fields_as_dict['num_output_rows']) if 'num_output_rows' in fields_as_dict else -1
        finished_at = fields_as_dict['finished_at']
        return BiLevelPersistedRunResult(
            strategy_name=strategy_name,
            language=self.language,
            engine=self.engine,
            interface=parse_interface_python(interface, self.engine),
            num_source_rows=num_source_rows,
            relative_cardinality_between_groupings=relative_cardinality,
            elapsed_time=elapsed_time,
            num_output_rows=num_output_rows,
            finished_at=finished_at,
        )


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
        self.file.flush()
