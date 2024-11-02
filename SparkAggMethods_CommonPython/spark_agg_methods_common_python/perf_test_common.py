import math
import os
import random
from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass
from dataclasses import fields as dataclass_fields
from enum import Enum, StrEnum
from typing import Callable, Generic, Sequence, TextIO, TypeVar

import numpy
import pandas as pd
import scipy

from spark_agg_methods_common_python.utils.utils import set_random_seed

ELAPSED_TIME_COLUMN_NAME: str = 'elapsed_time'
LOCAL_TEST_DATA_FILE_LOCATION = 'd:/temp/SparkPerfTesting'
LOCAL_NUM_EXECUTORS = 7


class SolutionLanguage(StrEnum):
    PYTHON = 'python'
    SCALA = 'scala'


class CalcEngine(StrEnum):
    DASK = 'dask'
    PYSPARK = 'pyspark'
    PYTHON_ONLY = 'python_only'
    SCALA_SPARK = 'sc_spark'


class Challenge(StrEnum):
    VANILLA = 'vanilla'
    BI_LEVEL = 'bilevel'
    CONDITIONAL = 'conditional'
    SECTIONAL = 'sectional'
    DEDUPLICATION = 'deduplication'


SIX_TEST_CHALLENGES = [Challenge.BI_LEVEL, Challenge.CONDITIONAL, Challenge.VANILLA]


class SolutionInterfaceInvalid(StrEnum):
    INVALID = 'invalid'


class SolutionInterfaceDask(StrEnum):
    DASK_BAG = 'bag'
    DASK_DATAFRAME = 'ddf'
    DASK_SQL = 'sql'


class SolutionInterfacePythonOnly(StrEnum):
    PYTHON = 'python'
    PANDAS = 'pandas'


class SolutionInterfacePySpark(StrEnum):
    PYSPARK_RDD = 'rdd'
    PYSPARK_DATAFRAME_SQL = 'sql'
    PYSPARK_DATAFRAME_FLUENT = 'fluent'
    PYSPARK_DATAFRAME_PANDAS = 'pandas'


class SolutionInterfaceScalaSpark(StrEnum):
    SPARK_RDD = 'rdd'
    SPARK_SQL = 'sql'
    SPARK_DATAFRAME_PANDAS = 'pandas'


class NumericalToleranceExpectations(Enum):
    NOT_APPLICABLE = -1.0
    NUMPY = 1e-12
    NUMBA = 1e-10
    SIMPLE_SUM = 1e-11


SolutionInterface = (
    SolutionInterfaceInvalid
    | SolutionInterfaceDask
    | SolutionInterfacePythonOnly
    | SolutionInterfacePySpark
    | SolutionInterfaceScalaSpark
)

SolutionInterfacePython = (
    SolutionInterfaceDask
    | SolutionInterfacePythonOnly
    | SolutionInterfacePySpark
)
TSolutionInterface = TypeVar(
    'TSolutionInterface',
    SolutionInterfaceInvalid,
    SolutionInterfaceDask,
    SolutionInterfacePythonOnly,
    SolutionInterfacePySpark,
    SolutionInterfaceScalaSpark,
    SolutionInterfacePython,
    SolutionInterface,
)
TChallengeMethodDelegate = TypeVar('TChallengeMethodDelegate', bound=Callable)


def parse_interface_any(
        interface: str,
        engine: CalcEngine,
) -> SolutionInterface:
    match engine:
        case CalcEngine.DASK:
            return SolutionInterfaceDask(interface)
        case CalcEngine.PYSPARK:
            return SolutionInterfacePySpark(interface)
        case CalcEngine.PYTHON_ONLY:
            return SolutionInterfacePythonOnly(interface)
        case CalcEngine.SCALA_SPARK:
            return SolutionInterfaceScalaSpark(interface)
        case _:
            raise ValueError(f"Unknown engine: {engine}")


def parse_interface_python(
        interface: str,
        engine: CalcEngine,
) -> SolutionInterfacePython:
    match engine:
        case CalcEngine.DASK:
            return SolutionInterfaceDask(interface)
        case CalcEngine.PYSPARK:
            return SolutionInterfacePySpark(interface)
        case CalcEngine.PYTHON_ONLY:
            return SolutionInterfacePythonOnly(interface)
        case _:
            raise ValueError(f"Unknown engine: {engine}")


@dataclass(frozen=True)
class ChallengeMethodRegistrationBase(Generic[TSolutionInterface, TChallengeMethodDelegate]):
    strategy_name_2018: str | None
    strategy_name: str
    language: SolutionLanguage
    engine: CalcEngine
    interface: TSolutionInterface
    requires_gpu: bool
    delegate: TChallengeMethodDelegate


class DataSetDescriptionBase(ABC):
    debugging_only: bool
    num_source_rows: int
    size_code: str

    def __init__(
            self,
            *,
            debugging_only: bool,
            num_source_rows: int,
            size_code: str,
    ):
        self.debugging_only = debugging_only
        self.num_source_rows = num_source_rows
        self.size_code = size_code

    @classmethod
    def regressor_field_name(cls) -> str:
        raise NotImplementedError()

    @property
    def regressor_value(self) -> int:
        value = getattr(self, self.regressor_field_name())
        assert isinstance(value, int)
        return value


@dataclass(frozen=True)
class ExecutionParametersBase:
    default_parallelism: int
    num_executors: int


@dataclass(frozen=True)
class RunResultBase:
    num_source_rows: int
    elapsed_time: float
    num_output_rows: int
    finished_at: str | None


@dataclass(frozen=True)
class PersistedRunResultBase(Generic[TSolutionInterface], RunResultBase):
    language: SolutionLanguage
    engine: CalcEngine
    interface: TSolutionInterface
    strategy_name: str


TPersistedRunResult = TypeVar('TPersistedRunResult', bound=PersistedRunResultBase)


@dataclass(frozen=True)
class SummarizedPerformanceOfMethodAtDataSize:
    challenge: Challenge
    strategy_name: str
    language: SolutionLanguage
    engine: CalcEngine
    interface: str
    regressor: int
    number_of_runs: int
    elapsed_time_avg: float
    elapsed_time_std: float | None
    elapsed_time_rl: float | None
    elapsed_time_rh: float | None


class PersistedRunResultLog(Generic[TPersistedRunResult], ABC):
    engine: CalcEngine
    language: SolutionLanguage
    log_file_path: str

    def __init__(
            self,
            engine: CalcEngine,
            language: SolutionLanguage,
            log_file_path: str,
    ):
        self.engine = engine
        self.language = language
        self.log_file_path = log_file_path

    @abstractmethod
    def read_line_from_log_file(
            self,
            i_line: int,
            line: str,
            last_header_line: list[str],
            fields: list[str],
    ) -> TPersistedRunResult | None:
        pass

    @abstractmethod
    def result_looks_valid(
            self,
            result: TPersistedRunResult,
    ) -> bool:
        pass

    def read_run_result_file(
            self,
    ) -> list[TPersistedRunResult]:
        if self.log_file_path is None or not os.path.exists(self.log_file_path):
            return []
        test_runs: list[TPersistedRunResult] = []
        last_header_line: list[str] | None = None
        with open(self.log_file_path, 'r') as f:
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
                result = self.read_line_from_log_file(i_line+1, line, last_header_line, fields)
                if result is None:
                    print(f"Excluding line {i_line+1}: {line}")
                    continue
                if not self.result_looks_valid(result):
                    print(f"Excluding line {i_line+1}: {line}")
                    continue
                test_runs.append(result)
        return test_runs

    def structure_test_results(
            self,
            *,
            challenge_method_list: Sequence[ChallengeMethodRegistrationBase],
            expected_sizes: list[int],
            test_runs: list[TPersistedRunResult],
            regressor_from_run_result: Callable[[TPersistedRunResult], int],
    ) -> dict[str, dict[int, list[TPersistedRunResult]]]:
        strategy_names = (
            {x.strategy_name for x in challenge_method_list}
            .union([x.strategy_name for x in test_runs]))
        test_x_values = set(expected_sizes).union([regressor_from_run_result(x) for x in test_runs])
        test_results_by_strategy_name_by_data_size = {method: {x: []
                                                               for x in test_x_values} for method in strategy_names}
        for result in test_runs:
            x_value = regressor_from_run_result(result)
            test_results_by_strategy_name_by_data_size[result.strategy_name][x_value].append(result)
        return test_results_by_strategy_name_by_data_size

    def do_regression(
            self,
            *,
            challenge: Challenge,
            challenge_method_list: Sequence[ChallengeMethodRegistrationBase],
            test_results_by_strategy_name_by_data_size: dict[str, dict[int, list[TPersistedRunResult]]],
    ) -> list[SummarizedPerformanceOfMethodAtDataSize]:
        confidence = 0.95
        challenge_method_list = sorted(
            challenge_method_list,
            key=lambda x: (x.language, x.interface, x.strategy_name))
        challenge_method_by_strategy_name = ({
            x.strategy_name: x for x in challenge_method_list
        } | {
            x.strategy_name_2018: x for x in challenge_method_list
            if x.strategy_name_2018 is not None
        })

        rows: list[SummarizedPerformanceOfMethodAtDataSize] = []
        for strategy_name in test_results_by_strategy_name_by_data_size:
            print("Looking to analyze %s" % strategy_name)
            method = challenge_method_by_strategy_name[strategy_name]
            for regressor_value in test_results_by_strategy_name_by_data_size[strategy_name]:
                runs = test_results_by_strategy_name_by_data_size[strategy_name][regressor_value]
                ar: numpy.ndarray \
                    = numpy.asarray([x.elapsed_time for x in runs], dtype=float)
                numRuns = len(runs)
                mean = numpy.mean(ar).item()
                stdev = numpy.std(ar, ddof=1).item()
                rl, rh = (
                    scipy.stats.norm.interval(
                        confidence, loc=mean, scale=stdev / math.sqrt(numRuns))
                    if numRuns > 1 else
                    (math.nan, math.nan)
                )
                assert self.engine == method.engine
                rows.append(SummarizedPerformanceOfMethodAtDataSize(
                    challenge=challenge,
                    strategy_name=strategy_name,
                    language=method.language,
                    engine=method.engine,
                    interface=method.interface,
                    regressor=regressor_value,
                    number_of_runs=numRuns,
                    elapsed_time_avg=mean,
                    elapsed_time_std=stdev,
                    elapsed_time_rl=rl,
                    elapsed_time_rh=rh,
                ))
        return rows

    @staticmethod
    def print_summary(
            rows: list[SummarizedPerformanceOfMethodAtDataSize],
            file_report_file_path: str,
    ):
        df = pd.DataFrame([x.__dict__ for x in rows])
        df = df.sort_values(by=['challenge', 'strategy_name', 'language', 'regressor'])
        print(df)
        df.to_csv(file_report_file_path, index=False)


class RunResultFileWriterBase(Generic[TSolutionInterface], ABC):
    file: TextIO
    language: SolutionLanguage
    engine: CalcEngine
    persisted_row_type: type

    def __init__(
            self,
            *,
            log_file_path: str,
            language: SolutionLanguage,
            engine: CalcEngine,
            persisted_row_type: type
    ):
        self.file = open(log_file_path, mode='at+')
        self.language = language
        self.engine = engine
        self.persisted_row_type = persisted_row_type
        self.write_header()

    def write_header(
            self,
    ) -> None:
        header = ' '+','.join(sorted(
            [x.name for x in dataclass_fields(self.persisted_row_type)]
        ))+','
        print(header, file=self.file)

    @abstractmethod
    def write_run_result(
            self,
            challenge_method_registration: ChallengeMethodRegistrationBase,
            run_result: RunResultBase,
    ) -> None:
        ...

    def _persist_run_result(
            self,
            persisted_result_row: PersistedRunResultBase[TSolutionInterface],
    ) -> None:
        dict_result = asdict(persisted_result_row)
        line = ','.join(
            str(getattr(persisted_result_row, field_name))
            for field_name in sorted(dict_result)
        )+','
        print(line, file=self.file)
        self.file.flush()

    def close(self):
        self.file.close()
        del self.file


@dataclass(frozen=True)
class RunnerArgumentsBase:
    num_runs: int
    random_seed: int | None
    shuffle: bool
    sizes: list[str]
    strategy_names: list[str]


def assemble_itinerary(
        args: RunnerArgumentsBase,
) -> list[tuple[str, str]]:
    itinerary: list[tuple[str, str]] = [
        (strategy_name, size_code)
        for strategy_name in args.strategy_names
        for size_code in args.sizes
        for _ in range(0, args.num_runs)
    ]
    if args.random_seed is not None:
        set_random_seed(args.random_seed)
    if args.shuffle:
        random.shuffle(itinerary)
    return itinerary


def print_test_runs_summary(
        test_runs_summary: dict[str, dict[int, int]]
):
    df_test_runs_summary = pd.DataFrame.from_records(
        [
            {"strategy_name": strategy_name}
            | {
                "size_%d" % size: test_runs_summary[strategy_name][size]
                for size in sorted(test_runs_summary[strategy_name])
            }
            for strategy_name in sorted(test_runs_summary.keys())
        ], index="strategy_name")
    print(df_test_runs_summary)
