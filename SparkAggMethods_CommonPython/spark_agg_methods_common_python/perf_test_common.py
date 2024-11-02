import random
from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass
from dataclasses import fields as dataclass_fields
from enum import Enum, StrEnum
from typing import Callable, Generic, TextIO, TypeVar

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
