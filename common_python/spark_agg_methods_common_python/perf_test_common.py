import logging
import os
import platform
import random
import sys
import tempfile
from abc import ABC, abstractmethod
from collections.abc import Callable, Iterable
from dataclasses import asdict, dataclass
from dataclasses import fields as dataclass_fields
from enum import Enum
from pathlib import Path
from typing import Generic, Literal, TextIO, TypeVar

from spark_agg_methods_common_python.utils.utils import set_random_seed

logger = logging.getLogger(__name__)

ELAPSED_TIME_COLUMN_NAME: str = "elapsed_time"
LOCAL_TEST_DATA_FILE_LOCATION: Path = Path(
    os.environ.get("SPARK_PERF_TESTING_DATA_LOCATION")
    or Path(tempfile.gettempdir()) / "spark_test_data"
)
LOCAL_NUM_EXECUTORS = 5
# Hardware dependent: the number of six-field rows (data points) one task, worker or batch holds at once.
# Every engine and strategy that sizes its work by rows (Dask partitions and chunks, single-threaded batches) MUST
# derive from this one value so results differ by architecture, not by per-strategy tuning. Re-tune it when the
# benchmark machine's memory changes (earlier runs used 10**4 for Dask partitions and 10**6 for chunks).
# PySpark deliberately does NOT use it: Spark's default configuration struggles with partitions this large, so its
# own, smaller MAX_DATA_POINTS_PER_SPARK_PARTITION stays in effect as part of what the benchmark measures.
TARGET_ROWS_PER_PARTITION = 2 ** 20
TESTING_YEAR = 2026


class SolutionLanguage(str, Enum):
    PYTHON = "python"
    SCALA = "scala"


class CalcEngine(str, Enum):
    DASK = "dask"
    FREE_THREADED = "python_free_threaded"
    PYSPARK = "pyspark"
    SCALA_SPARK = "sc_spark"
    SINGLE_THREADED = "python_single_threaded"


ENGINE_FOLDER_MAP = {
    CalcEngine.DASK: "python_dask",
    CalcEngine.FREE_THREADED: "python_free_threaded",
    CalcEngine.PYSPARK: "py_spark",
    CalcEngine.SCALA_SPARK: "scala_spark",
    CalcEngine.SINGLE_THREADED: "python_single_threaded",
}


class Challenge(str, Enum):
    VANILLA = "vanilla"
    BILEVEL = "bilevel"
    CONDITIONAL = "conditional"
    SECTIONAL = "sectional"
    DEDUPE = "dedupe"


CHALLENGES_SIX_DATA = [
    Challenge.BILEVEL,
    Challenge.CONDITIONAL,
    Challenge.VANILLA,
]


class SolutionInterfaceInvalid(str, Enum):
    INVALID = "invalid"


class SolutionInterfaceDask(str, Enum):
    DASK_ACTORS = "actors"
    DASK_BAG = "bag"
    DASK_DATAFRAME = "ddf"
    DASK_DELAYED = "delayed"
    DASK_FUTURES = "futures"


class SolutionInterfacePythonST(str, Enum):
    NUMBA = "numba"
    NUMPY = "numpy"
    PANDAS = "pandas"
    POLARS = "polars"
    PYARROW = "pyarrow"
    PROGRESSIVE = "progressive"
    SINGLE_THREADED = "single_threaded"
    # QUEUES = "queues"


class SolutionInterfacePythonFreeThreaded(str, Enum):
    # PANDAS = "pandas"
    # PROGRESSIVE = "progressive"
    # SINGLE_THREADED = "single_threaded"
    QUEUES = "queues"


class SolutionInterfacePySpark(str, Enum):
    PYSPARK_RDD = "rdd"
    PYSPARK_DATAFRAME_SQL = "sql"
    PYSPARK_DATAFRAME_FLUENT = "fluent"
    PYSPARK_DATAFRAME_PANDAS = "pandas"


class NumericalToleranceExpectations(Enum):
    NOT_APPLICABLE = -1.0
    NUMPY = 5e-12
    NUMBA = 1e-10
    SIMPLE_SUM = 1e-11


SolutionInterface = (
    SolutionInterfaceInvalid
    | SolutionInterfaceDask
    | SolutionInterfacePythonST
    | SolutionInterfacePySpark
)

SolutionInterfacePython = (
    SolutionInterfaceDask
    | SolutionInterfacePythonST
    | SolutionInterfacePySpark
)
TSolutionInterface = TypeVar(
    "TSolutionInterface",
    SolutionInterfaceInvalid,
    SolutionInterfaceDask,
    SolutionInterfacePythonST,
    SolutionInterfacePythonFreeThreaded,
    SolutionInterfacePySpark,
    SolutionInterfacePython,
    SolutionInterface,
)
TChallengeMethodDelegate = TypeVar("TChallengeMethodDelegate", bound=Callable)


@dataclass(frozen=True)
class ChallengeMethodRegistrationBase(Generic[TSolutionInterface, TChallengeMethodDelegate]):
    strategy_name_2018: str | None
    strategy_name_2023: str | None
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


def _format_csv_field(value: object) -> str:
    # str() on a (str, Enum) member yields "Class.NAME"; persist the value instead.
    if isinstance(value, Enum):
        return str(value.value)
    return str(value)


def run_log_has_current_header(existing_lines: Iterable[str], header: str) -> bool:
    """True when the last header line already in a run log equals ``header``.

    Header lines start with a blank (see ``write_header``); result lines never do.
    """
    last_header = None
    for line in existing_lines:
        if line.startswith(" "):
            last_header = line.rstrip()
    return last_header == header


class RunResultFileWriterBase(Generic[TSolutionInterface], ABC):
    file: TextIO
    language: SolutionLanguage
    engine: CalcEngine
    persisted_row_type: type

    def __init__(
            self,
            *,
            log_file_name: str,
            language: SolutionLanguage,
            engine: CalcEngine,
            persisted_row_type: type
    ):
        run_log_file_path = construct_result_file_path(log_file_name)
        self.file = run_log_file_path.open(mode="at+")
        self.language = language
        self.engine = engine
        self.persisted_row_type = persisted_row_type
        self.write_header()

    def write_header(
            self,
    ) -> None:
        header = " "+",".join(sorted(
            [x.name for x in dataclass_fields(self.persisted_row_type)]  # ty: ignore[invalid-argument-type]
        ))+","
        if __debug__:
            logger.info(f"Skipping writing header {header}")
            return
        # A header starts a new segment for summarize_test_results, so a changed column set must get a new one;
        # an unchanged one is redundant when each step runs in its own process and reopens the log.
        self.file.seek(0)
        if run_log_has_current_header(
            existing_lines=self.file,
            header=header,
        ):
            logger.info(f"Header already current {header}")
            return
        logger.info(f"Writing header {header}")
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
        line = ",".join(
            _format_csv_field(getattr(persisted_result_row, field_name))
            for field_name in sorted(dict_result)
        )+","
        if __debug__:
            logger.info(f"Skipping writing line {line}")
        else:
            logger.info(f"Writing result {line}")
            print(line, file=self.file)
            self.file.flush()

    def close(self):
        self.file.flush()
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


def detect_environment() -> Literal["windows", "wsl"]:
    """The environment the benchmarks run in, which keeps results from different environments apart."""
    if sys.platform == "win32":
        return "windows"
    if sys.platform == "linux" and "microsoft" in platform.uname().release.lower():
        return "wsl"
    raise RuntimeError(f"Unsupported environment: sys.platform={sys.platform}, release={platform.uname().release}")


TESTING_ENVIRONMENT = detect_environment()


def construct_result_file_path(
        file_name: str,
) -> Path:
    return Path("results") / str(TESTING_YEAR) / TESTING_ENVIRONMENT / file_name
