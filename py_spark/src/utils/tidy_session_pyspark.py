import os
import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import findspark
from pyspark import SparkContext
from pyspark.sql import SparkSession
from spark_agg_methods_common_python.perf_test_common import LOCAL_NUM_EXECUTORS

SPARK_SCRATCH_FOLDER: Path = Path(
    os.environ.get("SPARK_SCRATCH_SPACE")
    or Path(tempfile.gettempdir()) / "spark_scratch"
)


def get_python_code_root_path() -> Path:
    return Path(__file__).resolve().parent.parent.parent


@dataclass(frozen=True)
class OpenSparkSession:
    python_interpreter_path: Path
    python_src_code_path: Path
    spark_session: SparkSession
    spark_context: SparkContext
    log: Any


def open_spark_session(
        config_dict: dict[str, Any],
        enable_hive_support: bool,
        spark_scratch_folder: Path,
        local_num_executors: int,
) -> OpenSparkSession:
    findspark.init()
    python_code_root_path = get_python_code_root_path()
    python_src_code_path = python_code_root_path
    import sys
    path_to_python_interpreter = Path(sys.executable)
    os.environ["PYSPARK_PYTHON"] = str(path_to_python_interpreter)
    os.environ["PYSPARK_DRIVER_PYTHON"] = str(path_to_python_interpreter)
    os.environ["SPARK_LOCAL_DIRS"] = str(spark_scratch_folder)
    spark = (
        # cSpell: disable
        SparkSession
        .builder
        .appName("PerfTestApp")  # pyright: ignore[reportAttributeAccessIssue]
        .master(f"local[{local_num_executors}]")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", 2)
        .config("spark.pyspark.python", str(path_to_python_interpreter))
        .config("spark.rdd.compress", "false")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.ui.enabled", "false")
        # cSpell: enable
    )
    for key, value in config_dict.items():
        spark = spark.config(key, value)
    if enable_hive_support:
        spark = spark.enableHiveSupport()
    spark_session = spark.getOrCreate()
    sc = spark_session.sparkContext
    log4jLogger = cast(Any, sc)._jvm.org.apache.log4j
    log = log4jLogger.LogManager.getLogger(__name__)
    log.info("script initialized")
    sc.setCheckpointDir(str(spark_scratch_folder / "SectionAggCheckpoint"))
    return OpenSparkSession(
        python_interpreter_path=path_to_python_interpreter,
        python_src_code_path=python_src_code_path,
        spark_session=spark_session,
        spark_context=sc,
        log=log
    )


class TidySparkSession:
    python_code_root_path: Path
    python_interpreter_path: Path
    python_src_code_path: Path
    spark: SparkSession
    spark_context: SparkContext
    logger: Any

    def __init__(
            self,
            config_dict: dict[str, Any],
            enable_hive_support: bool,
    ):
        self.create_scratch_folder()
        self.clean_up_scratch_folder()
        open_session = open_spark_session(
            config_dict=config_dict,
            enable_hive_support=enable_hive_support,
            spark_scratch_folder=SPARK_SCRATCH_FOLDER,
            local_num_executors=LOCAL_NUM_EXECUTORS)
        self.python_code_root_path = get_python_code_root_path()
        self.python_interpreter_path = open_session.python_interpreter_path
        self.python_src_code_path = open_session.python_src_code_path
        self.spark = open_session.spark_session
        self.spark_context = open_session.spark_context
        self.logger = open_session.log

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback) -> None:
        self.spark.stop()

    def create_scratch_folder(self) -> None:
        SPARK_SCRATCH_FOLDER.mkdir(parents=True, exist_ok=True)

    def clean_up_scratch_folder(self) -> None:
        for item in SPARK_SCRATCH_FOLDER.iterdir():
            shutil.rmtree(item)
