import os
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import findspark
from pyspark import SparkContext
from pyspark.sql import SparkSession

SPARK_SCRATCH_FOLDER = "D:\\temp\\spark_scratch"
LOCAL_NUM_EXECUTORS = 7


def get_python_code_root_path() -> str:
    return str(Path(os.path.abspath(__file__)).parent.parent.parent)


@dataclass(frozen=True)
class OpenSparkSession:
    # python_code_root_path: str
    python_interpreter_path: str
    python_src_code_path: str
    spark_session: SparkSession
    spark_context: SparkContext
    log: Any


def open_spark_session(
        config_dict: dict[str, Any],
        enable_hive_support: bool,
        spark_scratch_folder: str,
        local_num_executors: int,
) -> OpenSparkSession:
    findspark.init()
    python_code_root_path = get_python_code_root_path()
    python_src_code_path = python_code_root_path  # os.path.join(python_code_root_path, "src")
    path_to_python_interpreter = os.path.join(
        python_code_root_path, "venv", "scripts", "python.exe")
    os.environ["PYSPARK_PYTHON"] = path_to_python_interpreter
    os.environ["PYSPARK_DRIVER_PYTHON"] = path_to_python_interpreter
    os.environ["SPARK_LOCAL_DIRS"] = spark_scratch_folder
    spark = (
        # cSpell: disable
        SparkSession
        .builder
        .appName("PerfTestApp")  # pyright: ignore[reportAttributeAccessIssue]
        .master(f"local[{local_num_executors}]")
        .config("spark.pyspark.python", path_to_python_interpreter)
        .config("spark.ui.enabled", "false")
        .config('spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version', 2)
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.rdd.compress", "false")
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
    sc.setCheckpointDir(
        os.path.join(
            spark_scratch_folder,
            "SectionAggCheckpoint"))
    return OpenSparkSession(
        # python_code_root_path=python_code_root_path,
        python_interpreter_path=path_to_python_interpreter,
        python_src_code_path=python_src_code_path,
        spark_session=spark_session,
        spark_context=sc,
        log=log
    )


class TidySparkSession:
    python_code_root_path: str
    python_interpreter_path: str
    python_src_code_path: str
    spark: SparkSession
    spark_context: SparkContext
    log: Any

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
        self.log = open_session.log

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback) -> None:
        self.spark.stop()

    def create_scratch_folder(self) -> None:
        if os.path.exists(SPARK_SCRATCH_FOLDER) is False:
            os.mkdir(SPARK_SCRATCH_FOLDER)

    def clean_up_scratch_folder(self) -> None:
        for item in Path(SPARK_SCRATCH_FOLDER).iterdir():
            shutil.rmtree(item)
