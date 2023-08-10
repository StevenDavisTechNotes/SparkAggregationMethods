import os
from pathlib import Path
import shutil
from typing import Any, Dict, Tuple, cast

import findspark
from pyspark import SparkContext
from pyspark.sql import SparkSession


SPARK_SCRATCH_FOLDER = "C:\\temp\\spark_scratch"
LOCAL_NUM_EXECUTORS = 7


def openSparkSession(
        config_dict: Dict[str, Any],
        enable_hive_support: bool,
        spark_scratch_folder: str,
        local_num_executors: int,
) -> Tuple[SparkSession, SparkContext, Any]:
    findspark.init()
    full_path_to_python = os.path.join(
        os.getcwd(), "venv", "scripts", "python.exe")
    os.environ["PYSPARK_PYTHON"] = full_path_to_python
    os.environ["PYSPARK_DRIVER_PYTHON"] = full_path_to_python
    os.environ["SPARK_LOCAL_DIRS"] = spark_scratch_folder
    spark = (
        SparkSession
        .builder
        .appName("PerfTestApp")
        .master(f"local[{local_num_executors}]")
        .config("spark.pyspark.python", full_path_to_python)
        .config("spark.ui.enabled", "false")
        .config('spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version', 2)
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.rdd.compress", "false")
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
    return spark_session, sc, log


class TidySparkSession:
    spark: SparkSession
    spark_context: SparkContext
    log: Any

    def __init__(
            self,
            config_dict: Dict[str, Any],
            enable_hive_support: bool
    ):
        self.createScratchFolder()
        self.cleanUpScratchFolder()
        self.spark, self.spark_context, self.log \
            = openSparkSession(
                config_dict, enable_hive_support,
                SPARK_SCRATCH_FOLDER, LOCAL_NUM_EXECUTORS)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback) -> None:
        self.spark.stop()

    def createScratchFolder(self) -> None:
        if os.path.exists(SPARK_SCRATCH_FOLDER) is False:
            os.mkdir(SPARK_SCRATCH_FOLDER)

    def cleanUpScratchFolder(self) -> None:
        for item in Path(SPARK_SCRATCH_FOLDER).iterdir():
            shutil.rmtree(item)
