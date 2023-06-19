from typing import Callable, Tuple, Any, Dict

import os
from pathlib import Path
import shutil

import pandas as pd

import findspark
from pyspark import RDD, SparkContext
from pyspark.sql import GroupedData, SparkSession
from pyspark.sql.dataframe import DataFrame as spark_DataFrame
from pyspark.sql.types import StructType

SPARK_SRATCH_FOLDER = "C:\\temp\\spark_scratch"


class GroupedDataWithPdDataFrame:
    def __init__(self, src: GroupedData) -> None:
        self.src = src

    def applyInPandas(
            self,
            func: Callable[[pd.DataFrame], pd.DataFrame],
            schema: StructType
    ) -> spark_DataFrame:
        return self.src.applyInPandas(func, schema) # type: ignore


def cast_from_pd_dataframe(src: GroupedData) -> GroupedDataWithPdDataFrame:
    return GroupedDataWithPdDataFrame(src)


class RddWithNoArgSortByKey:
    def __init__(self, src: RDD) -> None:
        self.src = src

    def sortByKey(self) -> RDD:
        return self.src.sortByKey()


def cast_no_arg_sort_by_key(src: RDD) -> RddWithNoArgSortByKey:
    return RddWithNoArgSortByKey(src)


SPARK_SRATCH_FOLDER = "C:\\temp\\spark_scratch"

def createSparkContext(config_dict: Dict[str, Any]) -> SparkSession:
    findspark.init()
    full_path_to_python = os.path.join(
        os.getcwd(), "venv", "scripts", "python.exe")
    os.environ["PYSPARK_PYTHON"] = full_path_to_python
    os.environ["PYSPARK_DRIVER_PYTHON"] = full_path_to_python
    os.environ["SPARK_LOCAL_DIRS"] = SPARK_SRATCH_FOLDER
    spark = (
        SparkSession
        .builder
        .appName("PerfTestApp")
        .master("local[8]")
        .config("spark.pyspark.python", full_path_to_python)
    )
    for key, value in config_dict.items():
        spark = spark.config(key, value)
    return spark.getOrCreate()

def createScratchFolder() -> None:
    cleanUpScratchFolder()
    if os.path.exists(SPARK_SRATCH_FOLDER) is False:
        os.mkdir(SPARK_SRATCH_FOLDER)

def cleanUpScratchFolder() -> None:
    for item in Path(SPARK_SRATCH_FOLDER).iterdir():
        shutil.rmtree(item)

def setupSparkContext(in_spark) -> Tuple[SparkContext, Any]:
    spark = in_spark
    sc = spark.sparkContext
    log4jLogger = sc._jvm.org.apache.log4j
    log = log4jLogger.LogManager.getLogger(__name__)
    log.info("script initialized")
    return sc, log

class TidySparkSession:
    spark: SparkSession
    spark_context: SparkContext
    log: Any

    def __init__(self, config_dict: Dict[str, Any]):
        createScratchFolder()
        self.spark = createSparkContext(config_dict)
        self.spark_context = self.spark.sparkContext
        log4jLogger = self.spark_context._jvm.org.apache.log4j # type: ignore
        self.log = log4jLogger.LogManager.getLogger(__name__)
        self.log.info("script initialized")
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_value, exc_traceback) -> None:
        self.spark.stop()