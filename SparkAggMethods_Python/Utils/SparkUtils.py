from typing import Tuple, Any, Dict

import os
from pathlib import Path
import shutil


import findspark
from pyspark import RDD, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame as spark_DataFrame
import pyspark.sql.types as DataTypes

SPARK_SRATCH_FOLDER = "C:\\temp\\spark_scratch"
NUM_EXECUTORS = 8

class RddWithNoArgSortByKey:
    def __init__(self, src: RDD) -> None:
        self.src = src

    def sortByKey(self) -> RDD:
        return self.src.sortByKey()


def cast_no_arg_sort_by_key(src: RDD) -> RddWithNoArgSortByKey:
    return RddWithNoArgSortByKey(src)


SPARK_SRATCH_FOLDER = "C:\\temp\\spark_scratch"


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
        self.spark = self.openSparkSession(
            config_dict, enable_hive_support=enable_hive_support)
        sc, log = self.setupSparkContext()
        self.spark_context = sc
        self.log = log

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback) -> None:
        self.spark.stop()

    def openSparkSession(
            self, config_dict: Dict[str, Any], enable_hive_support: bool) -> SparkSession:
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
            .master(f"local[{NUM_EXECUTORS}]")
            .config("spark.pyspark.python", full_path_to_python)
            .config("spark.ui.enabled", "false")
            .config('spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version', 2)
        )
        for key, value in config_dict.items():
            spark = spark.config(key, value)
        if enable_hive_support:
            spark = spark.enableHiveSupport()
        return spark.getOrCreate()

    def createScratchFolder(self) -> None:
        if os.path.exists(SPARK_SRATCH_FOLDER) is False:
            os.mkdir(SPARK_SRATCH_FOLDER)

    def cleanUpScratchFolder(self) -> None:
        for item in Path(SPARK_SRATCH_FOLDER).iterdir():
            shutil.rmtree(item)

    def setupSparkContext(self) -> Tuple[SparkContext, Any]:
        spark: Any = self.spark
        sc = spark.sparkContext
        log4jLogger = sc._jvm.org.apache.log4j
        log = log4jLogger.LogManager.getLogger(__name__)
        log.info("script initialized")
        sc.setCheckpointDir(
            os.path.join(
                SPARK_SRATCH_FOLDER,
                "SectionAggCheckpoint"))
        return sc, log


# from
# https://stackoverflow.com/questions/30304810/dataframe-ified-zipwithindex/32741497#32741497


def dfZipWithIndex(
    df, spark: SparkSession, offset: int = 1,
    colName: str = "rowId"
) -> spark_DataFrame:
    '''
        Enumerates dataframe rows is native order, like rdd.ZipWithIndex(), but on a dataframe
        and preserves a schema

        :param df: source dataframe
        :param offset: adjustment to zipWithIndex()'s index
        :param colName: name of the index column
    '''
    #
    new_schema = DataTypes.StructType(
        [DataTypes.StructField(
            colName, DataTypes.LongType(), True)]
        + df.schema.fields)
    #
    zipped_rdd = df.rdd.zipWithIndex()
    #
    new_rdd = zipped_rdd.map(
        lambda kv: ([kv[1] + offset] + list(kv[0])))
    #
    return spark.createDataFrame(new_rdd, new_schema)


#
