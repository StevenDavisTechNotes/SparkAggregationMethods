from typing import Callable, Tuple, Any, Dict

import os

import pandas as pd

import findspark
from pyspark import SparkContext, RDD
from pyspark.rdd import PythonEvalType
from pyspark.sql import SparkSession, GroupedData
from pyspark.sql.dataframe import DataFrame as spark_DataFrame
from pyspark.sql.functions import pandas_udf
import pyspark.sql.types as DataTypes
from pyspark.sql.types import StructType


def createSparkContext(config_dict: Dict[str, Any]) -> SparkSession:
    findspark.init()
    full_path_to_python = os.path.join(
        os.getcwd(), "venv", "scripts", "python.exe")
    os.environ["PYSPARK_PYTHON"] = full_path_to_python
    os.environ["PYSPARK_DRIVER_PYTHON"] = full_path_to_python
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


def setupSparkContext(in_spark) -> Tuple[SparkContext, Any]:
    spark = in_spark
    sc = spark.sparkContext
    log4jLogger = sc._jvm.org.apache.log4j
    log = log4jLogger.LogManager.getLogger(__name__)
    log.info("script initialized")
    return sc, log


def pandas_udf_df_to_df(
    returnType: DataTypes.StructType | str,
) -> Callable[[Callable[[pd.DataFrame], pd.DataFrame]], Callable]:
    def _(work_func):
        transform = pandas_udf(
            f=returnType,
            returnType=PythonEvalType.SQL_COGROUPED_MAP_PANDAS_UDF)
        return transform(work_func)
    return _


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
