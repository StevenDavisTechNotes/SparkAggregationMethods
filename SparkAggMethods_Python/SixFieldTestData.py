import collections
import random
from dataclasses import dataclass
from functools import reduce
from typing import Callable, Tuple

import pyspark.sql.types as DataTypes
from pyspark import RDD, StorageLevel
from pyspark.sql import DataFrame as spark_DataFrame

from Utils.SparkUtils import NUM_EXECUTORS, TidySparkSession
from Utils.Utils import round_up


@dataclass(frozen=True)
class ExecutionParameters:
    NumExecutors: int


DataPoint = collections.namedtuple(
    "DataPoint",
    ["id", "grp", "subgrp", "A", "B", "C", "D", "E", "F"])
DataPointSchema = DataTypes.StructType([
    DataTypes.StructField('id', DataTypes.LongType(), False),
    DataTypes.StructField('grp', DataTypes.LongType(), False),
    DataTypes.StructField('subgrp', DataTypes.LongType(), False),
    DataTypes.StructField('A', DataTypes.LongType(), False),
    DataTypes.StructField('B', DataTypes.LongType(), False),
    DataTypes.StructField('C', DataTypes.DoubleType(), False),
    DataTypes.StructField('D', DataTypes.DoubleType(), False),
    DataTypes.StructField('E', DataTypes.DoubleType(), False),
    DataTypes.StructField('F', DataTypes.DoubleType(), False)])


@dataclass(frozen=True)
class DataSet:
    NumDataPoints: int
    NumGroups: int
    NumSubGroups: int
    SizeCode: str
    SrcNumPartitions: int
    AggTgtNumPartitions: int
    RelativeCardinalityBetweenGroupings: int
    dfSrc: spark_DataFrame
    rddSrc: RDD[DataPoint]


@dataclass(frozen=True)
class PythonTestMethod:
    strategy_name: str
    language: str
    interface: str
    delegate: Callable[
        [TidySparkSession, ExecutionParameters, DataSet],
        Tuple[RDD | None, spark_DataFrame | None]]


@dataclass(frozen=True)
class RunResult:
    dataSize: int
    elapsedTime: float
    recordCount: int


def generateData(
    size_code: str,
    sprk_session: TidySparkSession,
    numGrp1: int = 3,
    numGrp2: int = 3,
    repetition: int = 1000,
) -> DataSet:
    data_points = [
        DataPoint(
            id=i,
            grp=(i // numGrp2) % numGrp1,
            subgrp=i % numGrp2,
            A=random.randint(1, repetition),
            B=random.randint(1, repetition),
            C=random.uniform(1, 10),
            D=random.uniform(1, 10),
            E=random.normalvariate(0, 10),
            F=random.normalvariate(1, 10))
        for i in range(0, numGrp1 * numGrp2 * repetition)]
    # Need to split this up, upfront, into many partitions
    # to avoid memory issues and
    # avoid preferential treatment of methods that don't repartition
    tgt_num_partitions = 9  # numGrp1 * numGrp2
    max_data_points_per_partition = 10000
    src_num_partitions = max(
        NUM_EXECUTORS * 2,
        tgt_num_partitions,
        round_up(
            numGrp1 * numGrp2 * repetition,
            max_data_points_per_partition))
    print(f"Using {numGrp1}, {numGrp2}, {repetition} tgt_num_partitions={src_num_partitions} each {numGrp1 * numGrp2 * repetition/src_num_partitions}")
    data_point_slices = [
        [x for x in data_points if (x.id % src_num_partitions) == igrp]
        for igrp in range(0, src_num_partitions)
    ]
    rdd_src = reduce(lambda lhs, rhs: lhs.union(rhs), [
        sprk_session.spark.sparkContext.parallelize(data_point_slice, 1)
        for data_point_slice in data_point_slices
        if len(data_point_slice) > 0
    ])
    rdd_src.persist(StorageLevel.DISK_ONLY)
    print("Found rdd %i rows in %i parts ratio %f" % (
        rdd_src.count(), rdd_src.getNumPartitions(), rdd_src.count() / rdd_src.getNumPartitions()))
    df_src = sprk_session.spark.createDataFrame(
        rdd_src, schema=DataPointSchema)
    df_src.persist(StorageLevel.DISK_ONLY)
    print("Found df %i rows in %i parts ratio %f" % (
        df_src.count(), df_src.rdd.getNumPartitions(), df_src.count() / df_src.rdd.getNumPartitions()))
    return DataSet(
        NumDataPoints=len(data_points),
        NumGroups=numGrp1,
        NumSubGroups=numGrp2,
        SizeCode=size_code,
        SrcNumPartitions=src_num_partitions,
        AggTgtNumPartitions=tgt_num_partitions,
        RelativeCardinalityBetweenGroupings=numGrp2 // numGrp1,
        dfSrc=df_src,
        rddSrc=rdd_src,
    )
