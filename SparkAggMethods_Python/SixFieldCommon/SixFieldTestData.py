import collections
import os
from pathlib import Path
import pickle
import random
from dataclasses import dataclass

from functools import reduce
from typing import Callable, Tuple
import pandas as pd
import numpy as np

import pyspark.sql.types as DataTypes
from pyspark import RDD, StorageLevel
from pyspark.sql import DataFrame as spark_DataFrame

from Utils.SparkUtils import NUM_EXECUTORS, TidySparkSession
from Utils.Utils import always_true, int_divide_round_up

LOCAL_TEST_DATA_FILE_LOCATION = "d:/temp/SparkPerfTesting"
MAX_DATA_POINTS_PER_PARTITION = 5 * 10**3


@dataclass(frozen=True)
class ExecutionParameters:
    NumExecutors: int


DataPoint = collections.namedtuple(
    "DataPoint",
    ["id", "grp", "subgrp", "A", "B", "C", "D", "E", "F"])
DataPointSchema = DataTypes.StructType([
    DataTypes.StructField('id', DataTypes.IntegerType(), False),
    DataTypes.StructField('grp', DataTypes.IntegerType(), False),
    DataTypes.StructField('subgrp', DataTypes.IntegerType(), False),
    DataTypes.StructField('A', DataTypes.IntegerType(), False),
    DataTypes.StructField('B', DataTypes.IntegerType(), False),
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
    vanilla_answer: pd.DataFrame
    bilevel_answer: pd.DataFrame
    conditional_answer: pd.DataFrame


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
    spark_session: TidySparkSession,
    numGrp1: int,
    numGrp2: int,
    repetition: int,
) -> DataSet:
    num_data_points = numGrp1 * numGrp2 * repetition
    # Need to split this up, upfront, into many partitions
    # to avoid memory issues and
    # avoid preferential treatment of methods that don't repartition
    tgt_num_partitions = numGrp1 * numGrp2
    src_num_partitions = max(
        NUM_EXECUTORS * 2,
        # tgt_num_partitions,
        int_divide_round_up(
            num_data_points,
            MAX_DATA_POINTS_PER_PARTITION))
    staging_file_name_csv = os.path.join(
        LOCAL_TEST_DATA_FILE_LOCATION,
        "SixField_Test_Data",
        f"SixFieldTestData_{numGrp1}_{numGrp2}_{repetition}.pkl")
    if os.path.exists(staging_file_name_csv) is False:
        generate_data_to_file(
            file_name=staging_file_name_csv,
            numGrp1=numGrp1,
            numGrp2=numGrp2,
            repetition=repetition,
        )
    with open(staging_file_name_csv, "rb") as fh:
        df = pickle.load(fh)
        assert len(df) == num_data_points
    vanilla_answer = pd.DataFrame.from_records([
        {
            "grp": grp,
            "subgrp": subgrp,
            "mean_of_C": df_cluster.C.mean(),
            "max_of_D": df_cluster.D.max(),
            "var_of_E": df_cluster.E.var(ddof=0),
            "var_of_E2": df_cluster.E.var(ddof=0),
        }
        for grp in range(numGrp1)
        for subgrp in range(numGrp2)
        if always_true(df_cluster := df[(df.grp == grp) & (df.subgrp == subgrp)])
    ])
    bilevel_answer = pd.DataFrame.from_records([
        {
            "grp": grp,
            "mean_of_C": df_cluster.C.mean(),
            "max_of_D": df_cluster.D.max(),
            "avg_var_of_E": subgroupedE.var(ddof=0).mean(),
            "avg_var_of_E2":
                subgroupedE
                .agg(lambda E:
                     ((E * E).sum() / E.count() -
                      (E.sum() / E.count())**2))
                .mean(),
        }
        for grp in range(numGrp1)
        if always_true(df_cluster := df[(df.grp == grp)])
        if always_true(subgroupedE := df_cluster.groupby('subgrp')['E'])
    ])
    conditional_answer = pd.DataFrame.from_records([
        {
            "grp": grp,
            "subgrp": subgrp,
            "mean_of_C": df_cluster.C.mean(),
            "max_of_D": df_cluster.D.max(),
            "cond_var_of_E": negE.var(ddof=0),
            "cond_var_of_E2":
                negE
                .agg(lambda E: (
                    (E * E).sum() / E.count() -
                    (E.sum() / E.count())**2
                )),
        }
        for grp in range(numGrp1)
        for subgrp in range(numGrp2)
        if always_true(df_cluster := df[(df.grp == grp) & (df.subgrp == subgrp)])
        if always_true(negE := df_cluster[df_cluster.E < 0]['E'])
    ])
    print(f"Using {numGrp1}, {numGrp2}, {repetition} "
          f"tgt_num_partitions={src_num_partitions} "
          f"each {numGrp1 * numGrp2 * repetition/src_num_partitions}")
    if num_data_points < src_num_partitions:
        rdd_src = spark_session.spark.sparkContext.parallelize((
            DataPoint(*r)
            for r in df.to_records(index=False)
        ), src_num_partitions)
    else:
        rdd_src = reduce(lambda lhs, rhs: lhs.union(rhs), [
            spark_session.spark.sparkContext.parallelize((
                DataPoint(*r)
                for r in df[df.id % src_num_partitions == ipart]
                .to_records(index=False)
            ), 1)
            for ipart in range(src_num_partitions)
        ])
    rdd_src.persist(StorageLevel.DISK_ONLY)
    cnt, parts = rdd_src.count(), rdd_src.getNumPartitions()
    print("Found rdd %i rows in %i parts ratio %f" % (cnt, parts, cnt / parts))
    assert cnt == num_data_points

    if len(df) < src_num_partitions:
        df_src = (
            spark_session.spark
            .createDataFrame(df)
            .repartition(src_num_partitions)
        )
    else:
        df_src = reduce(lambda lhs, rhs: lhs.unionAll(rhs), [
            spark_session.spark.createDataFrame(
                df[df.id % src_num_partitions == ipart]
            ).coalesce(1)
            for ipart in range(src_num_partitions)
        ])
    df_src.persist(StorageLevel.DISK_ONLY)
    cnt, parts = df_src.count(), df_src.rdd.getNumPartitions()
    print("Found df %i rows in %i parts ratio %f" % (cnt, parts, cnt / parts))
    assert cnt == num_data_points

    del df
    return DataSet(
        NumDataPoints=numGrp1 * numGrp2 * repetition,
        NumGroups=numGrp1,
        NumSubGroups=numGrp2,
        SizeCode=size_code,
        SrcNumPartitions=src_num_partitions,
        AggTgtNumPartitions=tgt_num_partitions,
        RelativeCardinalityBetweenGroupings=numGrp2 // numGrp1,
        dfSrc=df_src,
        rddSrc=rdd_src,
        vanilla_answer=vanilla_answer,
        bilevel_answer=bilevel_answer,
        conditional_answer=conditional_answer,
    )


def generate_data_to_file(
        file_name: str,
        numGrp1: int,
        numGrp2: int,
        repetition: int,
) -> None:
    num_data_points = numGrp1 * numGrp2 * repetition
    df = pd.DataFrame(np.array([[
        i,
        (i // numGrp2) % numGrp1,
        i % numGrp2,
    ] for i in range(num_data_points)]), columns=['id', 'grp', 'subgrp'], dtype=np.int32)
    df['A'] = np.random.randint(1, repetition, num_data_points, dtype=np.int32)
    df['B'] = np.random.randint(1, repetition, num_data_points, dtype=np.int32)
    df['C'] = np.random.uniform(1, 10, num_data_points)
    df['D'] = np.random.uniform(1, 10, num_data_points)
    df['E'] = np.random.normal(0, 10, num_data_points)
    df['F'] = np.random.normal(1, 10, num_data_points)
    Path(file_name).parent.mkdir(parents=True, exist_ok=True)
    tmp_file_name = f'{file_name}_t'
    with open(tmp_file_name, "wb") as fh:
        df.to_pickle(fh)
    os.rename(tmp_file_name, file_name)


def generate_data_to_file_using_python_random(
        file_name: str,
        numGrp1: int,
        numGrp2: int,
        repetition: int,
) -> None:
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
    Path(file_name).parent.mkdir(parents=True, exist_ok=True)
    tmp_file_name = f'{file_name}_t'
    with open(tmp_file_name, "wb") as fh:
        pickle.dump(data_points, fh)
    os.rename(tmp_file_name, file_name)
