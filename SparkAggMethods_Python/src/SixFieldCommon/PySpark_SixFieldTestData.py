from dataclasses import dataclass
from functools import reduce
from typing import Callable, Tuple

from pyspark import RDD, StorageLevel
from pyspark.sql import DataFrame as spark_DataFrame

from SixFieldCommon.SixFieldTestData import (DataPoint, DataSetAnswer,
                                             DataSetDescription,
                                             ExecutionParameters,
                                             populate_data_set_generic)
from Utils.TidySparkSession import TidySparkSession

# region PySpark version


@dataclass(frozen=True)
class PysparkDataSetData():
    SrcNumPartitions: int
    AggTgtNumPartitions: int
    dfSrc: spark_DataFrame
    rddSrc: RDD[DataPoint]


@dataclass(frozen=True)
class PysparkDataSet():
    description: DataSetDescription
    data: PysparkDataSetData


@dataclass(frozen=True)
class PySparkDataSetWithAnswer(PysparkDataSet):
    answer: DataSetAnswer


@dataclass(frozen=True)
class PysparkPythonTestMethod:
    strategy_name: str
    language: str
    interface: str
    delegate: Callable[
        [TidySparkSession, ExecutionParameters, PysparkDataSet],
        Tuple[RDD | None, spark_DataFrame | None]]

# endregion


def populate_data_set_pyspark(
        spark_session: TidySparkSession,
        exec_params: ExecutionParameters,
        size_code: str,
        num_grp_1: int,
        num_grp_2: int,
        repetition: int,
) -> PySparkDataSetWithAnswer:
    num_data_points, tgt_num_partitions, src_num_partitions, df, \
        vanilla_answer, bilevel_answer, conditional_answer = populate_data_set_generic(
            exec_params, num_grp_1, num_grp_2, repetition)
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
    print("Found rdd %i rows in %i parts ratio %.1f" % (cnt, parts, cnt / parts))
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
    print("Found df %i rows in %i parts ratio %.1f" % (cnt, parts, cnt / parts))
    assert cnt == num_data_points

    del df
    return PySparkDataSetWithAnswer(
        description=DataSetDescription(
            NumDataPoints=num_grp_1 * num_grp_2 * repetition,
            NumGroups=num_grp_1,
            NumSubGroups=num_grp_2,
            SizeCode=size_code,
            RelativeCardinalityBetweenGroupings=num_grp_2 // num_grp_1,
        ),
        data=PysparkDataSetData(
            SrcNumPartitions=src_num_partitions,
            AggTgtNumPartitions=tgt_num_partitions,
            dfSrc=df_src,
            rddSrc=rdd_src,
        ),
        answer=DataSetAnswer(
            vanilla_answer=vanilla_answer,
            bilevel_answer=bilevel_answer,
            conditional_answer=conditional_answer,
        ),
    )
