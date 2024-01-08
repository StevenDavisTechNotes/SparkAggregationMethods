from dataclasses import dataclass
from functools import reduce
from typing import Any, Callable, NamedTuple, cast

from pyspark import RDD, StorageLevel
from pyspark.sql import DataFrame as spark_DataFrame
from pyspark.sql import Row

from six_field_test_data.six_test_data_types import (DataPoint, DataSetAnswer,
                                                     DataSetDescription,
                                                     ExecutionParameters,
                                                     populate_data_set_generic)
from utils.tidy_spark_session import TidySparkSession

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


class GrpTotal(NamedTuple):
    grp: int
    subgrp: int
    mean_of_C: float
    max_of_D: float | None
    cond_var_of_E: float


@dataclass(frozen=True)
class PysparkPythonPendingAnswerSet:
    feasible: bool = True
    rdd_tuple: RDD[GrpTotal] | None = None
    rdd_row:  RDD[Row] | None = None
    spark_df: spark_DataFrame | None = None

    def to_rdd(self) -> RDD[GrpTotal] | RDD[Row] | None:
        assert self.feasible is False
        return (
            self.rdd_tuple if self.rdd_tuple is not None else
            self.rdd_row if self.rdd_row is not None else
            self.spark_df.rdd if self.spark_df is not None else
            None
        )


@dataclass(frozen=True)
class PysparkPythonTestMethod:
    original_strategy_name: str
    strategy_name: str
    language: str
    interface: str
    only_when_gpu_testing: bool
    delegate: Callable[
        [TidySparkSession, ExecutionParameters, PysparkDataSet],
        PysparkPythonPendingAnswerSet]


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
    rdd_src: RDD[DataPoint]
    if num_data_points < src_num_partitions:
        rdd_src = spark_session.spark.sparkContext.parallelize((
            DataPoint(*r)  # type: ignore
            for r in cast(list[dict[str, Any]],
                          df.to_records(index=False))  # type: ignore
        ), src_num_partitions)
    else:
        def union_rdd_set(lhs: RDD[DataPoint], rhs: RDD[DataPoint]) -> RDD[DataPoint]:
            return lhs.union(rhs)
        rdd_src = reduce(union_rdd_set, [
            spark_session.spark.sparkContext.parallelize((
                DataPoint(*r)
                for r in df[df["id"] % src_num_partitions == ipart]
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
        def union_all(lhs: spark_DataFrame, rhs: spark_DataFrame) -> spark_DataFrame:
            return lhs.unionAll(rhs)
        df_src = reduce(union_all, [
            spark_session.spark.createDataFrame(
                df[df["id"] % src_num_partitions == ipart]
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
