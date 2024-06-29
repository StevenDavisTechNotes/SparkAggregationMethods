from dataclasses import dataclass
from functools import reduce
from typing import Any, Literal, NamedTuple, Protocol, cast

from pyspark import RDD, StorageLevel
from pyspark.sql import DataFrame as PySparkDataFrame
from pyspark.sql import Row

from perf_test_common import CalcEngine
from six_field_test_data.six_test_data_types import (DataPoint, DataSetAnswer,
                                                     DataSetDescription,
                                                     ExecutionParameters,
                                                     populate_data_set_generic)
from utils.tidy_spark_session import TidySparkSession

# region PySpark version


@dataclass(frozen=True)
class DataSetDataPyspark():
    SrcNumPartitions: int
    AggTgtNumPartitions: int
    dfSrc: PySparkDataFrame
    rddSrc: RDD[DataPoint]


@dataclass(frozen=True)
class DataSetPyspark():
    description: DataSetDescription
    data: DataSetDataPyspark


@dataclass(frozen=True)
class DataSetPysparkWithAnswer(DataSetPyspark):
    answer: DataSetAnswer


class GrpTotal(NamedTuple):
    grp: int
    subgrp: int
    mean_of_C: float
    max_of_D: float | None
    cond_var_of_E: float


TChallengePendingAnswerPythonPyspark = Literal["infeasible"] | RDD[GrpTotal] | RDD[Row] | PySparkDataFrame


class IChallengeMethodPythonPyspark(Protocol):
    def __call__(
        self,
        *,
        spark_session: TidySparkSession,
        exec_params: ExecutionParameters,
        data_set: DataSetPyspark
    ) -> TChallengePendingAnswerPythonPyspark: ...


@dataclass(frozen=True)
class ChallengeMethodPythonPysparkRegistration:
    original_strategy_name: str
    strategy_name: str
    language: str
    interface: str
    requires_gpu: bool
    delegate: IChallengeMethodPythonPyspark


# endregion

def populate_data_set_pyspark(
        spark_session: TidySparkSession,
        exec_params: ExecutionParameters,
        size_code: str,
        num_grp_1: int,
        num_grp_2: int,
        repetition: int,
) -> DataSetPysparkWithAnswer:
    raw_data = populate_data_set_generic(
        CalcEngine.PYSPARK,  exec_params, num_grp_1, num_grp_2, repetition)
    df = raw_data.dfSrc
    rdd_src: RDD[DataPoint]
    if raw_data.num_data_points < raw_data.src_num_partitions:
        rdd_src = spark_session.spark.sparkContext.parallelize((
            DataPoint(*r)  # type: ignore
            for r in cast(list[dict[str, Any]],
                          df.to_records(index=False))
        ), raw_data.src_num_partitions)
    else:
        def union_rdd_set(lhs: RDD[DataPoint], rhs: RDD[DataPoint]) -> RDD[DataPoint]:
            return lhs.union(rhs)
        rdd_src = reduce(union_rdd_set, [
            spark_session.spark.sparkContext.parallelize((
                DataPoint(*r)
                for r in df[df["id"] % raw_data.src_num_partitions == i_part]
                .to_records(index=False)
            ), 1)
            for i_part in range(raw_data.src_num_partitions)
        ])
    rdd_src.persist(StorageLevel.DISK_ONLY)
    cnt, parts = rdd_src.count(), rdd_src.getNumPartitions()
    print("Found rdd %i rows in %i parts ratio %.1f" % (cnt, parts, cnt / parts))
    assert cnt == raw_data.num_data_points

    if len(df) < raw_data.src_num_partitions:
        df_src = (
            spark_session.spark
            .createDataFrame(df)
            .repartition(raw_data.src_num_partitions)
        )
    else:
        def union_all(lhs: PySparkDataFrame, rhs: PySparkDataFrame) -> PySparkDataFrame:
            return lhs.unionAll(rhs)
        df_src = reduce(union_all, [
            spark_session.spark.createDataFrame(
                df[df["id"] % raw_data.src_num_partitions == i_part]
            ).coalesce(1)
            for i_part in range(raw_data.src_num_partitions)
        ])
    df_src.persist(StorageLevel.DISK_ONLY)
    cnt, parts = df_src.count(), df_src.rdd.getNumPartitions()
    print("Found df %i rows in %i parts ratio %.1f" % (cnt, parts, cnt / parts))
    assert cnt == raw_data.num_data_points

    del df
    return DataSetPysparkWithAnswer(
        description=DataSetDescription(
            NumDataPoints=num_grp_1 * num_grp_2 * repetition,
            NumGroups=num_grp_1,
            NumSubGroups=num_grp_2,
            SizeCode=size_code,
            RelativeCardinalityBetweenGroupings=num_grp_2 // num_grp_1,
        ),
        data=DataSetDataPyspark(
            SrcNumPartitions=raw_data.src_num_partitions,
            AggTgtNumPartitions=raw_data.tgt_num_partitions,
            dfSrc=df_src,
            rddSrc=rdd_src,
        ),
        answer=DataSetAnswer(
            vanilla_answer=raw_data.vanilla_answer,
            bilevel_answer=raw_data.bilevel_answer,
            conditional_answer=raw_data.conditional_answer,
        ),
    )
