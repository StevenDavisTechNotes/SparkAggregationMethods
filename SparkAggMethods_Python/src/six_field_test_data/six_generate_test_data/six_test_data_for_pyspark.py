from dataclasses import dataclass
from functools import reduce
from typing import Literal, Protocol

from pyspark import RDD, StorageLevel
from pyspark.sql import DataFrame as PySparkDataFrame
from pyspark.sql import Row

from src.perf_test_common import CalcEngine, SolutionInterfacePySpark, SolutionInterfaceScalaSpark, SolutionLanguage
from src.six_field_test_data.six_test_data_types import (
    Challenge, DataPointNT, DataSetAnswers, NumericalToleranceExpectations, SixTestDataChallengeMethodRegistrationBase,
    SixTestDataSetDescription, SixTestExecutionParameters, populate_data_set_generic,
)
from src.utils.tidy_spark_session import TidySparkSession


@dataclass(frozen=True)
class SixFieldDataSetDataPyspark():
    src_num_partitions: int
    agg_tgt_num_partitions_1_level: int
    agg_tgt_num_partitions_2_level: int
    df_src: PySparkDataFrame
    rdd_src: RDD[DataPointNT]


def pick_agg_tgt_num_partitions_pyspark(data: SixFieldDataSetDataPyspark, challenge: Challenge) -> int:
    match challenge:
        case Challenge.BI_LEVEL | Challenge.CONDITIONAL:
            return data.agg_tgt_num_partitions_1_level
        case Challenge.VANILLA:
            return data.agg_tgt_num_partitions_2_level
        case _:
            raise KeyError(f"Unknown challenge {challenge}")


@dataclass(frozen=True)
class SixFieldDataSetPyspark():
    data_description: SixTestDataSetDescription
    data: SixFieldDataSetDataPyspark


@dataclass(frozen=True)
class SixFieldDataSetPysparkWithAnswer(SixFieldDataSetPyspark):
    answer: DataSetAnswers


TSixFieldChallengePendingAnswerPythonPyspark = (
    Literal["infeasible"]
    | RDD[Row]
    | PySparkDataFrame
)


class ISixFieldChallengeMethodPythonPyspark(Protocol):
    def __call__(
        self,
        *,
        spark_session: TidySparkSession,
        exec_params: SixTestExecutionParameters,
        data_set: SixFieldDataSetPyspark,
    ) -> TSixFieldChallengePendingAnswerPythonPyspark: ...


@dataclass(frozen=True)
class SixFieldChallengeMethodPythonPysparkRegistration(
    SixTestDataChallengeMethodRegistrationBase[
        SolutionInterfacePySpark, ISixFieldChallengeMethodPythonPyspark
    ]
):
    strategy_name_2018: str | None
    strategy_name: str
    language: SolutionLanguage
    engine: CalcEngine
    interface: SolutionInterfacePySpark
    numerical_tolerance: NumericalToleranceExpectations
    requires_gpu: bool
    delegate: ISixFieldChallengeMethodPythonPyspark


class ISixFieldChallengeMethodScalaSpark(Protocol):
    def __call__(
        self,
    ) -> TSixFieldChallengePendingAnswerPythonPyspark: ...


@dataclass(frozen=True)
class SixFieldChallengeMethodScalaSparkRegistration(
    SixTestDataChallengeMethodRegistrationBase[
        SolutionInterfaceScalaSpark, ISixFieldChallengeMethodScalaSpark
    ]
):
    strategy_name_2018: str | None
    strategy_name: str
    language: SolutionLanguage
    engine: CalcEngine
    interface: SolutionInterfaceScalaSpark
    numerical_tolerance: NumericalToleranceExpectations
    requires_gpu: bool
    delegate: ISixFieldChallengeMethodScalaSpark


def populate_data_set_pyspark(
        spark_session: TidySparkSession,
        exec_params: SixTestExecutionParameters,
        data_size: SixTestDataSetDescription,
) -> SixFieldDataSetPysparkWithAnswer:
    raw_data = populate_data_set_generic(
        CalcEngine.PYSPARK, exec_params, data_size=data_size)
    df = raw_data.df_src
    rdd_src: RDD[DataPointNT]
    if raw_data.num_source_rows < raw_data.src_num_partitions:
        rdd_src = spark_session.spark.sparkContext.parallelize((
            DataPointNT(*r)
            for r in df.to_records(index=False)
        ), raw_data.src_num_partitions)
    else:
        def union_rdd_set(lhs: RDD[DataPointNT], rhs: RDD[DataPointNT]) -> RDD[DataPointNT]:
            return lhs.union(rhs)
        rdd_src = reduce(union_rdd_set, [
            spark_session.spark.sparkContext.parallelize((
                DataPointNT(*r)
                for r in df[df["id"] % raw_data.src_num_partitions == i_part]
                .to_records(index=False)
            ), 1)
            for i_part in range(raw_data.src_num_partitions)
        ])
    rdd_src.persist(StorageLevel.DISK_ONLY)
    cnt, parts = rdd_src.count(), rdd_src.getNumPartitions()
    print("Found rdd %i rows in %i parts ratio %.1f" % (cnt, parts, cnt / parts))
    assert cnt == raw_data.num_source_rows

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
    assert cnt == raw_data.num_source_rows

    del df
    return SixFieldDataSetPysparkWithAnswer(
        data_description=data_size,
        data=SixFieldDataSetDataPyspark(
            src_num_partitions=raw_data.src_num_partitions,
            agg_tgt_num_partitions_1_level=raw_data.tgt_num_partitions_1_level,
            agg_tgt_num_partitions_2_level=raw_data.tgt_num_partitions_2_level,
            df_src=df_src,
            rdd_src=rdd_src,
        ),
        answer=DataSetAnswers(
            vanilla_answer=raw_data.vanilla_answer,
            bilevel_answer=raw_data.bilevel_answer,
            conditional_answer=raw_data.conditional_answer,
        ),
    )
