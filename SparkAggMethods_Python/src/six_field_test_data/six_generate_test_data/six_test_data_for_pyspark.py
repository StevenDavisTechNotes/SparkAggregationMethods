from dataclasses import dataclass
from functools import reduce
from typing import Any, Callable, Literal, Protocol, cast

from pyspark import RDD, StorageLevel
from pyspark.sql import DataFrame as PySparkDataFrame
from pyspark.sql import Row

from src.perf_test_common import (
    CalcEngine, SolutionInterface, SolutionInterfacePySpark, SolutionInterfaceScalaSpark, SolutionLanguage,
)
from src.six_field_test_data.six_test_data_types import (
    Challenge, DataPointNT, DataSetAnswer, DataSetDescription, ExecutionParameters, NumericalToleranceExpectations,
    SixTestDataChallengeMethodRegistrationBase, populate_data_set_generic,
)
from src.utils.tidy_spark_session import TidySparkSession

# region PySpark version


@dataclass(frozen=True)
class DataSetDataPyspark():
    src_num_partitions: int
    agg_tgt_num_partitions_1_level: int
    agg_tgt_num_partitions_2_level: int
    df_src: PySparkDataFrame
    rdd_src: RDD[DataPointNT]


def pick_agg_tgt_num_partitions_pyspark(data: DataSetDataPyspark, challenge: Challenge) -> int:
    match challenge:
        case Challenge.BI_LEVEL | Challenge.CONDITIONAL:
            return data.agg_tgt_num_partitions_1_level
        case Challenge.VANILLA:
            return data.agg_tgt_num_partitions_2_level
        case _:
            raise KeyError(f"Unknown challenge {challenge}")


@dataclass(frozen=True)
class DataSetPyspark():
    description: DataSetDescription
    data: DataSetDataPyspark


@dataclass(frozen=True)
class DataSetPysparkWithAnswer(DataSetPyspark):
    answer: DataSetAnswer


TChallengePendingAnswerPythonPyspark = (
    Literal["infeasible"]
    | RDD[Row]
    | PySparkDataFrame
)


class IChallengeMethodPythonPyspark(Protocol):
    def __call__(
        self,
        *,
        spark_session: TidySparkSession,
        exec_params: ExecutionParameters,
        data_set: DataSetPyspark
    ) -> TChallengePendingAnswerPythonPyspark: ...


@dataclass(frozen=True)
class ChallengeMethodPythonPysparkRegistration(SixTestDataChallengeMethodRegistrationBase):
    strategy_name_2018: str | None
    strategy_name: str
    language: SolutionLanguage
    engine: CalcEngine
    interface: SolutionInterfacePySpark
    numerical_tolerance: NumericalToleranceExpectations
    requires_gpu: bool
    delegate: IChallengeMethodPythonPyspark

    @property
    def delegate_getter(self) -> Callable:
        return self.delegate

    @property
    def interface_getter(self) -> SolutionInterface:
        return self.interface


@dataclass(frozen=True)
class ChallengeMethodScalaSparkRegistration(SixTestDataChallengeMethodRegistrationBase):
    strategy_name_2018: str | None
    strategy_name: str
    language: SolutionLanguage
    engine: CalcEngine
    interface: SolutionInterfaceScalaSpark
    numerical_tolerance: NumericalToleranceExpectations
    requires_gpu: bool

    @property
    def delegate_getter(self) -> Callable | None:
        return None

    @property
    def interface_getter(self) -> SolutionInterface:
        return self.interface


# endregion

def populate_data_set_pyspark(
        spark_session: TidySparkSession,
        exec_params: ExecutionParameters,
        data_size: DataSetDescription,
) -> DataSetPysparkWithAnswer:
    raw_data = populate_data_set_generic(
        CalcEngine.PYSPARK,  exec_params, data_size=data_size)
    df = raw_data.df_src
    rdd_src: RDD[DataPointNT]
    if raw_data.num_data_points < raw_data.src_num_partitions:
        rdd_src = spark_session.spark.sparkContext.parallelize((
            DataPointNT(*r)  # type: ignore
            for r in cast(list[dict[str, Any]],
                          df.to_records(index=False))
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
        description=data_size,
        data=DataSetDataPyspark(
            src_num_partitions=raw_data.src_num_partitions,
            agg_tgt_num_partitions_1_level=raw_data.tgt_num_partitions_1_level,
            agg_tgt_num_partitions_2_level=raw_data.tgt_num_partitions_2_level,
            df_src=df_src,
            rdd_src=rdd_src,
        ),
        answer=DataSetAnswer(
            vanilla_answer=raw_data.vanilla_answer,
            bilevel_answer=raw_data.bilevel_answer,
            conditional_answer=raw_data.conditional_answer,
        ),
    )
