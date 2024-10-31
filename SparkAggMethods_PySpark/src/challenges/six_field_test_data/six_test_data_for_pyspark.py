from dataclasses import dataclass
from typing import Literal, Protocol

from pyspark import RDD, StorageLevel
from pyspark.sql import DataFrame as PySparkDataFrame
from pyspark.sql import Row
from spark_agg_methods_common_python.challenges.six_field_test_data.six_test_data_types import (
    DataPointNT, SixTestDataChallengeMethodRegistrationBase, SixTestDataSetDescription, SixTestExecutionParameters,
    six_derive_source_test_data_file_path,
)
from spark_agg_methods_common_python.perf_test_common import (
    CalcEngine, Challenge, NumericalToleranceExpectations, SolutionInterfacePySpark, SolutionInterfaceScalaSpark,
    SolutionLanguage,
)
from spark_agg_methods_common_python.utils.utils import int_divide_round_up

from src.utils.tidy_session_pyspark import TidySparkSession

MAX_DATA_POINTS_PER_SPARK_PARTITION = 5 * 10**3


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


def six_populate_data_set_pyspark(
        spark_session: TidySparkSession,
        exec_params: SixTestExecutionParameters,
        data_description: SixTestDataSetDescription,
) -> SixFieldDataSetDataPyspark:
    num_grp_1 = data_description.num_grp_1
    num_grp_2 = data_description.num_grp_2
    points_per_index = data_description.points_per_index
    num_source_rows = num_grp_1 * num_grp_2 * points_per_index

    source_file_name_parquet, source_file_name_csv = six_derive_source_test_data_file_path(
        data_description=data_description,
    )
    max_data_points_per_partition = MAX_DATA_POINTS_PER_SPARK_PARTITION
    src_num_partitions = (
        1 if max_data_points_per_partition < 0 else
        max(
            exec_params.default_parallelism,
            int_divide_round_up(
                num_source_rows,
                max_data_points_per_partition,
            )
        )
    )
    df_src = spark_session.spark.read.parquet(source_file_name_parquet)
    df_src = df_src.repartition(src_num_partitions, df_src.Id)
    rdd_src: RDD[DataPointNT] = df_src.rdd.map(lambda r: DataPointNT(*r))
    df_src.persist(StorageLevel.DISK_ONLY)
    rdd_src.persist(StorageLevel.DISK_ONLY)
    cnt, parts = rdd_src.count(), rdd_src.getNumPartitions()
    print("Found rdd %i rows in %i parts ratio %.1f" % (cnt, parts, cnt / parts))
    assert cnt == num_source_rows

    cnt, parts = df_src.count(), df_src.rdd.getNumPartitions()
    print("Found df %i rows in %i parts ratio %.1f" % (cnt, parts, cnt / parts))
    assert cnt == num_source_rows

    return SixFieldDataSetDataPyspark(
        src_num_partitions=src_num_partitions,
        agg_tgt_num_partitions_1_level=num_grp_1,
        agg_tgt_num_partitions_2_level=num_grp_1 * num_grp_2,
        df_src=df_src,
        rdd_src=rdd_src,
    )
