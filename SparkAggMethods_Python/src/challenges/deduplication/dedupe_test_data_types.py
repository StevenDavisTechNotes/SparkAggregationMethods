import inspect
import math
from dataclasses import dataclass
from typing import Literal, Protocol

import pyspark.sql.types as DataTypes
from pyspark import RDD
from pyspark.sql import DataFrame as PySparkDataFrame
from pyspark.sql import Row
from spark_agg_methods_common_python.perf_test_common import (
    CalcEngine, ChallengeMethodRegistrationBase, DataSetDescriptionBase, SolutionInterfacePySpark, SolutionLanguage,
)

from src.utils.tidy_spark_session import TidySparkSession


@dataclass(frozen=True)
class ExecutionParameters:
    in_cloud_mode: bool
    num_executors: int
    can_assume_no_dupes_per_partition: bool
    default_parallelism: int
    test_data_folder_location: str


RecordSparseStruct = DataTypes.StructType([
    DataTypes.StructField("FirstName", DataTypes.StringType(), False),
    DataTypes.StructField("LastName", DataTypes.StringType(), False),
    DataTypes.StructField("StreetAddress", DataTypes.StringType(), True),
    DataTypes.StructField("City", DataTypes.StringType(), True),
    DataTypes.StructField("ZipCode", DataTypes.StringType(), False),
    DataTypes.StructField("SecretKey", DataTypes.IntegerType(), False),
    DataTypes.StructField("FieldA", DataTypes.StringType(), True),
    DataTypes.StructField("FieldB", DataTypes.StringType(), True),
    DataTypes.StructField("FieldC", DataTypes.StringType(), True),
    DataTypes.StructField("FieldD", DataTypes.StringType(), True),
    DataTypes.StructField("FieldE", DataTypes.StringType(), True),
    DataTypes.StructField("FieldF", DataTypes.StringType(), True),
])


class DedupeDataSetDescription(DataSetDescriptionBase):
    # for DataSetDescriptionBase
    debugging_only: bool
    num_source_rows: int
    size_code: str
    # for DedupeDataSetDescription
    num_people: int
    num_b_recs: int
    num_sources: int
    data_size_exponent: int

    def __init__(
            self,
            *,
            num_people: int,
            num_b_recs: int,
            num_sources: int,
    ):
        debugging_only = (num_people == 1)
        num_source_rows = (num_sources - 1) * num_people + num_b_recs
        logical_data_size = num_sources * num_people
        size_code = (
            str(logical_data_size)
            if logical_data_size < 1000 else
            f'{logical_data_size//1000}k'
        )
        super().__init__(
            debugging_only=debugging_only,
            num_source_rows=num_source_rows,
            size_code=size_code,
        )
        self.num_people = num_people
        self.num_b_recs = num_b_recs
        self.num_sources = num_sources
        self.data_size_exponent = round(math.log10(num_source_rows-num_b_recs))

    @classmethod
    def regressor_field_name(cls) -> str:
        regressor_field_name = "num_source_rows"
        assert regressor_field_name in inspect.get_annotations(cls)
        return regressor_field_name


@dataclass(frozen=True)
class DedupeDataSetBase:
    data_description: DedupeDataSetDescription


@dataclass(frozen=True)
class DedupePySparkDataSet(DedupeDataSetBase):
    data_description: DedupeDataSetDescription
    grouped_num_partitions: int
    df: PySparkDataFrame


TChallengePendingAnswerPythonPyspark = Literal["infeasible"] | RDD[Row] | PySparkDataFrame


class IChallengeMethodPythonPyspark(Protocol):
    def __call__(
        self,
        *,
        spark_session: TidySparkSession,
        exec_params: ExecutionParameters,
        data_set: DedupePySparkDataSet
    ) -> TChallengePendingAnswerPythonPyspark: ...


@dataclass(frozen=True)
class DedupeChallengeMethodPythonPysparkRegistration(
    ChallengeMethodRegistrationBase[SolutionInterfacePySpark, IChallengeMethodPythonPyspark]
):
    # for ChallengeMethodRegistrationBase
    strategy_name_2018: str | None
    strategy_name: str
    language: SolutionLanguage
    engine: CalcEngine
    interface: SolutionInterfacePySpark
    requires_gpu: bool
    delegate: IChallengeMethodPythonPyspark


@dataclass(frozen=True)
class DedupeItineraryItem:
    challenge_method_registration: DedupeChallengeMethodPythonPysparkRegistration
    data_set: DedupePySparkDataSet
