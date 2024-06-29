import os
import pickle
import random
from dataclasses import dataclass
from pathlib import Path
from typing import NamedTuple, cast

import numpy as np
import pandas as pd
import pyspark.sql.types as DataTypes

from perf_test_common import CalcEngine
from t_utils.pandas_helpers import PandasSeriesOfFloat
from t_utils.t_utils import always_true, int_divide_round_up

SHARED_LOCAL_TEST_DATA_FILE_LOCATION = "d:/temp/SparkPerfTesting"
MAX_DATA_POINTS_PER_SPARK_PARTITION = 5 * 10**3
MAX_DATA_POINTS_PER_DASK_PARTITION = 1 * 10**5


@dataclass(frozen=True)
class ExecutionParameters:
    DefaultParallelism: int
    TestDataFolderLocation: str


class DataPoint(NamedTuple):
    id: int
    grp: int
    subgrp: int
    A: float
    B: float
    C: float
    D: float
    E: float
    F: float


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
class DataSetDescription:
    SizeCode: str
    NumDataPoints: int
    NumGroups: int
    NumSubGroups: int
    RelativeCardinalityBetweenGroupings: int


@dataclass(frozen=True)
class DataSetAnswer():
    vanilla_answer: pd.DataFrame
    bilevel_answer: pd.DataFrame
    conditional_answer: pd.DataFrame


@dataclass(frozen=True)
class RunResult:
    engine: CalcEngine
    dataSize: int
    elapsedTime: float
    recordCount: int


def populate_data_set_generic(exec_params: ExecutionParameters, num_grp_1: int, num_grp_2: int, repetition: int):
    num_data_points = num_grp_1 * num_grp_2 * repetition
    # Need to split this up, upfront, into many partitions
    # to avoid memory issues and
    # avoid preferential treatment of methods that don't repartition
    tgt_num_partitions = num_grp_1 * num_grp_2
    src_num_partitions = max(
        exec_params.DefaultParallelism,
        int_divide_round_up(
            num_data_points,
            MAX_DATA_POINTS_PER_DASK_PARTITION))
    staging_file_name_csv = os.path.join(
        exec_params.TestDataFolderLocation,
        "SixField_Test_Data",
        f"SixFieldTestData_{num_grp_1}_{num_grp_2}_{repetition}.parquet")
    if os.path.exists(staging_file_name_csv) is False:
        generate_data_to_file(
            file_name=staging_file_name_csv,
            numGrp1=num_grp_1,
            numGrp2=num_grp_2,
            repetition=repetition,
        )
    df = pd.read_parquet(staging_file_name_csv)
    assert len(df) == num_data_points
    vanilla_answer: pd.DataFrame \
        = pd.DataFrame.from_records(
            [
                {
                    "grp": grp,
                    "subgrp": subgrp,
                    "mean_of_C": df_cluster["C"].mean(),
                    "max_of_D": df_cluster["D"].max(),
                    "var_of_E": df_cluster["E"].var(ddof=0),
                    "var_of_E2": df_cluster["E"].var(ddof=0),
                }
                for grp in range(num_grp_1)
                for subgrp in range(num_grp_2)
                if always_true(df_cluster := df[(df["grp"] == grp) & (df["subgrp"] == subgrp)])
            ])

    def hand_coded_variance(E: PandasSeriesOfFloat) -> float:
        return (
            (E * E).sum() / E.count() -
            (E.sum() / E.count())**2
        )
    bilevel_answer \
        = pd.DataFrame.from_records(  # type: ignore
            [
                {
                    "grp": grp,
                    "mean_of_C": df_cluster.C.mean(),  # type: ignore
                    "max_of_D": df_cluster.D.max(),  # type: ignore
                    "avg_var_of_E": sub_group_e.var(ddof=0).mean(),  # type: ignore
                    "avg_var_of_E2":
                    sub_group_e
                        .agg(  # type: ignore
                            hand_coded_variance)
                        .mean(),  # type: ignore
                }
                for grp in range(num_grp_1)
                if always_true(df_cluster := df[(df["grp"] == grp)])
                if always_true(sub_group_e :=
                               cast(PandasSeriesOfFloat,
                                    df_cluster.groupby(  # type: ignore
                                        by=['subgrp'])['E']))
            ])
    conditional_answer = pd.DataFrame.from_records(  # type: ignore
        [
            {
                "grp": grp,
                "subgrp": subgrp,
                "mean_of_C": df_cluster.C.mean(),  # type: ignore
                "max_of_D": df_cluster.D.max(),  # type: ignore
                "cond_var_of_E": negE.var(ddof=0),  # type: ignore
                "cond_var_of_E2":
                negE
                .agg(  # type: ignore
                    hand_coded_variance),
            }
            for grp in range(num_grp_1)
            for subgrp in range(num_grp_2)
            if always_true(df_cluster := df[(df["grp"] == grp) & (df["subgrp"] == subgrp)])
            if always_true(negE := cast(PandasSeriesOfFloat, df_cluster[df_cluster["E"] < 0]['E']))
        ])
    print(f"Using {num_grp_1}, {num_grp_2}, {repetition} "
          f"tgt_num_partitions={src_num_partitions} "
          f"each {num_grp_1 * num_grp_2 * repetition/src_num_partitions:.1f}")

    return num_data_points, tgt_num_partitions, src_num_partitions, df, \
        vanilla_answer, bilevel_answer, conditional_answer


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
    df['A'] = np.random.randint(1, repetition + 1, num_data_points, dtype=np.int32)
    df['B'] = np.random.randint(1, repetition + 1, num_data_points, dtype=np.int32)
    df['C'] = np.random.uniform(1, 10, num_data_points)
    df['D'] = np.random.uniform(1, 10, num_data_points)
    df['E'] = np.random.normal(0, 10, num_data_points)
    df['F'] = np.random.normal(1, 10, num_data_points)
    Path(file_name).parent.mkdir(parents=True, exist_ok=True)
    tmp_file_name = f'{file_name}_t'
    with open(tmp_file_name, "wb") as fh:
        df.to_parquet(fh)
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
