from typing import Iterable, cast

from pyspark import RDD

from six_field_test_data.six_generate_test_data import (
    DataSetPyspark, GrpTotal, TChallengePendingAnswerPythonPyspark)
from six_field_test_data.six_test_data_types import (
    MAX_DATA_POINTS_PER_SPARK_PARTITION, DataPoint, ExecutionParameters)
from utils.tidy_spark_session import TidySparkSession


def cond_pyspark_rdd_grp_map(
        spark_session: TidySparkSession,
        exec_params: ExecutionParameters,
        data_set: DataSetPyspark,
) -> TChallengePendingAnswerPythonPyspark:
    if (
            data_set.description.NumDataPoints
            > MAX_DATA_POINTS_PER_SPARK_PARTITION
            * data_set.description.NumGroups * data_set.description.NumSubGroups
    ):
        # This strategy only works if all of the values per key can fit into memory at once.
        return "infeasible"
    rddResult = cast(
        RDD[GrpTotal],
        data_set.data.rddSrc
        .groupBy(lambda x: (x.grp, x.subgrp))
        .map(lambda pair: process_data_1(pair[0], pair[1]))
        .sortByKey()  # type: ignore
        .values()
    )
    return rddResult


def process_data_1(
        key: tuple[int, int],
        iterator: Iterable[DataPoint],
) -> tuple[tuple[int, int], GrpTotal]:
    import math
    sum_of_C = 0
    unconditional_count = 0
    max_of_D: float | None = None
    cond_sum_of_E_squared = 0
    cond_sum_of_E = 0
    cond_count_of_E = 0
    for item in iterator:
        sum_of_C = sum_of_C + item.C
        unconditional_count = unconditional_count + 1
        max_of_D = item.D \
            if max_of_D is None or max_of_D < item.D \
            else max_of_D
        if item.E < 0:
            cond_sum_of_E_squared = \
                cond_sum_of_E_squared + item.E * item.E
            cond_sum_of_E = cond_sum_of_E + item.E
            cond_count_of_E = cond_count_of_E + 1
    mean_of_C = sum_of_C / unconditional_count \
        if unconditional_count > 0 else math.nan
    cond_var_of_E = (
        cond_sum_of_E_squared / cond_count_of_E
        - (cond_sum_of_E / cond_count_of_E)**2)
    return (key,
            GrpTotal(
                grp=key[0],
                subgrp=key[1],
                mean_of_C=mean_of_C,
                max_of_D=max_of_D,
                cond_var_of_E=cond_var_of_E))
