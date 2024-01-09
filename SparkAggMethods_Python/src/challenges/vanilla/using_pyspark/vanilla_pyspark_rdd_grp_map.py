from typing import Iterable

from pyspark.sql import Row

from six_field_test_data.six_generate_test_data_using_pyspark import (
    PysparkDataSet, TPysparkPythonPendingAnswerSet)
from six_field_test_data.six_test_data_types import (
    MAX_DATA_POINTS_PER_SPARK_PARTITION, DataPoint, ExecutionParameters)
from utils.tidy_spark_session import TidySparkSession


def vanilla_pyspark_rdd_grp_map(
        _spark_session: TidySparkSession,
        _exec_params: ExecutionParameters,
        data_set: PysparkDataSet
) -> TPysparkPythonPendingAnswerSet:

    if (
            data_set.description.NumDataPoints
            > MAX_DATA_POINTS_PER_SPARK_PARTITION
            * data_set.description.NumGroups * data_set.description.NumSubGroups
    ):
        # This strategy only works if all of the values per key can fit into memory at once.
        return "infeasible"

    rddResult = (
        data_set.data.rddSrc
        .groupBy(lambda x: (x.grp, x.subgrp))
        .map(lambda pair: process_data_1(pair[0], pair[1]))
        .repartition(numPartitions=1)
        .sortByKey()  # type: ignore
        .values()
    )
    return rddResult


def process_data_1(
        key: tuple[int, int],
        iterator: Iterable[DataPoint],
) -> tuple[tuple[int, int], Row]:
    import math
    sum_of_C = 0
    count = 0
    max_of_D = None
    sum_of_E_squared = 0
    sum_of_E = 0
    for item in iterator:
        sum_of_C = sum_of_C + item.C
        count = count + 1
        max_of_D = item.D \
            if max_of_D is None or max_of_D < item.D \
            else max_of_D
        sum_of_E_squared += item.E * item.E
        sum_of_E += item.E
    mean_of_C = sum_of_C / count \
        if count > 0 else math.nan
    var_of_E = sum_of_E_squared / count - (sum_of_E / count)**2
    return (
        key,
        Row(grp=key[0],
            subgrp=key[1],
            mean_of_C=mean_of_C,
            max_of_D=max_of_D,
            var_of_E=var_of_E))
