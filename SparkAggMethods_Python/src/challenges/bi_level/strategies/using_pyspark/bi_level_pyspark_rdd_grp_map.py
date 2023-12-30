from typing import Iterable, cast

from pyspark import RDD
from pyspark.sql import Row

from six_field_test_data.six_generate_test_data_using_pyspark import (
    PysparkDataSet, PysparkPythonPendingAnswerSet)
from six_field_test_data.six_test_data_types import (
    MAX_DATA_POINTS_PER_SPARK_PARTITION, DataPoint, ExecutionParameters)
from utils.tidy_spark_session import TidySparkSession


def bi_level_pyspark_rdd_grp_map(
        spark_session: TidySparkSession,
        _exec_params: ExecutionParameters,
        data_set: PysparkDataSet
) -> PysparkPythonPendingAnswerSet:
    rddSrc: RDD[DataPoint] = data_set.data.rddSrc

    if (
            data_set.description.NumDataPoints
            > MAX_DATA_POINTS_PER_SPARK_PARTITION
            * data_set.description.NumGroups
    ):
        raise ValueError(
            "This strategy only works if all of the values per key can fit into memory at once.")

    rddResult = cast(
        RDD[Row],
        rddSrc
        .groupBy(lambda x: x.grp)
        .map(lambda pair: process_data_1(pair[0], pair[1]))
        .repartition(numPartitions=1)
        .sortByKey()  # type: ignore
        .values()
    )

    return PysparkPythonPendingAnswerSet(rdd_row=rddResult)


class MutableRunningTotal:
    running_sub_sum_of_E_squared: float
    running_sub_sum_of_E: float
    running_sub_count: int

    def __init__(
            self,
            grp: int,
    ):
        self.grp = grp
        self.running_sub_sum_of_E_squared = 0
        self.running_sub_sum_of_E = 0
        self.running_sub_count = 0


def process_data_1(
        grp: int,
        iterator: Iterable[DataPoint]
) -> tuple[int, Row]:
    import math
    import statistics
    running_sum_of_C = 0
    running_grp_count = 0
    running_max_of_D = None
    running_subs_of_E: dict[int, MutableRunningTotal] = dict()

    for item in iterator:
        running_sum_of_C += item.C
        running_grp_count += 1
        running_max_of_D = item.D \
            if running_max_of_D is None or \
            running_max_of_D < item.D \
            else running_max_of_D
        if item.subgrp not in running_subs_of_E:
            running_subs_of_E[item.subgrp] = MutableRunningTotal(grp)
        running_sub = running_subs_of_E[item.subgrp]
        running_sub.running_sub_sum_of_E_squared += \
            item.E * item.E
        running_sub.running_sub_sum_of_E += item.E
        running_sub.running_sub_count += 1
    mean_of_C = running_sum_of_C / running_grp_count \
        if running_grp_count > 0 else math.nan
    ar = [(
        x.running_sub_sum_of_E_squared / x.running_sub_count
        - (x.running_sub_sum_of_E / x.running_sub_count)**2)
        for x in running_subs_of_E.values()]
    avg_var_of_E = statistics.mean(ar)
    return (grp,
            Row(grp=grp,
                mean_of_C=mean_of_C,
                max_of_D=running_max_of_D,
                avg_var_of_E=avg_var_of_E))
