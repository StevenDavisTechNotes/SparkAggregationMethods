import math
from dataclasses import dataclass
from typing import Iterable, NamedTuple, Optional

from pyspark.sql import Row

from six_field_test_data.six_generate_test_data import (
    DataSetPyspark, TChallengePendingAnswerPythonPyspark)
from six_field_test_data.six_test_data_types import (DataPoint,
                                                     ExecutionParameters)
from utils.tidy_spark_session import TidySparkSession


class SubTotal(NamedTuple):
    running_sum_of_C: float
    running_count: int
    running_max_of_D: Optional[float]
    running_sum_of_E_squared: float
    running_sum_of_E: float


@dataclass(frozen=False)
class MutableRunningTotal:
    running_sum_of_C: float
    running_count: int
    running_max_of_D: Optional[float]
    running_sum_of_E_squared: float
    running_sum_of_E: float

    @staticmethod
    def zero():
        return MutableRunningTotal(
            running_sum_of_C=0,
            running_count=0,
            running_max_of_D=None,
            running_sum_of_E_squared=0,
            running_sum_of_E=0)


def vanilla_pyspark_rdd_mappart(
        spark_session: TidySparkSession,
        exec_params: ExecutionParameters,
        data_set: DataSetPyspark
) -> TChallengePendingAnswerPythonPyspark:
    rddSrc = data_set.data.rddSrc
    sumCount = (
        rddSrc
        .mapPartitions(partition_triage)
        .groupByKey()
        .map(lambda kv: (kv[0], merge_combiners_3(kv[0], kv[1])))
        .map(lambda kv: final_analytics_2(kv[0], kv[1]))
    )
    rddResult = sumCount.sortBy(
        lambda x: (x.grp, x.subgrp),  # type: ignore
        numPartitions=data_set.data.AggTgtNumPartitions)
    return rddResult


def partition_triage(
        iterator: Iterable[DataPoint],
) -> Iterable[tuple[tuple[int, int], SubTotal]]:
    running_subtotals = {}
    for v in iterator:
        k = (v.grp, v.subgrp)
        if k not in running_subtotals:
            running_subtotals[k] = MutableRunningTotal.zero()
        sub = running_subtotals[k]
        sub.running_sum_of_C += v.C
        sub.running_count += 1
        sub.running_max_of_D = max(sub.running_max_of_D, v.D)
        sub.running_sum_of_E_squared += v.E * v.E
        sub.running_sum_of_E += v.E
    for k in running_subtotals:
        sub = running_subtotals[k]
        yield (k, SubTotal(
            running_sum_of_C=sub.running_sum_of_C,
            running_count=sub.running_count,
            running_max_of_D=sub.running_max_of_D,
            running_sum_of_E_squared=sub.running_sum_of_E_squared,
            running_sum_of_E=sub.running_sum_of_E))


def max(
        lhs: Optional[float],
        rhs: Optional[float],
) -> Optional[float]:
    if lhs is None:
        return rhs
    if rhs is None:
        return lhs
    if lhs > rhs:
        return lhs
    return rhs


def merge_combiners_3(
        key: tuple[int, int],
        iterable: Iterable[SubTotal],
) -> SubTotal:
    lsub = MutableRunningTotal.zero()
    for rsub in iterable:
        lsub.running_sum_of_C += rsub.running_sum_of_C
        lsub.running_count += rsub.running_count
        lsub.running_max_of_D = max(lsub.running_max_of_D, rsub.running_max_of_D)
        lsub.running_sum_of_E_squared += \
            rsub.running_sum_of_E_squared
        lsub.running_sum_of_E += rsub.running_sum_of_E
    return SubTotal(
        running_sum_of_C=lsub.running_sum_of_C,
        running_count=lsub.running_count,
        running_max_of_D=lsub.running_max_of_D,
        running_sum_of_E_squared=lsub.running_sum_of_E_squared,
        running_sum_of_E=lsub.running_sum_of_E)


def final_analytics_2(
        key: tuple[int, int],
        final: SubTotal,
) -> Row:
    sum_of_C = final.running_sum_of_C
    count = final.running_count
    max_of_D = final.running_max_of_D
    sum_of_E_squared = final.running_sum_of_E_squared
    sum_of_E = final.running_sum_of_E
    return Row(
        grp=key[0], subgrp=key[1],
        mean_of_C=math.nan
        if count < 1 else
        sum_of_C / count,
        max_of_D=max_of_D,
        var_of_E=(
            sum_of_E_squared / count -
            (sum_of_E / count)**2)
    )
