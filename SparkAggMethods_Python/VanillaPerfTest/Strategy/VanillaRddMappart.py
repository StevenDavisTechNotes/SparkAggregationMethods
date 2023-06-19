from typing import List, Tuple, Optional

from dataclasses import dataclass
import math

from pyspark import RDD, Row
from pyspark.sql import SparkSession, DataFrame as spark_DataFrame

from ..VanillaTestData import DataPointAsTuple


@dataclass(frozen=True)
class SubTotal:
    running_sum_of_C: float
    running_count: int
    running_max_of_D: Optional[float]
    running_sum_of_E_squared: float
    running_sum_of_E: float


class MutableRunningTotal:
    def __init__(self):
        self.running_sum_of_C = 0
        self.running_count = 0
        self.running_max_of_D = None
        self.running_sum_of_E_squared = 0
        self.running_sum_of_E = 0


def vanilla_rdd_mappart(
    spark: SparkSession, pyData: List[DataPointAsTuple]
) -> Tuple[Optional[RDD], Optional[spark_DataFrame]]:

    def partitionTriage(iterator):
        running_subtotals = {}
        for v in iterator:
            k = (v.grp, v.subgrp)
            if k not in running_subtotals:
                running_subtotals[k] = MutableRunningTotal()
            sub = running_subtotals[k]
            sub.running_sum_of_C += v.C
            sub.running_count += 1
            sub.running_max_of_D = \
                sub.running_max_of_D \
                if sub.running_max_of_D is not None and \
                sub.running_max_of_D > v.D \
                else v.D
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

    def mergeCombiners3(key, iterable):
        lsub = MutableRunningTotal()
        for rsub in iterable:
            lsub.running_sum_of_C += rsub.running_sum_of_C
            lsub.running_count += rsub.running_count
            lsub.running_max_of_D = lsub.running_max_of_D \
                if lsub.running_max_of_D is not None and \
                lsub.running_max_of_D > rsub.running_max_of_D \
                else rsub.running_max_of_D
            lsub.running_sum_of_E_squared += \
                rsub.running_sum_of_E_squared
            lsub.running_sum_of_E += rsub.running_sum_of_E
        return SubTotal(
            running_sum_of_C=lsub.running_sum_of_C,
            running_count=lsub.running_count,
            running_max_of_D=lsub.running_max_of_D,
            running_sum_of_E_squared=lsub.running_sum_of_E_squared,
            running_sum_of_E=lsub.running_sum_of_E)

    def finalAnalytics2(key, final):
        sum_of_C = final.running_sum_of_C
        count = final.running_count
        max_of_D = final.running_max_of_D
        sum_of_E_squared = final.running_sum_of_E_squared
        sum_of_E = final.running_sum_of_E
        return Row(
            grp=key[0], subgrp=key[1],
            mean_of_C=math.nan
            if count < 1 else
            sum_of_C/count,
            max_of_D=max_of_D,
            var_of_E=math.nan
            if count < 2 else
            (
                sum_of_E_squared -
                sum_of_E * sum_of_E / count
            ) / (count - 1))

    sc = spark.sparkContext
    rddData = sc.parallelize(pyData)
    sumCount = rddData \
        .mapPartitions(partitionTriage) \
        .groupByKey() \
        .map(lambda kv: (kv[0], mergeCombiners3(kv[0], kv[1]))) \
        .map(lambda kv: finalAnalytics2(kv[0], kv[1]))
    rddResult = sumCount.sortBy(lambda x: (x.grp, x.subgrp))  # type: ignore
    return rddResult, None