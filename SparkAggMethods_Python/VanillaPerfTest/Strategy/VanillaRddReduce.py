from typing import List, Tuple, Optional, NamedTuple

import math

from pyspark import RDD, Row
from pyspark.sql import DataFrame as spark_DataFrame

from Utils.SparkUtils import TidySparkSession

from ..VanillaTestData import DataPoint


class SubTotalDC(NamedTuple):
    running_sum_of_C: float
    running_count: int
    running_max_of_D: Optional[float]
    running_sum_of_E_squared: float
    running_sum_of_E: float


def vanilla_rdd_reduce(
    spark_session: TidySparkSession, pyData: List[DataPoint]
) -> Tuple[Optional[RDD], Optional[spark_DataFrame]]:
    def max(lhs: Optional[float], rhs: Optional[float]) -> Optional[float]:
        if lhs is None:
            return rhs
        if rhs is None:
            return lhs
        if lhs > rhs:
            return lhs
        return rhs

    def createAccumulator() -> SubTotalDC:
        return SubTotalDC(
            running_sum_of_C=0,
            running_count=0,
            running_max_of_D=None,
            running_sum_of_E_squared=0,
            running_sum_of_E=0)

    def mergeValue2(sub: SubTotalDC, v: DataPoint) -> SubTotalDC:
        running_sum_of_E_squared = sub.running_sum_of_E_squared + v.E * v.E
        running_sum_of_E = sub.running_sum_of_E + v.E
        return SubTotalDC(
            running_sum_of_C=sub.running_sum_of_C + v.C,
            running_count=sub.running_count + 1,
            running_max_of_D=max(sub.running_max_of_D, v.D),
            running_sum_of_E_squared=running_sum_of_E_squared,
            running_sum_of_E=running_sum_of_E)

    def createCombiner2(v: DataPoint) -> SubTotalDC:
        return mergeValue2(createAccumulator(), v)

    def mergeCombiners2(lsub: SubTotalDC, rsub: SubTotalDC) -> SubTotalDC:
        return SubTotalDC(
            running_sum_of_C=lsub.running_sum_of_C + rsub.running_sum_of_C,
            running_count=lsub.running_count + rsub.running_count,
            running_max_of_D=max(lsub.running_max_of_D, rsub.running_max_of_D),
            running_sum_of_E_squared=lsub.running_sum_of_E_squared +
            rsub.running_sum_of_E_squared,
            running_sum_of_E=lsub.running_sum_of_E + rsub.running_sum_of_E
        )

    def finalAnalytics2(key: Tuple[int, int], total: SubTotalDC):
        sum_of_C = total.running_sum_of_C
        count = total.running_count
        max_of_D = total.running_max_of_D
        sum_of_E_squared = total.running_sum_of_E_squared
        sum_of_E = total.running_sum_of_E
        return Row(
            grp=key[0], subgrp=key[1],
            mean_of_C=math.nan
            if count < 1 else sum_of_C / count,
            max_of_D=max_of_D,
            var_of_E=math.nan
            if count < 2 else (
                sum_of_E_squared -
                sum_of_E * sum_of_E / count
            ) / (count - 1))

    sc = spark_session.spark_context
    rddData = sc.parallelize(pyData)
    sumCount: RDD[Row] = (
        rddData
        .map(lambda x: ((x.grp, x.subgrp), x))
        .combineByKey(createCombiner2,
                      mergeValue2,
                      mergeCombiners2)
        .map(lambda kv: finalAnalytics2(kv[0], kv[1])))
    rddResult = sumCount.sortBy(
        keyfunc=lambda x: (x.grp, x.subgrp))  # type: ignore
    return rddResult, None
