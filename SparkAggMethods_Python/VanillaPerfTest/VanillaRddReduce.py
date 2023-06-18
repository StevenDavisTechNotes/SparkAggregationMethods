from typing import List, Tuple, Optional

from dataclasses import dataclass
import math

from pyspark import RDD, Row
from pyspark.sql import SparkSession, DataFrame as spark_DataFrame

from .VanillaTestData import DataPoint


def vanilla_rdd_reduce(
    spark: SparkSession, pyData: List[DataPoint]
) -> Tuple[Optional[RDD], Optional[spark_DataFrame]]:
    sc = spark.sparkContext

    @dataclass(frozen=True)
    class SubTotal:
        running_sum_of_C: float
        running_count: int
        running_max_of_D: Optional[float]
        running_sum_of_E_squared: float
        running_sum_of_E: float

    rddData = sc.parallelize(pyData)
    # SubTotal = collections.namedtuple("SubTotal",
    #                                   ["running_sum_of_C", "running_count", "running_max_of_D",
    #                                    "running_sum_of_E_squared", "running_sum_of_E"])

    def mergeValue2(sub, v):
        running_sum_of_C = sub.running_sum_of_C + v.C
        running_count = sub.running_count + 1
        running_max_of_D = sub.running_max_of_D \
            if sub.running_max_of_D is not None and \
            sub.running_max_of_D > v.D \
            else v.D
        running_sum_of_E_squared = sub.running_sum_of_E_squared
        running_sum_of_E = sub.running_sum_of_E
        running_sum_of_E_squared += v.E * v.E
        running_sum_of_E += v.E
        return SubTotal(
            running_sum_of_C,
            running_count,
            running_max_of_D,
            running_sum_of_E_squared,
            running_sum_of_E)

    def createCombiner2(v):
        return mergeValue2(SubTotal(
            running_sum_of_C=0,
            running_count=0,
            running_max_of_D=None,
            running_sum_of_E_squared=0,
            running_sum_of_E=0), v)

    def mergeCombiners2(lsub, rsub):
        return SubTotal(
            running_sum_of_C=lsub.running_sum_of_C + rsub.running_sum_of_C,
            running_count=lsub.running_count + rsub.running_count,
            running_max_of_D=lsub.running_max_of_D
            if lsub.running_max_of_D is not None and
            lsub.running_max_of_D > rsub.running_max_of_D
            else rsub.running_max_of_D,
            running_sum_of_E_squared=lsub.running_sum_of_E_squared +
            rsub.running_sum_of_E_squared,
            running_sum_of_E=lsub.running_sum_of_E + rsub.running_sum_of_E)

    def finalAnalytics2(key, total):
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
            if count < 2 else
            (
                sum_of_E_squared -
                sum_of_E * sum_of_E / count
            ) / (count - 1))

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
