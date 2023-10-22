import math
from typing import Tuple

from pyspark import RDD
from pyspark.sql import DataFrame as spark_DataFrame

from ConditionalPerfTest.CondDataTypes import GrpTotal, SubTotal
from SixFieldCommon.PySpark_SixFieldTestData import PysparkDataSet
from SixFieldCommon.SixFieldTestData import DataPoint, ExecutionParameters
from Utils.TidySparkSession import TidySparkSession


def cond_rdd_reduce(
        spark_session: TidySparkSession,
        _exec_params: ExecutionParameters,
        data_set: PysparkDataSet,
) -> Tuple[RDD[GrpTotal] | None, spark_DataFrame | None]:
    rddSumCount = (
        data_set.data.rddSrc
        .map(lambda x: ((x.grp, x.subgrp), x))
        .combineByKey(createCombiner2,
                      mergeValue2,
                      mergeCombiners2,
                      numPartitions=data_set.description.NumGroups * data_set.description.NumSubGroups)
        .map(lambda kv: (kv[0], finalAnalytics2(kv[0], kv[1])))
        .sortByKey()  # type: ignore
        .values()
    )
    return rddSumCount, None


def mergeValue2(
        sub: SubTotal,
        v: DataPoint,
) -> SubTotal:
    running_sum_of_C = sub.running_sum_of_C + v.C
    running_uncond_count = sub.running_uncond_count + 1
    running_max_of_D = sub.running_max_of_D \
        if sub.running_max_of_D is not None and \
        sub.running_max_of_D > v.D \
        else v.D
    running_cond_sum_of_E_squared = sub.running_cond_sum_of_E_squared
    running_cond_sum_of_E = sub.running_cond_sum_of_E
    running_cond_count = sub.running_cond_count
    if v.E < 0:
        running_cond_sum_of_E_squared += v.E * v.E
        running_cond_sum_of_E += v.E
        running_cond_count += 1
    return SubTotal(
        running_sum_of_C,
        running_uncond_count,
        running_max_of_D,
        running_cond_sum_of_E_squared,
        running_cond_sum_of_E,
        running_cond_count)


def createCombiner2(
        v: DataPoint,
) -> SubTotal:
    return mergeValue2(SubTotal(
        running_sum_of_C=0,
        running_uncond_count=0,
        running_max_of_D=None,
        running_cond_sum_of_E_squared=0,
        running_cond_sum_of_E=0,
        running_cond_count=0), v)


def mergeCombiners2(
        lsub: SubTotal,
        rsub: SubTotal,
) -> SubTotal:
    return SubTotal(
        running_sum_of_C=lsub.running_sum_of_C + rsub.running_sum_of_C,
        running_uncond_count=lsub.running_uncond_count + rsub.running_uncond_count,
        running_max_of_D=lsub.running_max_of_D
        if lsub.running_max_of_D is not None and
        lsub.running_max_of_D > rsub.running_max_of_D
        else rsub.running_max_of_D,
        running_cond_sum_of_E_squared=lsub.running_cond_sum_of_E_squared +
        rsub.running_cond_sum_of_E_squared,
        running_cond_sum_of_E=lsub.running_cond_sum_of_E + rsub.running_cond_sum_of_E,
        running_cond_count=lsub.running_cond_count + rsub.running_cond_count)


def finalAnalytics2(
        key: Tuple[int, int],
        total: SubTotal,
) -> GrpTotal:
    sum_of_C = total.running_sum_of_C
    uncond_count = total.running_uncond_count
    max_of_D = total.running_max_of_D
    cond_sum_of_E_squared = total.running_cond_sum_of_E_squared
    cond_sum_of_E = total.running_cond_sum_of_E
    cond_count = total.running_cond_count
    return GrpTotal(
        grp=key[0], subgrp=key[1],
        mean_of_C=math.nan
        if cond_count < 1 else
        sum_of_C / uncond_count,
        max_of_D=max_of_D,
        cond_var_of_E=(
            cond_sum_of_E_squared / cond_count
            - (cond_sum_of_E / cond_count)**2)
    )
