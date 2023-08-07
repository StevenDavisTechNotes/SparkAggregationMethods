import collections
import math
from typing import Iterable, Optional, Tuple

from pyspark import RDD
from pyspark.sql import DataFrame as spark_DataFrame
from pyspark.sql import Row

from SixFieldCommon.SixFieldTestData import DataSet, ExecutionParameters, DataPoint
from Utils.SparkUtils import TidySparkSession

SubTotal = collections.namedtuple(
    "SubTotal",
    ["running_sum_of_C", "running_count", "running_max_of_D",
     "running_sum_of_E_squared", "running_sum_of_E"])


def bi_rdd_reduce2(
    spark_session: TidySparkSession,
    _exec_params: ExecutionParameters,
    data_set: DataSet
) -> Tuple[Optional[RDD], Optional[spark_DataFrame]]:
    rddSrc = data_set.data.rddSrc

    rddResult = (
        rddSrc
        .map(lambda x: ((x.grp, x.subgrp), x))
        .combineByKey(createCombiner,
                      mergeValue,
                      mergeCombiners,
                      numPartitions=data_set.data.AggTgtNumPartitions)
        .map(lambda x: (x[0][0], x[1]))
        .groupByKey(numPartitions=1)
        .map(lambda x: (x[0], finalAnalytics(x[0], x[1])))
        .sortByKey()
        .values()
    )
    return rddResult, None


def mergeValue(
        pre: SubTotal,
        v: DataPoint,
) -> SubTotal:
    return SubTotal(
        running_sum_of_C=pre.running_sum_of_C + v.C,
        running_count=pre.running_count + 1,
        running_max_of_D=pre.running_max_of_D
        if pre.running_max_of_D is not None and
        pre.running_max_of_D > v.D
        else v.D,
        running_sum_of_E_squared=pre.running_sum_of_E_squared +
        v.E * v.E,
        running_sum_of_E=pre.running_sum_of_E + v.E)


def createCombiner(
        v: DataPoint,
) -> SubTotal:
    return mergeValue(SubTotal(
        running_sum_of_C=0,
        running_count=0,
        running_max_of_D=None,
        running_sum_of_E_squared=0,
        running_sum_of_E=0), v)


def mergeCombiners(
        lsub: SubTotal,
        rsub: SubTotal,
) -> SubTotal:
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


def finalAnalytics(
        grp: int,
        iterator: Iterable[SubTotal]
) -> Row:
    running_sum_of_C = 0
    running_grp_count = 0
    running_max_of_D = None
    running_sum_of_var_of_E = 0
    running_count_of_subgrp = 0

    for sub in iterator:
        count = sub.running_count
        running_sum_of_C += sub.running_sum_of_C
        running_grp_count += count
        running_max_of_D = sub.running_max_of_D \
            if running_max_of_D is None or \
            running_max_of_D < sub.running_max_of_D \
            else running_max_of_D
        var_of_E = (
            sub.running_sum_of_E_squared / count
            - (sub.running_sum_of_E / count)**2
        )
        running_sum_of_var_of_E += var_of_E
        running_count_of_subgrp += 1

    return Row(
        grp=grp,
        mean_of_C=math.nan
        if running_grp_count < 1 else
        running_sum_of_C / running_grp_count,
        max_of_D=running_max_of_D,
        avg_var_of_E=running_sum_of_var_of_E / running_count_of_subgrp)
