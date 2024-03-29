import collections
import math
from typing import Optional, Tuple

from pyspark import RDD
from pyspark.sql import DataFrame as spark_DataFrame
from pyspark.sql import Row

from SixFieldCommon.SixFieldTestData import DataSet, ExecutionParameters, DataPoint
from Utils.TidySparkSession import TidySparkSession

SubTotal1 = collections.namedtuple(
    "SubTotal1",
    ["running_sum_of_C", "running_max_of_D",
     "subgrp_running_totals"])
SubTotal2 = collections.namedtuple(
    "SubTotal2",
    ["running_sum_of_E_squared",
     "running_sum_of_E", "running_count"])


def bi_rdd_reduce1(
        spark_session: TidySparkSession,
        _exec_params: ExecutionParameters,
        data_set: DataSet
) -> Tuple[Optional[RDD], Optional[spark_DataFrame]]:  # noqa: C901
    rddSrc = data_set.data.rddSrc

    rddResult = (
        rddSrc
        .map(lambda x: (x.grp, x))
        .combineByKey(createCombiner,
                      mergeValue,
                      mergeCombiners,
                      numPartitions=data_set.data.AggTgtNumPartitions)
        .sortByKey()
        .map(lambda x: finalAnalytics(x[0], x[1]))
    )
    return rddResult, None


def mergeValue(
        pre: SubTotal1,
        v: DataPoint,
) -> SubTotal1:
    subgrp_running_totals = pre.subgrp_running_totals.copy()
    if v.subgrp not in subgrp_running_totals:
        subgrp_running_totals[v.subgrp] = \
            SubTotal2(
                running_sum_of_E_squared=0,
                running_sum_of_E=0,
                running_count=0
        )
    subsub = subgrp_running_totals[v.subgrp]
    subgrp_running_totals[v.subgrp] = SubTotal2(
        subsub.running_sum_of_E_squared + v.E * v.E,
        subsub.running_sum_of_E + v.E,
        subsub.running_count + 1)
    return SubTotal1(
        running_sum_of_C=pre.running_sum_of_C + v.C,
        running_max_of_D=pre.running_max_of_D
        if pre.running_max_of_D is not None and
        pre.running_max_of_D > v.D
        else v.D,
        subgrp_running_totals=subgrp_running_totals)


def createCombiner(
        v: DataPoint,
) -> SubTotal1:
    return mergeValue(SubTotal1(
        running_sum_of_C=0,
        running_max_of_D=None,
        subgrp_running_totals={}), v)


def mergeCombiners(
        lsub: SubTotal1,
        rsub: SubTotal1,
) -> SubTotal1:
    subgrp_running_totals = {}
    all_subgrp = set(lsub.subgrp_running_totals.keys() |
                     rsub.subgrp_running_totals.keys())
    for subgrp in all_subgrp:
        list_of_subgrp_running_totals = []
        if subgrp in lsub.subgrp_running_totals:
            list_of_subgrp_running_totals.append(
                lsub.subgrp_running_totals[subgrp])
        if subgrp in rsub.subgrp_running_totals:
            list_of_subgrp_running_totals.append(
                rsub.subgrp_running_totals[subgrp])
        if len(list_of_subgrp_running_totals) == 1:
            result = list_of_subgrp_running_totals[0]
        else:
            result = SubTotal2(
                running_sum_of_E_squared=sum(
                    x.running_sum_of_E_squared
                    for x in list_of_subgrp_running_totals),
                running_sum_of_E=sum(
                    x.running_sum_of_E
                    for x in list_of_subgrp_running_totals),
                running_count=sum(
                    x.running_count
                    for x in list_of_subgrp_running_totals))
        subgrp_running_totals[subgrp] = result
    return SubTotal1(
        running_sum_of_C=lsub.running_sum_of_C + rsub.running_sum_of_C,
        running_max_of_D=lsub.running_max_of_D
        if lsub.running_max_of_D is not None and
        lsub.running_max_of_D > rsub.running_max_of_D
        else rsub.running_max_of_D,
        subgrp_running_totals=subgrp_running_totals)


def finalAnalytics(
        grp: int,
        level1: SubTotal1,
) -> Row:
    import statistics
    running_grp_count = 0
    list_of_var_of_E = []
    for sub in level1.subgrp_running_totals.values():
        count = sub.running_count
        running_grp_count += count
        var_of_E = (
            sub.running_sum_of_E_squared / count
            - (sub.running_sum_of_E / count)**2)
        list_of_var_of_E.append(var_of_E)

    return Row(
        grp=grp,
        mean_of_C=math.nan
        if running_grp_count < 1 else
        level1.running_sum_of_C / running_grp_count,
        max_of_D=level1.running_max_of_D,
        avg_var_of_E=statistics.mean(list_of_var_of_E))
