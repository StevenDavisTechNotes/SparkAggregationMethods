import math
from typing import NamedTuple

from pyspark.sql import Row

from src.six_field_test_data.six_generate_test_data import (
    DataSetPyspark, TChallengePendingAnswerPythonPyspark)
from src.six_field_test_data.six_generate_test_data.six_test_data_for_pyspark import \
    pick_agg_tgt_num_partitions_pyspark
from src.six_field_test_data.six_test_data_types import (Challenge, DataPoint,
                                                         ExecutionParameters)
from src.utils.tidy_spark_session import TidySparkSession

CHALLENGE = Challenge.BI_LEVEL


class SubTotal2(NamedTuple):
    running_sum_of_E_squared: float
    running_sum_of_E: float
    running_count: int


class SubTotal1(NamedTuple):
    running_sum_of_C: float
    running_max_of_D: float
    subgrp_running_totals: dict[int, SubTotal2]


def bi_level_pyspark_rdd_reduce_1(
        spark_session: TidySparkSession,
        exec_params: ExecutionParameters,
        data_set: DataSetPyspark
) -> TChallengePendingAnswerPythonPyspark:
    rddSrc = data_set.data.rdd_src
    agg_tgt_num_partitions = pick_agg_tgt_num_partitions_pyspark(data_set.data, CHALLENGE)

    rddResult = (
        rddSrc
        .map(lambda x: (x.grp, x))
        .combineByKey(create_combiner,
                      merge_value,
                      merge_combiners)
        .sortByKey(numPartitions=agg_tgt_num_partitions)  # type: ignore
        .map(lambda pair: final_analytics(pair[0], pair[1]))
    )
    return rddResult


def merge_value(
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
    sub_sub = subgrp_running_totals[v.subgrp]
    subgrp_running_totals[v.subgrp] = SubTotal2(
        sub_sub.running_sum_of_E_squared + v.E * v.E,
        sub_sub.running_sum_of_E + v.E,
        sub_sub.running_count + 1)
    return SubTotal1(
        running_sum_of_C=pre.running_sum_of_C + v.C,
        running_max_of_D=(pre.running_max_of_D
                          if not math.isnan(pre.running_max_of_D) and
                          pre.running_max_of_D > v.D
                          else v.D),
        subgrp_running_totals=subgrp_running_totals)


def create_combiner(
        v: DataPoint,
) -> SubTotal1:
    return merge_value(SubTotal1(
        running_sum_of_C=0,
        running_max_of_D=math.nan,
        subgrp_running_totals=dict()), v)


def merge_combiners(
        lsub: SubTotal1,
        rsub: SubTotal1,
) -> SubTotal1:
    subgrp_running_totals = {}
    all_subgrp = set(lsub.subgrp_running_totals.keys() |
                     rsub.subgrp_running_totals.keys())
    for subgrp in all_subgrp:
        list_of_subgrp_running_totals: list[SubTotal2] = []
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
    assert not math.isnan(rsub.running_max_of_D)
    return SubTotal1(
        running_sum_of_C=lsub.running_sum_of_C + rsub.running_sum_of_C,
        running_max_of_D=(lsub.running_max_of_D
                          if not math.isnan(lsub.running_max_of_D) and
                          lsub.running_max_of_D > rsub.running_max_of_D
                          else rsub.running_max_of_D),
        subgrp_running_totals=subgrp_running_totals)


def final_analytics(
        grp: int,
        level1: SubTotal1,
) -> Row:
    import statistics
    running_grp_count = 0
    list_of_var_of_E: list[float] = []
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
