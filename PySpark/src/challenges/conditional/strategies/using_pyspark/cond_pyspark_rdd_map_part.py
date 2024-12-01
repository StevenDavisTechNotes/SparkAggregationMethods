import math
from typing import Iterable

from pyspark.sql import Row
from spark_agg_methods_common_python.challenges.conditional.conditional_test_data_types import SubTotal
from spark_agg_methods_common_python.challenges.six_field_test_data.six_test_data_types import (
    Challenge, DataPointNT, SixTestExecutionParameters,
)

from src.challenges.six_field_test_data.six_test_data_for_pyspark import (
    SixFieldDataSetPyspark, TSixFieldChallengePendingAnswerPythonPyspark, pick_agg_tgt_num_partitions_pyspark,
)
from src.utils.tidy_session_pyspark import TidySparkSession

CHALLENGE = Challenge.CONDITIONAL


def cond_pyspark_rdd_map_part(
        spark_session: TidySparkSession,
        exec_params: SixTestExecutionParameters,
        data_set: SixFieldDataSetPyspark,
) -> TSixFieldChallengePendingAnswerPythonPyspark:
    if (data_set.data_description.num_source_rows >= 9*10**8):
        return "infeasible", "Too slow"
    agg_tgt_num_partitions = pick_agg_tgt_num_partitions_pyspark(data_set.data, CHALLENGE)
    rdd_src = data_set.data.open_source_data_as_rdd(spark_session)
    rdd_sum_count = (
        rdd_src
        .map(lambda r: DataPointNT(*r))
        .mapPartitionsWithIndex(partition_triage)
        .groupByKey(
            numPartitions=data_set.data_description.num_grp_1 * data_set.data_description.num_grp_2)
        .map(lambda kv: (kv[0], merge_combiners_3(kv[0], kv[1])), preservesPartitioning=True)
        .map(lambda kv: (kv[0], final_analytics_2(kv[0], kv[1])), preservesPartitioning=True)
        .sortByKey(numPartitions=agg_tgt_num_partitions)  # type: ignore
        .values()
    )
    return rdd_sum_count


class MutableRunningTotal:
    running_sum_of_C: float
    running_uncond_count: int
    running_max_of_D: float
    running_cond_sum_of_E_squared: float
    running_cond_sum_of_E: float
    running_cond_count: int

    def __init__(self):
        self.running_sum_of_C = 0
        self.running_uncond_count = 0
        self.running_max_of_D = math.nan
        self.running_cond_sum_of_E_squared = 0
        self.running_cond_sum_of_E = 0
        self.running_cond_count = 0


def partition_triage(
        _splitIndex: int,
        iterator: Iterable[DataPointNT]
) -> Iterable[tuple[tuple[int, int], SubTotal]]:
    running_subtotals: dict[tuple[int, int], MutableRunningTotal] = dict()
    for v in iterator:
        k = (v.grp, v.subgrp)
        if k not in running_subtotals:
            running_subtotals[k] = MutableRunningTotal()
        sub = running_subtotals[k]
        sub.running_sum_of_C += v.C
        sub.running_uncond_count += 1
        sub.running_max_of_D = (
            sub.running_max_of_D
            if not math.isnan(sub.running_max_of_D) and
            sub.running_max_of_D > v.D
            else v.D)
        if v.E < 0:
            sub.running_cond_sum_of_E_squared += v.E * v.E
            sub.running_cond_sum_of_E += v.E
            sub.running_cond_count += 1
    for k in running_subtotals:
        sub = running_subtotals[k]
        assert not math.isnan(sub.running_max_of_D)
        yield (k, SubTotal(
            running_sum_of_C=sub.running_sum_of_C,
            running_uncond_count=sub.running_uncond_count,
            running_max_of_D=sub.running_max_of_D,
            running_cond_sum_of_E_squared=sub.running_cond_sum_of_E_squared,
            running_cond_sum_of_E=sub.running_cond_sum_of_E,
            running_cond_count=sub.running_cond_count))


def merge_combiners_3(
        _key: tuple[int, int],
        iterable: Iterable[SubTotal],
) -> SubTotal:
    lsub = MutableRunningTotal()
    for rsub in iterable:
        lsub.running_sum_of_C += rsub.running_sum_of_C
        lsub.running_uncond_count += rsub.running_uncond_count
        assert not math.isnan(rsub.running_max_of_D)
        lsub.running_max_of_D = (lsub.running_max_of_D
                                 if not math.isnan(lsub.running_max_of_D) and
                                 lsub.running_max_of_D > rsub.running_max_of_D
                                 else rsub.running_max_of_D)
        lsub.running_cond_sum_of_E_squared += \
            rsub.running_cond_sum_of_E_squared
        lsub.running_cond_sum_of_E += rsub.running_cond_sum_of_E
        lsub.running_cond_count += rsub.running_cond_count
    assert not math.isnan(lsub.running_max_of_D)
    return SubTotal(
        running_sum_of_C=lsub.running_sum_of_C,
        running_uncond_count=lsub.running_uncond_count,
        running_max_of_D=lsub.running_max_of_D,
        running_cond_sum_of_E_squared=lsub.running_cond_sum_of_E_squared,
        running_cond_sum_of_E=lsub.running_cond_sum_of_E,
        running_cond_count=lsub.running_cond_count)


def final_analytics_2(
        key: tuple[int, int],
        total: SubTotal,
) -> Row:
    sum_of_C = total.running_sum_of_C
    uncond_count = total.running_uncond_count
    max_of_D = total.running_max_of_D
    cond_sum_of_E_squared = total.running_cond_sum_of_E_squared
    cond_sum_of_E = total.running_cond_sum_of_E
    cond_count = total.running_cond_count
    return Row(
        grp=key[0], subgrp=key[1],
        mean_of_C=math.nan
        if cond_count < 1 else
        sum_of_C / uncond_count,
        max_of_D=max_of_D,
        cond_var_of_E=(
            cond_sum_of_E_squared / cond_count
            - (cond_sum_of_E / cond_count)**2
            if cond_count > 0 else math.nan)
    )
