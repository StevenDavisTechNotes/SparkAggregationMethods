import math
from typing import Iterable

from challenges.conditional.conditional_test_data_types import SubTotal
from six_field_test_data.six_generate_test_data import (
    DataSetPyspark, GrpTotal, TChallengePendingAnswerPythonPyspark)
from six_field_test_data.six_test_data_types import (DataPoint,
                                                     ExecutionParameters)
from utils.tidy_spark_session import TidySparkSession


def cond_pyspark_rdd_map_part(
        spark_session: TidySparkSession,
        exec_params: ExecutionParameters,
        data_set: DataSetPyspark,
) -> TChallengePendingAnswerPythonPyspark:

    rddSumCount = (
        data_set.data.rddSrc
        .mapPartitionsWithIndex(partition_triage)
        .groupByKey(
            numPartitions=data_set.description.NumGroups * data_set.description.NumSubGroups)
        .map(lambda kv: (kv[0], merge_combiners_3(kv[0], kv[1])))
        .map(lambda kv: (kv[0], final_analytics_2(kv[0], kv[1])))
        .sortByKey()  # type: ignore
        .values()
    )
    return rddSumCount


class MutableRunningTotal:
    running_sum_of_C: float
    running_uncond_count: int
    running_max_of_D: float | None
    running_cond_sum_of_E_squared: float
    running_cond_sum_of_E: float
    running_cond_count: int

    def __init__(self):
        self.running_sum_of_C = 0
        self.running_uncond_count = 0
        self.running_max_of_D = None
        self.running_cond_sum_of_E_squared = 0
        self.running_cond_sum_of_E = 0
        self.running_cond_count = 0


def partition_triage(
        _splitIndex: int,
        iterator: Iterable[DataPoint]
) -> Iterable[tuple[tuple[int, int], SubTotal]]:
    running_subtotals: dict[tuple[int, int], MutableRunningTotal] = dict()
    for v in iterator:
        k = (v.grp, v.subgrp)
        if k not in running_subtotals:
            running_subtotals[k] = MutableRunningTotal()
        sub = running_subtotals[k]
        sub.running_sum_of_C += v.C
        sub.running_uncond_count += 1
        sub.running_max_of_D = \
            sub.running_max_of_D \
            if sub.running_max_of_D is not None and \
            sub.running_max_of_D > v.D \
            else v.D
        if v.E < 0:
            sub.running_cond_sum_of_E_squared += v.E * v.E
            sub.running_cond_sum_of_E += v.E
            sub.running_cond_count += 1
    for k in running_subtotals:
        sub = running_subtotals[k]
        assert sub.running_max_of_D is not None
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
        assert rsub.running_max_of_D is not None
        lsub.running_max_of_D = lsub.running_max_of_D \
            if lsub.running_max_of_D is not None and \
            lsub.running_max_of_D > rsub.running_max_of_D \
            else rsub.running_max_of_D
        lsub.running_cond_sum_of_E_squared += \
            rsub.running_cond_sum_of_E_squared
        lsub.running_cond_sum_of_E += rsub.running_cond_sum_of_E
        lsub.running_cond_count += rsub.running_cond_count
    assert lsub.running_max_of_D is not None
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
