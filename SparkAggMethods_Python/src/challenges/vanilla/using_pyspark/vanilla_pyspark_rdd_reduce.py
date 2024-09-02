from typing import NamedTuple, Optional, cast

from pyspark import RDD
from pyspark.sql import Row

from src.six_field_test_data.six_generate_test_data import (
    DataSetPyspark, TChallengePendingAnswerPythonPyspark)
from src.six_field_test_data.six_generate_test_data.six_test_data_for_pyspark import \
    pick_agg_tgt_num_partitions_pyspark
from src.six_field_test_data.six_test_data_types import (Challenge,
                                                         DataPointNT,
                                                         ExecutionParameters)
from src.utils.tidy_spark_session import TidySparkSession

CHALLENGE = Challenge.VANILLA


class SubTotalDC(NamedTuple):
    running_sum_of_C: float
    running_count: int
    running_max_of_D: Optional[float]
    running_sum_of_E_squared: float
    running_sum_of_E: float


def vanilla_pyspark_rdd_reduce(
        spark_session: TidySparkSession,
        exec_params: ExecutionParameters,
        data_set: DataSetPyspark
) -> TChallengePendingAnswerPythonPyspark:
    agg_tgt_num_partitions = pick_agg_tgt_num_partitions_pyspark(data_set.data, CHALLENGE)
    sumCount: RDD[Row] = (
        data_set.data.rdd_src
        .map(lambda x: ((x.grp, x.subgrp), x))
        .combineByKey(create_combiner_2,
                      merge_value_2,
                      merge_combiners_2)
        .map(lambda kv: final_analytics_2(kv[0], kv[1])))
    rddResult = sumCount.sortBy(
        keyfunc=lambda x: cast(tuple[int, int], (x.grp, x.subgrp)),  # type: ignore
        numPartitions=agg_tgt_num_partitions)
    return rddResult


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


def create_accumulator() -> SubTotalDC:
    return SubTotalDC(
        running_sum_of_C=0,
        running_count=0,
        running_max_of_D=None,
        running_sum_of_E_squared=0,
        running_sum_of_E=0)


def merge_value_2(
        sub: SubTotalDC,
        v: DataPointNT,
) -> SubTotalDC:
    running_sum_of_E_squared = sub.running_sum_of_E_squared + v.E * v.E
    running_sum_of_E = sub.running_sum_of_E + v.E
    return SubTotalDC(
        running_sum_of_C=sub.running_sum_of_C + v.C,
        running_count=sub.running_count + 1,
        running_max_of_D=max(sub.running_max_of_D, v.D),
        running_sum_of_E_squared=running_sum_of_E_squared,
        running_sum_of_E=running_sum_of_E)


def create_combiner_2(
        v: DataPointNT,
) -> SubTotalDC:
    return merge_value_2(create_accumulator(), v)


def merge_combiners_2(
        lsub: SubTotalDC,
        rsub: SubTotalDC,
) -> SubTotalDC:
    return SubTotalDC(
        running_sum_of_C=lsub.running_sum_of_C + rsub.running_sum_of_C,
        running_count=lsub.running_count + rsub.running_count,
        running_max_of_D=max(lsub.running_max_of_D, rsub.running_max_of_D),
        running_sum_of_E_squared=lsub.running_sum_of_E_squared +
        rsub.running_sum_of_E_squared,
        running_sum_of_E=lsub.running_sum_of_E + rsub.running_sum_of_E
    )


def final_analytics_2(
        key: tuple[int, int],
        total: SubTotalDC,
) -> Row:
    sum_of_C = total.running_sum_of_C
    count = total.running_count
    max_of_D = total.running_max_of_D
    sum_of_E_squared = total.running_sum_of_E_squared
    sum_of_E = total.running_sum_of_E
    return Row(
        grp=key[0], subgrp=key[1],
        mean_of_C=sum_of_C / count,
        max_of_D=max_of_D,
        var_of_E=(
            sum_of_E_squared / count
            - (sum_of_E / count) ** 2)
    )
