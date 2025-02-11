import math
from typing import Iterable, NamedTuple

from pyspark import RDD
from pyspark.sql import Row
from spark_agg_methods_common_python.challenges.six_field_test_data.six_test_data_types import (
    Challenge, DataPointNT, SixTestExecutionParameters,
)

from src.challenges.six_field_test_data.six_test_data_for_pyspark import (
    SixFieldDataSetPyspark, TSixFieldChallengePendingAnswerPythonPyspark,
    pick_agg_tgt_num_partitions_pyspark,
)
from src.utils.tidy_session_pyspark import TidySparkSession

CHALLENGE = Challenge.BI_LEVEL


class SubTotal(NamedTuple):
    running_count: int
    running_sum_of_C: float
    running_max_of_D: float
    running_sum_of_E_squared: float
    running_sum_of_E: float


def bi_level_pyspark_rdd_reduce_2(
        spark_session: TidySparkSession,
        exec_params: SixTestExecutionParameters,
        data_set: SixFieldDataSetPyspark
) -> TSixFieldChallengePendingAnswerPythonPyspark:
    rdd_src: RDD[Row] = data_set.data.open_source_data_as_rdd(spark_session)
    agg_tgt_num_partitions = pick_agg_tgt_num_partitions_pyspark(data_set.data, CHALLENGE)

    rdd_result = (
        rdd_src
        .map(lambda r: DataPointNT(*r))
        .map(lambda x: ((x.grp, x.subgrp), x))
        .combineByKey(create_combiner,
                      merge_value,
                      merge_combiners)
        .map(lambda x: (x[0][0], x[1]))
        .groupByKey(numPartitions=data_set.data_description.num_grp_1 * data_set.data_description.num_grp_2)
        .map(lambda x: (x[0], final_analytics(x[0], x[1])), preservesPartitioning=True)
        .sortByKey(numPartitions=agg_tgt_num_partitions)  # type: ignore
        .values()
    )
    return rdd_result


def merge_value(
        pre: SubTotal,
        v: DataPointNT,
) -> SubTotal:
    return SubTotal(
        running_sum_of_C=pre.running_sum_of_C + v.C,
        running_count=pre.running_count + 1,
        running_max_of_D=(pre.running_max_of_D
                          if not math.isnan(pre.running_max_of_D) and
                          pre.running_max_of_D > v.D
                          else v.D),
        running_sum_of_E_squared=pre.running_sum_of_E_squared +
        v.E * v.E,
        running_sum_of_E=pre.running_sum_of_E + v.E)


def create_combiner(
        v: DataPointNT,
) -> SubTotal:
    return merge_value(SubTotal(
        running_sum_of_C=0,
        running_count=0,
        running_max_of_D=math.nan,
        running_sum_of_E_squared=0,
        running_sum_of_E=0), v)


def merge_combiners(
        lsub: SubTotal,
        rsub: SubTotal,
) -> SubTotal:
    assert not math.isnan(rsub.running_max_of_D)
    return SubTotal(
        running_sum_of_C=lsub.running_sum_of_C + rsub.running_sum_of_C,
        running_count=lsub.running_count + rsub.running_count,
        running_max_of_D=(lsub.running_max_of_D
                          if not math.isnan(lsub.running_max_of_D) and
                          lsub.running_max_of_D > rsub.running_max_of_D
                          else rsub.running_max_of_D),
        running_sum_of_E_squared=lsub.running_sum_of_E_squared +
        rsub.running_sum_of_E_squared,
        running_sum_of_E=lsub.running_sum_of_E + rsub.running_sum_of_E)


def final_analytics(
        grp: int,
        iterator: Iterable[SubTotal]
) -> Row:
    running_sum_of_C = 0
    running_grp_count = 0
    running_max_of_D = math.nan
    running_sum_of_var_of_E = 0
    running_count_of_subgrp = 0

    for sub in iterator:
        assert not math.isnan(sub.running_max_of_D)
        count = sub.running_count
        running_sum_of_C += sub.running_sum_of_C
        running_grp_count += count
        running_max_of_D = (sub.running_max_of_D
                            if not math.isnan(running_max_of_D) or
                            running_max_of_D < sub.running_max_of_D
                            else running_max_of_D)
        var_of_E = (
            sub.running_sum_of_E_squared / count
            - (sub.running_sum_of_E / count)**2
        )
        running_sum_of_var_of_E += var_of_E
        running_count_of_subgrp += 1

    return Row(
        grp=grp,
        mean_of_C=(
            math.nan
            if running_grp_count < 1 else
            running_sum_of_C / running_grp_count),
        max_of_D=running_max_of_D,
        avg_var_of_E=running_sum_of_var_of_E / running_count_of_subgrp)
