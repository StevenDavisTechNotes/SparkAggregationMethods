from typing import Tuple

from pyspark import RDD
from pyspark.sql import DataFrame as spark_DataFrame

from SixFieldCommon.SixFieldTestData import (MAX_DATA_POINTS_PER_PARTITION,
                                             DataSet, ExecutionParameters)
from Utils.SparkUtils import TidySparkSession

from ..CondDataTypes import GrpTotal


def cond_rdd_grpmap(
    spark_session: TidySparkSession,
    _exec_params: ExecutionParameters,
    data_set: DataSet,
) -> Tuple[RDD[GrpTotal] | None, spark_DataFrame | None]:

    def processData1(key, iterator):
        import math
        sum_of_C = 0
        unconditional_count = 0
        max_of_D = None
        cond_sum_of_E_squared = 0
        cond_sum_of_E = 0
        cond_count_of_E = 0
        for item in iterator:
            sum_of_C = sum_of_C + item.C
            unconditional_count = unconditional_count + 1
            max_of_D = item.D \
                if max_of_D is None or max_of_D < item.D \
                else max_of_D
            if item.E < 0:
                cond_sum_of_E_squared = \
                    cond_sum_of_E_squared + item.E * item.E
                cond_sum_of_E = cond_sum_of_E + item.E
                cond_count_of_E = cond_count_of_E + 1
        mean_of_C = sum_of_C / unconditional_count \
            if unconditional_count > 0 else math.nan
        cond_var_of_E = (
            cond_sum_of_E_squared / cond_count_of_E
            - (cond_sum_of_E / cond_count_of_E)**2)
        return (key,
                GrpTotal(
                    grp=key[0],
                    subgrp=key[1],
                    mean_of_C=mean_of_C,
                    max_of_D=max_of_D,
                    cond_var_of_E=cond_var_of_E))

    if (
            data_set.NumDataPoints
            > MAX_DATA_POINTS_PER_PARTITION
            * data_set.NumGroups * data_set.NumSubGroups
    ):
        raise ValueError(
            "This strategy only works if all of the values per key can fit into memory at once.")

    rddResult = (
        data_set.rddSrc
        .groupBy(lambda x: (x.grp, x.subgrp))
        .map(lambda pair: processData1(pair[0], pair[1]))
        .sortByKey()  # type: ignore
        .values()
    )
    return rddResult, None
