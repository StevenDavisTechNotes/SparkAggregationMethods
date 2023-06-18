from typing import List, Tuple, Optional, Iterable

from pyspark import RDD, Row
from pyspark.sql import SparkSession, DataFrame as spark_DataFrame

from Utils.SparkUtils import cast_no_arg_sort_by_key
from .VanillaTestData import DataPoint


def vanilla_rdd_grpmap(
    spark: SparkSession, pyData: List[DataPoint]
) -> Tuple[Optional[RDD], Optional[spark_DataFrame]]:
    sc = spark.sparkContext
    rddData = sc.parallelize(pyData)

    def processData1(key, iterator) -> Tuple[Tuple[int, int], Row]:
        import math
        sum_of_C = 0
        count = 0
        max_of_D = None
        sum_of_E_squared = 0
        sum_of_E = 0
        for item in iterator:
            sum_of_C = sum_of_C + item.C
            count = count + 1
            max_of_D = item.D \
                if max_of_D is None or max_of_D < item.D \
                else max_of_D
            sum_of_E_squared += item.E * item.E
            sum_of_E += item.E
        mean_of_C = sum_of_C / count \
            if count > 0 else math.nan
        var_of_E = math.nan
        if count >= 2:
            var_of_E = \
                (
                    sum_of_E_squared
                    - sum_of_E * sum_of_E / count
                ) / (count - 1)
        return (key,
                Row(grp=key[0],
                    subgrp=key[1],
                    mean_of_C=mean_of_C,
                    max_of_D=max_of_D,
                    var_of_E=var_of_E))

    rddProcessed: RDD[Tuple[Tuple[int, int], Iterable[Row]]] = (
        rddData
        .groupBy(lambda x: (x.grp, x.subgrp))
        .map(lambda pair: processData1(pair[0], pair[1]))
    )
    rddResult = (
        cast_no_arg_sort_by_key(
            rddProcessed
            .coalesce(numPartitions=1))
        .sortByKey()
        .values()
    )
    return rddResult, None
