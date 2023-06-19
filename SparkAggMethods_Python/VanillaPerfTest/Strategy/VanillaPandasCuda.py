from typing import List, Tuple, Optional

from pyspark import RDD
from pyspark.sql import DataFrame as spark_DataFrame

from Utils.SparkUtils import TidySparkSession

from ..VanillaTestData import DataPoint


def vanilla_panda_cupy(
    _spark_session: TidySparkSession, _pyData: List[DataPoint]
) -> Tuple[Optional[RDD], Optional[spark_DataFrame]]:

    # def inner_agg_method(dfPartition):
    #     group_key = dfPartition['grp'].iloc[0]
    #     subgroup_key = dfPartition['subgrp'].iloc[0]
    #     C = cupy.asarray(dfPartition['C'])
    #     D = cupy.asarray(dfPartition['D'])
    #     pdE = dfPartition['E']
    #     E = cupy.asarray(pdE)
    #     nE = pdE.count()
    #     return pd.DataFrame([[
    #         group_key,
    #         subgroup_key,
    #         np.float(cupy.asnumpy(cupy.mean(C))),
    #         np.float(cupy.asnumpy(cupy.max(D))),
    #         np.float(cupy.asnumpy(cupy.var(E))),
    #         np.float(cupy.asnumpy((cupy.inner(E, E) - cupy.sum(E)**2/nE)/(nE-1))),
    #     ]], columns=groupby_columns + agg_columns)

    # df = spark_session.spark.createDataFrame(
    #     map(lambda x: astuple(x), pyData), schema=DataPointSchema)
    # aggregates = (
    #     cast_from_pd_dataframe(
    #         df.groupby(df.grp, df.subgrp)
    #     ).applyInPandas(inner_agg_method, postAggSchema))
    # return None, aggregates
    raise NotImplementedError()
