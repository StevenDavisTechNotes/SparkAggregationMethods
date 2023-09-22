from typing import Optional, Tuple

from pyspark import RDD
from pyspark.sql import DataFrame as spark_DataFrame

from SixFieldCommon.SixFieldTestData import DataSet, ExecutionParameters
from Utils.TidySparkSession import TidySparkSession


def vanilla_panda_cupy(
        _spark_session: TidySparkSession,
        _exec_params: ExecutionParameters,
        _data_set: DataSet
) -> Tuple[Optional[RDD], Optional[spark_DataFrame]]:
    # df = data_set.data.dfSrc
    # aggregates = (
    #     df
    #     .groupby(df.grp, df.subgrp)
    #     .applyInPandas(inner_agg_method, postAggSchema)
    #     .orderBy(df.grp, df.subgrp)
    # )
    # return None, aggregates
    raise NotImplementedError()


# def inner_agg_method(
#         dfPartition: pd.DataFrame,
# ) -> pd.DataFrame:
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
#         np.float(cupy.asnumpy(
#             (cupy.inner(E, E) / nE - (cupy.sum(E) / nE)**2))),
#     ]], columns=result_columns)
