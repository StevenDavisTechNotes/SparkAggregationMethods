# def vanilla_panda_cupy(
#     spark: SparkSession, pyData: List[DataPoint]
# ) -> Tuple[Optional[RDD], Optional[spark_DataFrame]]:
#     df = spark.createDataFrame(
#         cast_data_points_to_tuples(pyData), 
#         schema=DataPointSchema)

#     groupby_columns = ['grp', 'subgrp']
#     agg_columns = ['mean_of_C','max_of_D', 'var_of_E', 'var_of_E2']
#     postAggSchema = DataTypes.StructType(
#         [x for x in DataPointSchema.fields if x.name in groupby_columns] +
#         [DataTypes.StructField(name, DataTypes.DoubleType(), False) for name in agg_columns])

#     def inner_agg_method(dfPartition):
#         group_key = dfPartition['grp'].iloc[0]
#         subgroup_key = dfPartition['subgrp'].iloc[0]
#         C = cupy.asarray(dfPartition['C'])
#         D = cupy.asarray(dfPartition['D'])
#         pdE = dfPartition['E']
#         E = cupy.asarray(pdE)
#         nE = pdE.count()
#         return pd.DataFrame([[
#             group_key,
#             subgroup_key,
#             np.float(cupy.asnumpy(cupy.mean(C))),
#             np.float(cupy.asnumpy(cupy.max(D))),
#             np.float(cupy.asnumpy(cupy.var(E))),
#             np.float(cupy.asnumpy((cupy.inner(E,E) - cupy.sum(E)**2/nE)/(nE-1))),
#             ]], columns=groupby_columns + agg_columns)

#     aggregates = cast_from_pd_dataframe(
#         df.groupby(df.grp, df.subgrp)
#        ).applyInPandas(inner_agg_method)
#     return None, aggregates
