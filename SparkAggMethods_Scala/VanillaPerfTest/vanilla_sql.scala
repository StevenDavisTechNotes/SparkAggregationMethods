package VanillaPerfTest

import org.apache.spark.rdd.{RDD}
import org.apache.spark.{ SparkContext}
import org.apache.spark.sql.{SparkSession, DataFrame, Dataset}

object vanilla_sql {
  def run(
      dataSize: Long, 
      listData: IndexedSeq[DataPoint], 
      sc: SparkContext, spark: SparkSession): (
          RDD[AggResult], 
          DataFrame,
          Dataset[AggResult]) = {
import spark.implicits._
var df = spark.createDataFrame(listData)
spark.catalog.dropTempView("exampledata")
df.createTempView("exampledata")
var query: String = """
SELECT 
    grp, subgrp, AVG(C) mean_of_C, MAX(D) max_of_D, 
    VAR_SAMP(E) var_of_E,
    (
        SUM(E*E) - 
        SUM(E) * SUM(E) / COUNT(E)
    ) / (COUNT(E) - 1) var_of_E2
FROM
    exampledata
GROUP BY grp, subgrp
ORDER BY grp, subgrp
"""
var df2 = spark.sql(query).as[AggResult]
    return (null, null, df2)
  }
}