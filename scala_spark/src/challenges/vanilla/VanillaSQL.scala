package challenges.vanilla

import org.apache.spark.rdd.{RDD}
import org.apache.spark.{ SparkContext}
import org.apache.spark.sql.{SparkSession, DataFrame, Dataset}

object VanillaSQL {
  def run(
      dataSize: Long, 
      listData: IndexedSeq[DataPoint], 
      sc: SparkContext, spark: SparkSession): StrategyResult = {
import spark.implicits._
val df = spark.createDataFrame(listData)
spark.catalog.dropTempView("exampledata")
df.createTempView("exampledata")
val query: String = """
SELECT 
    grp, subgrp, AVG(C) mean_of_C, MAX(D) max_of_D, 
    VAR_POP(E) var_of_E,
    (
        SUM(E*E) - 
        SUM(E) * SUM(E) / COUNT(E)
    ) / COUNT(E) var_of_E2
FROM
    exampledata
GROUP BY grp, subgrp
ORDER BY grp, subgrp
"""
val df2 = spark.sql(query).as[AggResult]
    DatasetResult(df2)
  }
}