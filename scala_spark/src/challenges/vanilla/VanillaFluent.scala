package challenges.vanilla

import org.apache.spark.rdd.{RDD}
import org.apache.spark.{SparkContext}
import org.apache.spark.sql.{SparkSession, DataFrame, Dataset}
import org.apache.spark.sql.functions.{sum, mean, var_pop, count, pow, max}
import org.apache.spark.sql.expressions.scalalang.typed.{
  count => typedCount, 
  avg => typedMean,
  sum => typedSum}

object VanillaFluent {
  def run(
      dataSize: Long, 
      listData: IndexedSeq[DataPoint], 
      sc: SparkContext, spark: SparkSession): StrategyResult = {
    import spark.implicits._
    val ds = spark.createDataset(listData).repartition(8)
    val ds2 = ds
        .groupByKey(x => (x.grp, x.subgrp))
        .agg(
            typedMean[DataPoint](_.C).name("mean_of_C"),
            max($"D").as[Double].name("max_of_D"),
            var_pop($"E").as[Double].name("var_of_E"),
            ((
                sum($"E" * $"E") 
                - pow(sum($"E"),2)/count($"E")
            )/count($"E"))
            .as[Double].name("var_of_E2")
        )
        .map { case ((grp, subgrp), mean_of_C, max_of_D, var_of_E, var_of_E2) =>
          AggResult(grp, subgrp, mean_of_C, max_of_D, var_of_E, var_of_E2) }
        .orderBy($"grp", $"subgrp")
    DatasetResult(ds2)
  }
}