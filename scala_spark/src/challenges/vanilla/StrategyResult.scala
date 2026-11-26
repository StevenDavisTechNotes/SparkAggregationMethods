package challenges.vanilla

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ Dataset, SparkSession }

/** What a strategy hands back: lazy, not yet collected, so the runner can time the collection. */
sealed trait StrategyResult {
  def collect(spark: SparkSession): Array[AggResult]
}

final case class RddResult(rdd: RDD[AggResult]) extends StrategyResult {
  override def collect(spark: SparkSession): Array[AggResult] = rdd.collect()
}

final case class DatasetResult(ds: Dataset[AggResult]) extends StrategyResult {
  override def collect(spark: SparkSession): Array[AggResult] = ds.collect()
}
