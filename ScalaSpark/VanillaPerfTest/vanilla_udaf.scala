package VanillaPerfTest

import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.types._

import org.apache.spark.rdd.{RDD}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SparkSession, Row, DataFrame, Dataset}
import org.apache.spark.sql.functions.{sum, mean, variance, count, pow, max}
import org.apache.spark.sql.expressions.scalalang.typed.{
  count => typedCount, 
  avg => typedMean,
  sum => typedSum}

class MyVarianceUDAF extends UserDefinedAggregateFunction {
  // This is the input fields for your aggregate function.
  override def inputSchema: org.apache.spark.sql.types.StructType =
    StructType(StructField("value", DoubleType) :: Nil)

  // This is the internal fields you keep for computing your aggregate.
  override def bufferSchema: StructType = StructType(
    StructField("count", LongType) ::
    StructField("sumE", DoubleType) :: 
    StructField("sumE2", DoubleType) :: Nil
  )

  // This is the output type of your aggregation function.
  override def dataType: DataType = DoubleType

  override def deterministic: Boolean = true

  // This is the initial value for your buffer schema.
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(1) = 0.0
    buffer(2) = 0.0
  }

  // This is how to update your buffer schema given an input.
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getAs[Long](0) + 1
    val value = input.getAs[Double](0)
    buffer(1) = buffer.getAs[Double](1) + value 
    buffer(2) = buffer.getAs[Double](2) + value * value 
  }

  // This is how to merge two objects with the bufferSchema type.
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getAs[Long](0) + buffer2.getAs[Long](0)
    buffer1(1) = buffer1.getAs[Double](1) + buffer2.getAs[Double](1)
    buffer1(2) = buffer1.getAs[Double](2) + buffer2.getAs[Double](2)
  }

  // This is where you output the final value, given the final value of your bufferSchema.
  override def evaluate(buffer: Row): Any = {
    val count = buffer.getAs[Long](0)
    val sumE = buffer.getAs[Double](1)
    val sumE2 = buffer.getAs[Double](2)
    (sumE2 - sumE * sumE / count)/(count-1)
  }
}

object vanilla_udaf {
  def run(
      dataSize: Long, 
      listData: IndexedSeq[DataPoint], 
      sc: SparkContext, spark: SparkSession): (
          RDD[AggResult], 
          DataFrame,
          Dataset[AggResult]) = {
    import spark.implicits._
    var ds = spark.createDataset(listData)
    var myVarianceUDAF = new MyVarianceUDAF
    var ds2 = ds
        .groupByKey(x => (x.grp, x.subgrp))
        .agg(
            typedMean[DataPoint](_.C).name("mean_of_C"),
            max($"D").as[Double].name("max_of_D"),
            variance($"E").as[Double].name("var_of_E"),
            myVarianceUDAF($"E").as[Double].name("var_of_E2")
        )
        .map { case ((grp, subgrp), mean_of_C, max_of_D, var_of_E, var_of_E2) =>
          AggResult(grp, subgrp, mean_of_C, max_of_D, var_of_E, var_of_E2) }
        .orderBy($"grp", $"subgrp")
    return (null, null, ds2)
  }
}