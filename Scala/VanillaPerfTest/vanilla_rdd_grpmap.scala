package VanillaPerfTest

import org.apache.spark.rdd.{RDD}
import org.apache.spark.{SparkContext}
import org.apache.spark.sql.{SparkSession, DataFrame, Dataset}

object vanilla_rdd_grpmap {
  def run(
      dataSize: Long, 
      listData: IndexedSeq[DataPoint], 
      sc: SparkContext, spark: SparkSession): (
          RDD[AggResult], 
          DataFrame,
          Dataset[AggResult]) = {
    import spark.implicits._
    val rddData = sc.parallelize(listData, numSlices=8)
    def processData1(grp: Long, subgrp: Long, 
        iterator: Iterable[DataPoint]):AggResult = {
      var sum_of_C: Double = 0
      var count: Long = 0
      var max_of_D: Double = Double.NaN
      var sum_of_E_squared: Double = 0
      var sum_of_E: Double = 0
      for(item <- iterator) {
            sum_of_C += item.C
            max_of_D = if(count == 0) item.D else 
              if(max_of_D < item.D) item.D else max_of_D
            count = count + 1
            sum_of_E_squared += item.E * item.E
            sum_of_E += item.E
      }
      val mean_of_C = if(count > 0) sum_of_C / count 
        else Double.NaN
      val var_of_E = if (count < 2) Double.NaN else
        (
            sum_of_E_squared
            - sum_of_E * sum_of_E / count
        )  / (count - 1)
      return new AggResult(grp, subgrp, 
          mean_of_C, max_of_D, var_of_E, var_of_E)
    }

    val rddResult = rddData
        .groupBy(x => (x.grp, x.subgrp))
        .map( { case ((grp, subgrp),lst) => 
          processData1(grp, subgrp, lst) })
        .sortBy(x=>(x.grp, x.subgrp))
    return (rddResult, null, null)
  }
}