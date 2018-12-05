package VanillaPerfTest

import org.apache.spark.rdd.{RDD}
import org.apache.spark.{SparkContext}
import org.apache.spark.sql.{SparkSession, DataFrame, Dataset}

object vanilla_rdd_reduce {
  def run(
      dataSize: Long, 
      listData: IndexedSeq[DataPoint], 
      sc: SparkContext, spark: SparkSession): (
          RDD[AggResult], 
          DataFrame,
          Dataset[AggResult]) = {
    import spark.implicits._
    val rddData = sc.parallelize(listData, numSlices=8)
    
    final case class SubTotal(
      running_sum_of_C: Double, 
      running_count: Long,
      running_max_of_D: Double,
      running_sum_of_E_squared: Double, 
      running_sum_of_E: Double);
    
    def createCombiner(v:DataPoint): SubTotal = {
      SubTotal(
        running_sum_of_C=v.C, 
        running_count=1,
        running_max_of_D=v.D, 
        running_sum_of_E_squared=v.E*v.E, 
        running_sum_of_E=v.E)
    }
    def mergeValues(sub: SubTotal, v:DataPoint): SubTotal = {
      SubTotal(
          sub.running_sum_of_C + v.C,
          sub.running_count + 1, 
          math.max(sub.running_max_of_D, v.D),
          sub.running_sum_of_E_squared + v.E * v.E, 
          sub.running_sum_of_E + v.E)
    }
    def mergeCombiners(lsub:SubTotal, rsub:SubTotal): SubTotal = {
      SubTotal(
        lsub.running_sum_of_C + rsub.running_sum_of_C, 
        lsub.running_count + rsub.running_count, 
        math.max(lsub.running_max_of_D, rsub.running_max_of_D), 
        lsub.running_sum_of_E_squared 
        + rsub.running_sum_of_E_squared, 
        lsub.running_sum_of_E + rsub.running_sum_of_E)
    }
    def finalAnalytics(key: (Long, Long), total:SubTotal): AggResult = {
        val sum_of_C = total.running_sum_of_C
        val count = total.running_count
        val max_of_D = total.running_max_of_D
        val sum_of_E_squared = total.running_sum_of_E_squared
        val sum_of_E = total.running_sum_of_E
        val var_of_E = 
              if (count < 2) Double.NaN else
                (
                    sum_of_E_squared - 
                    sum_of_E * sum_of_E / count
                )  / (count - 1)
        AggResult(
            grp=key._1, subgrp=key._2,
            mean_of_C= 
              if (count < 1) Double.NaN 
              else sum_of_C / count,
            max_of_D=max_of_D, 
            var_of_E = var_of_E,
            var_of_E2 = var_of_E)
    }
    val rddResult = rddData
        .map(x => ((x.grp, x.subgrp), x))
        .combineByKey(
            createCombiner,
            mergeValues,
            mergeCombiners)
        .map(kv => finalAnalytics(kv._1, kv._2))
        .sortBy(x=>(x.grp, x.subgrp))
    return (rddResult, null, null)
  }
}
