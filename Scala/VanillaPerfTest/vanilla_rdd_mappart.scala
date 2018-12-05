package VanillaPerfTest

import org.apache.spark.rdd.{RDD}
import org.apache.spark.{SparkContext}
import org.apache.spark.sql.{SparkSession, DataFrame, Dataset}

object vanilla_rdd_mappart {
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
      running_max_of_D: Option[Double],
      running_sum_of_E_squared: Double, 
      running_sum_of_E: Double);
    class RunningTotal() {
      var running_sum_of_C: Double = 0
      var running_count: Long = 0
      var running_max_of_D: Option[Double] = None
      var running_sum_of_E_squared: Double = 0
      var running_sum_of_E: Double = 0
      def Add(v: DataPoint) = {
        running_sum_of_C += v.C
        running_count += 1
        running_max_of_D = Some(
          if (running_max_of_D.isDefined) 
            math.max(running_max_of_D.get, v.D)
          else v.D)
        running_sum_of_E_squared += v.E * v.E
        running_sum_of_E += v.E
      }
      def Add(sub: SubTotal) = {
        running_sum_of_C += sub.running_sum_of_C
        running_count += sub.running_count
        running_max_of_D =
          if (running_max_of_D.isDefined) (
            if(sub.running_max_of_D.isDefined) 
              Some(math.max(running_max_of_D.get, 
                  sub.running_max_of_D.get))
            else
              running_max_of_D
          ) else sub.running_max_of_D
        running_sum_of_E_squared += sub.running_sum_of_E_squared
        running_sum_of_E += sub.running_sum_of_E
      }
      def ToSubTotal(): SubTotal = {
        SubTotal(
          running_sum_of_C, 
          running_count,
          running_max_of_D,
          running_sum_of_E_squared, 
          running_sum_of_E)
      }
    }
    def partitionTriage(iterator: Iterator[DataPoint]): 
      Iterator[((Long, Long), SubTotal)] = {
        var running_subtotals = 
          scala.collection.mutable.Map[(Long,Long),RunningTotal]()
        for(v <- iterator) {
            val k = (v.grp, v.subgrp)
            if (!(running_subtotals contains k)) {
              running_subtotals.update(k, new RunningTotal())
            }
            running_subtotals(k).Add(v)
        }
        (running_subtotals.map {
          case (k, sub) => (k, sub.ToSubTotal())
        }).toIterator
    }
    def mergeCombiners(iterable: Iterable[SubTotal]):
      SubTotal = {
        var runningTotal = new RunningTotal()
        for (rsub <- iterable) {
          runningTotal.Add(rsub)
        }
      runningTotal.ToSubTotal()
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
        max_of_D= if(max_of_D.isDefined) max_of_D.get else Double.NaN, 
        var_of_E = var_of_E,
        var_of_E2 = var_of_E)
    }

    val rddResult = rddData
        .mapPartitions(partitionTriage)
        .groupByKey()
        .map {
          case (k,v) => (k, mergeCombiners(v))
        }
        .map {
          case (k,v) => finalAnalytics(k, v)
        }
        .sortBy(x=>(x.grp, x.subgrp))
    return (rddResult, null, null)
  }
}