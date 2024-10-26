package VanillaPerfTest

import java.io._
import scala.collection.immutable.IndexedSeq

import org.apache.log4j.Logger
import org.apache.spark.rdd.{ RDD }
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.{ SparkSession, Row, DataFrame, Dataset, Encoder }

import Common.FileHelper
import Common.MathHelper

object LogHolder extends Serializable {
  @transient lazy val log = Logger.getLogger(getClass.getName)
}

object Runner {
  def createSparkSession(): SparkSession = {
    val ss = SparkSession
      .builder
      .master("local[*]")
      .appName("CondPerfTest")
      .config("spark.sql.shuffle.partitions", 8)
      .config("spark.ui.enabled", "false")
      .config("spark.rdd.compress", "false")
//      .config("spark.driver.memory", "20g") -Xms2g -Xmx20g
      .config("spark.shuffle.io.maxRetries", "0")
      .config("spark.port.maxRetries", "0")
      .config("spark.rpc.numRetries", "0")
      .config("spark.task.maxFailures", "1")
      .config("spark.sql.execution.arrow.enabled", "true")
      .config("spark.local.dir", "E:/Temp/SparkTemp")
      .enableHiveSupport()
      .getOrCreate()
    LogHolder.log.info("script initialized")
    ss
  }
  final case class ComputationMethod(
    name:      String,
    interface: String,
    delegate:  (Long, IndexedSeq[DataPoint], SparkContext, SparkSession) => (RDD[AggResult], DataFrame, Dataset[AggResult]))
  final case class ItineraryStop(
    compMethod: ComputationMethod,
    data:       IndexedSeq[DataPoint])
  final case class RunResult(
      dataSize: Long, 
      elapsedTime: Double, 
      recordCount: Long)

  def main(args: Array[String]) = {
    val ss = createSparkSession()
    val sc = ss.sparkContext
    import ss.implicits._

    val methodList = List(
      ComputationMethod("vanilla_sql", "sql", vanilla_sql.run),
      ComputationMethod("vanilla_fluent", "sql", vanilla_fluent.run),
      ComputationMethod("vanilla_udaf", "sql", vanilla_udaf.run),
      ComputationMethod("vanilla_rdd_grpmap", "rdd", vanilla_rdd_grpmap.run),
      ComputationMethod("vanilla_rdd_reduce", "rdd", vanilla_rdd_reduce.run),
      ComputationMethod("vanilla_rdd_mappart", "rdd", vanilla_rdd_mappart.run))

    val numGrp1 = 3
    val numGrp2 = 3
    val pyData_3_3_10 = DataPoint.generateData(3, 3, MathHelper.pow(10, 1))
    val pyData_3_3_100 = DataPoint.generateData(3, 3, MathHelper.pow(10, 2))
    val pyData_3_3_1k = DataPoint.generateData(3, 3, MathHelper.pow(10, 3))
    val pyData_3_3_10k = DataPoint.generateData(3, 3, MathHelper.pow(10, 4))
    val pyData_3_3_100k = DataPoint.generateData(3, 3, MathHelper.pow(10, 5))
    val datasets = pyData_3_3_10 :: pyData_3_3_100 :: 
      pyData_3_3_1k :: pyData_3_3_10k :: pyData_3_3_100k :: Nil
    var correctValues = scala.collection.mutable.Map[Long, Map[(Long,Long),AggResult]]() 
    for( dataset <- datasets) {
      correctValues(dataset.length) = DataPoint.generateCorrectValues(dataset)
    }
    val NumRunsPer = 100

    var itinerary = (for (
      method <- methodList;
      data <- datasets;
      iteration <- 0 until NumRunsPer 
    ) yield ItineraryStop(method, data)).toList
    var random = scala.util.Random
    itinerary = random.shuffle(itinerary)
    val itinerarySize = itinerary.length

    var pw = FileHelper.openAppend("Results/vanilla_runs_scala.csv")
    pw.println(s" outcome,rawmethod,interface,expectedSize,returnedSize,elapsedTime")
    try {
      for ((itineraryStop, index) <- itinerary.view.zipWithIndex) {
        val method = itineraryStop.compMethod
        val srcdata = itineraryStop.data
        println(s"Working on $index of $itinerarySize")
        val startedTime = System.nanoTime()
        var (rdd, df, ds) = method.delegate(srcdata.length, srcdata, sc, ss)
        var actual: Array[AggResult] = null
        if (rdd != null) {
          actual = rdd.collect()
        } else if (df != null) {
          actual = df.as[AggResult].collect()
        } else if (ds != null) {
          actual = ds.collect()
        } else {
          throw new Exception("no data returned")
        }
        val finishedTime = System.nanoTime()
        val recordCount = actual.length
        var success = true
        if (numGrp1 * numGrp2 != recordCount) {
          println(s"!Record count doesn't match {numGrp1} {numGrp2} {recordCount}")
          success = false
        } else {
          if(!DataPoint.checkData(numGrp1, numGrp2, correctValues(srcdata.length), actual)) {
            println(s"!Data isn't write for {numGrp1} {numGrp2} {recordCount}")
            success = false
          }
        }              
        val result = RunResult(
          dataSize = srcdata.length,
          elapsedTime = (finishedTime - startedTime).toDouble / 1e+9,
          recordCount = recordCount)
        var success2 = if (success) "success" else "failure"
        pw.println(s"${success2},${method.name},${method.interface},${result.dataSize},${result.recordCount},${result.elapsedTime}")
        pw.flush()
        actual = null
        df = null
        rdd = null
        ds = null
        System.gc()
        Thread.sleep(1000)
      }
    } finally {
      pw.close()
    }

    ss.stop
  }
}