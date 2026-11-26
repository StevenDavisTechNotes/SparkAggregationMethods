package challenges.vanilla

import scala.util.Random

import org.apache.log4j.Logger
import org.apache.spark.{ SparkContext }
import org.apache.spark.sql.{ SparkSession }

import common.FileHelper
import common.MathHelper

object LogHolder extends Serializable {
  @transient lazy val log = Logger.getLogger(getClass.getName)
}

/**
 * Scala Spark port of the "vanilla" challenge, kept only as a performance comparison for the PySpark
 * implementations. It was run once, in 2018. It is not wired into the Python runner harness or registry
 * (see py_spark/src/challenges/vanilla/vanilla_strategy_directory_scala_spark.py), generates its own random
 * data, and appends one row per run to [[Runner.ResultsFile]]. Run it from the scala_spark directory.
 */
object Runner {
  /** Same layout as the Python runners: results/<year>/<environment>/<challenge>_<engine>_runs.csv */
  val ResultsFile = "results/2026/wsl/vanilla_runs_scala.csv"
  private val ResultsHeader = "outcome,rawmethod,interface,expectedSize,returnedSize,elapsedTime"

  def createSparkSession(): SparkSession = {
    val ss = SparkSession
      .builder
      .master("local[*]")
      .appName("VanillaPerfTest")
      .config("spark.sql.shuffle.partitions", 8)
      .config("spark.ui.enabled", "false")
      .config("spark.rdd.compress", "false")
      .config("spark.shuffle.io.maxRetries", "0")
      .config("spark.port.maxRetries", "0")
      .config("spark.rpc.numRetries", "0")
      .config("spark.task.maxFailures", "1")
      .config("spark.local.dir", sys.env.getOrElse(
        "SPARK_SCRATCH_SPACE",
        new java.io.File(System.getProperty("java.io.tmpdir"), "spark_scratch").getAbsolutePath()))
      .getOrCreate()
    LogHolder.log.info("script initialized")
    ss
  }

  final case class ComputationMethod(
    name:      String,
    interface: String,
    delegate:  (Long, IndexedSeq[DataPoint], SparkContext, SparkSession) => StrategyResult)
  final case class ItineraryStop(
    compMethod: ComputationMethod,
    data:       IndexedSeq[DataPoint])

  def main(args: Array[String]): Unit = {
    val ss = createSparkSession()
    val sc = ss.sparkContext

    val methodList = List(
      ComputationMethod("vanilla_sql", "sql", VanillaSQL.run),
      ComputationMethod("vanilla_fluent", "sql", VanillaFluent.run),
      ComputationMethod("vanilla_udaf", "sql", VanillaUDAF.run),
      ComputationMethod("vanilla_rdd_grpmap", "rdd", VanillaRDDGrpMap.run),
      ComputationMethod("vanilla_rdd_reduce", "rdd", VanillaRDDReduce.run),
      ComputationMethod("vanilla_rdd_mappart", "rdd", VanillaRDDMapPart.run))

    val numGrp1 = 3
    val numGrp2 = 3
    val datasets = (1 to 5).map(exponent => DataPoint.generateData(numGrp1, numGrp2, MathHelper.pow(10, exponent)))
    val correctValues = datasets.map(dataset => dataset.length.toLong -> DataPoint.generateCorrectValues(dataset)).toMap
    val numRunsPer = 100

    val itinerary = Random.shuffle(
      for (
        method <- methodList;
        data <- datasets;
        _ <- 0 until numRunsPer
      ) yield ItineraryStop(method, data))
    val itinerarySize = itinerary.length

    val resultFile = new java.io.File(ResultsFile)
    val isNewFile = !resultFile.exists
    val pw = FileHelper.openAppend(ResultsFile)
    if (isNewFile) pw.println(ResultsHeader)
    try {
      for ((itineraryStop, index) <- itinerary.zipWithIndex) {
        val method = itineraryStop.compMethod
        val srcdata = itineraryStop.data
        LogHolder.log.info(s"Working on $index of $itinerarySize")
        val startedTime = System.nanoTime()
        val actual = method.delegate(srcdata.length, srcdata, sc, ss).collect(ss)
        val finishedTime = System.nanoTime()
        val recordCount = actual.length
        val success =
          if (numGrp1 * numGrp2 != recordCount) {
            LogHolder.log.error(s"Record count $recordCount doesn't match $numGrp1 x $numGrp2")
            false
          } else if (!DataPoint.checkData(numGrp1, numGrp2, correctValues(srcdata.length), actual)) {
            LogHolder.log.error(s"Data isn't right for $numGrp1 x $numGrp2 with $recordCount records")
            false
          } else {
            true
          }
        val elapsedTime = (finishedTime - startedTime).toDouble / 1e+9
        val outcome = if (success) "success" else "failure"
        pw.println(s"$outcome,${method.name},${method.interface},${srcdata.length},$recordCount,$elapsedTime")
        pw.flush()
        // Keep one run's garbage from being billed to the next run's timing.
        System.gc()
        Thread.sleep(1000)
      }
    } finally {
      pw.close()
    }

    ss.stop()
  }
}
