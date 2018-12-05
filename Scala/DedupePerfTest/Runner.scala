package DedupePerfTest

import java.io._
import scala.collection.immutable.IndexedSeq

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.rdd.{ RDD }
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.{ SparkSession, Row, DataFrame, Dataset, Encoder }

import Common.FileHelper
import Common.MathHelper
import util.control.Breaks._

object LogHolder extends Serializable {
  @transient lazy val log = Logger.getLogger(getClass.getName)
}

object IDERunner {
    def main(args: Array[String]) = {
      (new Runner(IsCloudMode=false, OverrideCanAssumeNoDupesPerPartition=false)).Run()
    }
}
case class Runner(IsCloudMode: Boolean, OverrideCanAssumeNoDupesPerPartition: Boolean) {
  val Debugging = false
  val CanAssumeNoDupesPerPartition = if (IsCloudMode) false else this.OverrideCanAssumeNoDupesPerPartition
  val NumExecutors = if (Debugging) 1 else if (IsCloudMode) 40 else 7
  val DefaultParallelism = if (IsCloudMode) 2 * NumExecutors else 16
  val SufflePartitions = DefaultParallelism
  val MaximumProcessableSegment = MathHelper.pow(10, 5)
  val log = Logger.getLogger(getClass.getName)

  def createSparkSession(): SparkSession = {
    val builder: SparkSession.Builder =
      if (IsCloudMode) SparkSession
        .builder
        .master("yarn")
        .config("spark.executor.instances", NumExecutors)
        .config("spark.dynamicAllocation.enabled", "false")
      else if (Debugging) SparkSession
        .builder
        .master("local[1, 0]")
      else SparkSession
        .builder
        .master("local[7, 0]")
        .config("spark.local.dir", "E:/Temp/SparkTemp")
    val ss: SparkSession =
      builder
        .appName("DedupePerfTest")
        .config("spark.sql.shuffle.partitions", SufflePartitions)
        .config("spark.default.parallelism", DefaultParallelism)
        .config("spark.ui.enabled", "false")
        .config("spark.rdd.compress", "false")
        .config("spark.shuffle.io.maxRetries", "0")
        .config("spark.port.maxRetries", "0")
        .config("spark.rpc.numRetries", "0")
        .config("spark.task.maxFailures", "1")
        .config("spark.sql.execution.arrow.enabled", "true")
        .config("spark.worker.cleanup.enabled", "true")
        .enableHiveSupport()
        .getOrCreate()

    log.info("script initialized")
    ss.sparkContext.setLogLevel("ERROR")
    ss
  }
  final case class ComputationMethod(
    name:      String,
    interface: String,
    delegate:  (GeneratedDataSet, Int, Boolean, SparkSession) => (RDD[Record], DataFrame, Dataset[Record]))
  final case class ItineraryStop(
    compMethod: ComputationMethod,
    numPeople:  Int,
    data:       GeneratedDataSet)
  final case class RunResult(
    DataSize:                     Long,
    ElapsedTime:                  Double,
    RecordCount:                  Long,
    IsCloudMode:                  Boolean,
    CanAssumeNoDupesPerPartition: Boolean)

  def Run() = {
    PropertyConfigurator.configure("log4j.properties")
    val ss = createSparkSession()
    val sc = ss.sparkContext
    import ss.implicits._

    var methodList =
      Seq(
        ComputationMethod("dedupe_fluent_windows", "df", dedupe_fluent_windows.run),
        ComputationMethod("fluent_nested_withCol", "df", method_fluent_nested_withCol.run),
        ComputationMethod("fluent_nested_singleudf", "df", method_fluent_nested_singleudf.run),
        ComputationMethod("rdd_groupby", "rdd", method_rdd_groupby.run),
        ComputationMethod("rdd_reduce", "rdd", method_rdd_reduce.run),
        ComputationMethod("rdd_mappart", "rdd", method_rdd_mappart.run),
        ComputationMethod("aggregator", "df", method_aggregator.run),
        ComputationMethod("udaf", "df", method_udaf.run))

    var srcDfListListSize = List(
      MathHelper.pow(10, 1),
      MathHelper.pow(10, 2),
      MathHelper.pow(10, 3),
      MathHelper.pow(10, 4))
    //    if (Debugging)
    srcDfListListSize = srcDfListListSize(2) :: Nil
    val srcDfListList =
      srcDfListListSize
        .map(n => SampleDataCache.readTestDataSet(
          n, Debugging, IsCloudMode, CanAssumeNoDupesPerPartition,
          NumExecutors, ss))
    System.gc();
    System.runFinalization();

    val NumRunsPer = 30 // 100

    var itinerary: List[ItineraryStop] = (for (
      method <- methodList;
      dataByNumPeople <- srcDfListList;
      dataByNumSources <- dataByNumPeople.dfSrc;
      iteration <- 0 until NumRunsPer
    //      if method.name == "method_udaf"
    ) yield {
      ItineraryStop(
        method,
        dataByNumPeople.NumPeople,
        dataByNumSources)
    }).toList
    var random = scala.util.Random
    itinerary = random.shuffle(itinerary)
    val itinerarySize = itinerary.length

    var pw = FileHelper.openAppend(
      if (IsCloudMode)
        "Results/dedupe_runs_scala_w_cloud.csv"
      else
        "Results/dedupe_runs_scala.csv")
    pw.println(s" outcome,rawmethod,interface,expectedSize,returnedSize,elapsedTime")
    try {
      for ((itineraryStop, index) <- itinerary.view.zipWithIndex) {
        val method = itineraryStop.compMethod
        val numPeople = itineraryStop.numPeople
        val gendataset = itineraryStop.data
        println(s"Working on $index of $itinerarySize sz=${gendataset.DataSize} ${method.name}")
        val startedTime = System.nanoTime()
        var (rdd, df, ds) = method.delegate(
          gendataset,
          this.NumExecutors,
          this.CanAssumeNoDupesPerPartition,
          ss)
        var actual: Array[Record] = null
        if (rdd != null) {
          actual = rdd.collect()
        } else if (df != null) {
          actual = df.as[Record].collect()
        } else if (ds != null) {
          actual = ds.collect()
        } else {
          throw new Exception("no data returned")
        }
        val finishedTime = System.nanoTime()
        val recordCount = actual.length
        var success = true
        if (rdd != null) {
          success = Verifier.verifyCorrectnessRdd(
            gendataset.NumSources,
            numPeople,
            gendataset.DataSize, rdd, ss)
        } else if (df != null) {
          success = Verifier.verifyCorrectnessDf(
            gendataset.NumSources,
            numPeople,
            gendataset.DataSize, df, ss)
        } else if (ds != null) {
          success = Verifier.verifyCorrectnessDs(
            gendataset.NumSources,
            numPeople,
            gendataset.DataSize, ds, ss)
        } else {
          throw new Exception("Don't know how to verify that")
        }
        val result = RunResult(
          DataSize = gendataset.DataSize,
          ElapsedTime = (finishedTime - startedTime).toDouble / 1e+9,
          RecordCount = recordCount,
          IsCloudMode = IsCloudMode,
          CanAssumeNoDupesPerPartition = CanAssumeNoDupesPerPartition)
        var success2 = if (success) "success" else "failure"
        pw.println(s"${success2},${method.name},${method.interface},${result.DataSize},${result.RecordCount},${result.ElapsedTime},${result.IsCloudMode},${result.CanAssumeNoDupesPerPartition}")
        pw.flush()
        actual = null
        df = null
        rdd = null
        ds = null
        System.gc()
        System.runFinalization();
        Thread.sleep(1000)
      }
    } finally {
      pw.close()
    }

    ss.stop
  }
}