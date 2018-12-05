package DedupePerfTest

import java.io.File
import java.io.PrintWriter
import org.apache.spark.sql.{ SparkSession, Row, DataFrame, Dataset }
import org.apache.spark.rdd.{ RDD }
import org.apache.spark.sql.functions.{ lit }
import org.apache.spark.storage.StorageLevel

final case class GeneratedDataSet(
  NumSources: Int,
  DataSize:   Long,
  rdd:        RDD[Record],
  df:         DataFrame,
  ds:         Dataset[Record],
  rddWSrc:    RDD[RecordWSrc],
  dfWSrc:     DataFrame,
  dsWSrc:     Dataset[RecordWSrc]);

final case class GeneratedDataSetCollection(
  NumPeople: Int,
  dfSrc:     List[GeneratedDataSet]);

final case class InternalDataSet(
  sourceId: Int,
  letter:   Character,
  filePath: String,
  rdd:      RDD[RecordWSrc],
  df:       DataFrame,
  ds:       Dataset[RecordWSrc]);

object SampleDataCache {
  def writeDataSet(filePath: String, numPeople: Int, letter: Character) = {
    val writer = new PrintWriter(new File(filePath))
    for (i <- 1 to numPeople) {
      val sRecord = RecordMethods.generateRecord(i, letter)
      writer.write(sRecord)
    }
    writer.close()
  }
  def blankToNull(s: String) = {
    if (s.length > 0) s else null
  }
  def parseIteratorOfStringsToRecordWSrcId(
    partitionIndex: Int,
    sourceId:       Int,
    iter:           Iterator[String]): Iterator[RecordWSrc] = {
    iter.map(sRow => {
      var f = sRow.split(",", -1)
      assert(f.length == 12)
      RecordWSrc(
        FirstName = f(0),
        LastName = f(1),
        StreetAddress = f(2),
        City = f(3),
        ZipCode = f(4),
        SecretKey = f(5).toInt,
        FieldA = blankToNull(f(6)),
        FieldB = blankToNull(f(7)),
        FieldC = blankToNull(f(8)),
        FieldD = blankToNull(f(9)),
        FieldE = blankToNull(f(10)),
        FieldF = blankToNull(f(11)),
        SourceId = sourceId)
    })
  }
  def readTestDataSet(
      numPeople: Int, 
      debugging: Boolean, 
      isCloudMode:Boolean, 
      canAssumeNoDupesPerPartition: Boolean, 
      numPartitions: Int, 
      ss: SparkSession): GeneratedDataSetCollection = {
    import ss.implicits._
    val letters = 'A' :: 'B' :: 'C' :: 'D' :: 'E' :: 'F' :: Nil

    val filenames = (letters.zipWithIndex.map {
      case (letter, sourceId) => {
        var filePath = if (isCloudMode) "adl:///sparkperftesting" else "e:/temp/sparkperftesting"
        filePath += f"/Field$letter$numPeople%d.csv"
        val file = new File(filePath)
        if (!file.exists) {
          var thisNumPeople: Int = -1
          if (letter != 'B') {
            thisNumPeople = numPeople
          } else {
            thisNumPeople = 2 * numPeople / 100
            thisNumPeople = if (thisNumPeople > 0) thisNumPeople else 1
          }
          writeDataSet(filePath, thisNumPeople, letter)
        }
        val rdd = ss.sparkContext.textFile(filePath, minPartitions = 1)
          .mapPartitionsWithIndex((partitionIndex, iter) => parseIteratorOfStringsToRecordWSrcId(partitionIndex, sourceId, iter))
        var df = ss.read.format("csv")
          .option("header", "false")
          .schema(RecordMethods.RecordSparseStruct)
          .load(filePath)
          .withColumn("SourceId", lit(sourceId))
        if (canAssumeNoDupesPerPartition) {
          df = df.coalesce(1)
        } else {
          df = df.repartition(numPartitions)
        }
        val ds = df
          .as[RecordWSrc]
        InternalDataSet(sourceId, letter, filePath, rdd, df, ds)
      }
    })

    val rddA = filenames(0).rdd
    val rddB = filenames(1).rdd
    val rddC = filenames(2).rdd
    val rddD = filenames(3).rdd
    val rddE = filenames(4).rdd
    val rddF = filenames(5).rdd
    var rddBunch2 = rddA.union(rddB)
    var rddBunch3 = rddBunch2.union(rddC)
    var rddBunch6 = rddBunch3
      .union(rddD).union(rddE).union(rddF)
    rddBunch2.persist(StorageLevel.DISK_ONLY)
    rddBunch3.persist(StorageLevel.DISK_ONLY)
    rddBunch6.persist(StorageLevel.DISK_ONLY)
    if (canAssumeNoDupesPerPartition) {
      assert(rddBunch2.getNumPartitions == 2)
      assert(rddBunch3.getNumPartitions == 3)
      assert(rddBunch6.getNumPartitions == 6)
    }

    val dsA = filenames(0).ds
    val dsB = filenames(1).ds
    val dsC = filenames(2).ds
    val dsD = filenames(3).ds
    val dsE = filenames(4).ds
    val dsF = filenames(5).ds
    var dsBunch2 = dsA.unionByName(dsB)
    var dsBunch3 = dsBunch2.unionByName(dsC)
    var dsBunch6 = dsBunch3
      .unionByName(dsD).unionByName(dsE).unionByName(dsF)
    dsBunch2.persist(StorageLevel.DISK_ONLY)
    dsBunch3.persist(StorageLevel.DISK_ONLY)
    dsBunch6.persist(StorageLevel.DISK_ONLY)
    if (canAssumeNoDupesPerPartition) {
      assert(dsBunch2.rdd.getNumPartitions == 2)
      assert(dsBunch3.rdd.getNumPartitions == 3)
      assert(dsBunch6.rdd.getNumPartitions == 6)
    }

    val dfA = filenames(0).df
    val dfB = filenames(1).df
    val dfC = filenames(2).df
    val dfD = filenames(3).df
    val dfE = filenames(4).df
    val dfF = filenames(5).df
    var dfBunch2 = dfA.unionByName(dfB)
    var dfBunch3 = dfBunch2.unionByName(dfC)
    var dfBunch6 = dfBunch3
      .unionByName(dfD).unionByName(dfE).unionByName(dfF)
    dfBunch2.persist(StorageLevel.DISK_ONLY)
    dfBunch3.persist(StorageLevel.DISK_ONLY)
    dfBunch6.persist(StorageLevel.DISK_ONLY)
    if (canAssumeNoDupesPerPartition) {
      assert(dfBunch2.rdd.getNumPartitions == 2)
      assert(dfBunch3.rdd.getNumPartitions == 3)
      assert(dfBunch6.rdd.getNumPartitions == 6)
    }

    val sizes2 = Seq(rddBunch2.count(), dfBunch2.count(), dsBunch2.count())
    val minsize2 = sizes2.min(Ordering.by((i: Long) => i))
    val maxsize2 = sizes2.max(Ordering.by((i: Long) => i))
    assert(minsize2 == maxsize2)

    val sizes3 = Seq(rddBunch3.count(), dfBunch3.count(), dsBunch3.count())
    val minsize3 = sizes3.min(Ordering.by((i: Long) => i))
    val maxsize3 = sizes3.max(Ordering.by((i: Long) => i))
    assert(minsize3 == maxsize3)

    val sizes6 = Seq(rddBunch6.count(), dfBunch6.count(), dsBunch6.count())
    val minsize6 = sizes6.min(Ordering.by((i: Long) => i))
    val maxsize6 = sizes6.max(Ordering.by((i: Long) => i))
    assert(minsize6 == maxsize6)

    val rddBunch2WO = rddBunch2
      .map(RecordMethods.RecordWSrcToRecord)
    val rddBunch3WO = rddBunch3
      .map(RecordMethods.RecordWSrcToRecord)
    val rddBunch6WO = rddBunch6
      .map(RecordMethods.RecordWSrcToRecord)
    rddBunch2WO.persist(StorageLevel.DISK_ONLY)
    rddBunch3WO.persist(StorageLevel.DISK_ONLY)
    rddBunch6WO.persist(StorageLevel.DISK_ONLY)
    assert(rddBunch2WO.count() == rddBunch2.count())
    assert(rddBunch3WO.count() == rddBunch3.count())
    assert(rddBunch6WO.count() == rddBunch6.count())

    val dsBunch2WO = dsBunch2
      .map(RecordMethods.RecordWSrcToRecord)
    val dsBunch3WO = dsBunch3
      .map(RecordMethods.RecordWSrcToRecord)
    val dsBunch6WO = dsBunch6
      .map(RecordMethods.RecordWSrcToRecord)
    dsBunch2WO.persist(StorageLevel.DISK_ONLY)
    dsBunch3WO.persist(StorageLevel.DISK_ONLY)
    dsBunch6WO.persist(StorageLevel.DISK_ONLY)
    assert(dsBunch2WO.count() == dsBunch2.count())
    assert(dsBunch3WO.count() == dsBunch3.count())
    assert(dsBunch6WO.count() == dsBunch6.count())

    val dfBunch2WO = dfBunch2
      .drop("SourceId")
    val dfBunch3WO = dfBunch3
      .drop("SourceId")
    val dfBunch6WO = dfBunch6
      .drop("SourceId")
    dfBunch2WO.persist(StorageLevel.DISK_ONLY)
    dfBunch3WO.persist(StorageLevel.DISK_ONLY)
    dfBunch6WO.persist(StorageLevel.DISK_ONLY)
    assert(dfBunch2WO.count() == dfBunch2.count())
    assert(dfBunch3WO.count() == dfBunch3.count())
    assert(dfBunch6WO.count() == dfBunch6.count())

    var dfSrc = List(
      GeneratedDataSet(
        NumSources = 2, DataSize = minsize2,
        rdd = rddBunch2WO, df = dfBunch2WO, ds = dsBunch2WO,
        rddWSrc = rddBunch2, dfWSrc = dfBunch2, dsWSrc = dsBunch2),
      GeneratedDataSet(
        NumSources = 3, DataSize = minsize3,
        rdd = rddBunch3WO, df = dfBunch3WO, ds = dsBunch3WO,
        rddWSrc = rddBunch3, dfWSrc = dfBunch3, dsWSrc = dsBunch3),
      GeneratedDataSet(
        NumSources = 6, DataSize = minsize6,
        rdd = rddBunch6WO, df = dfBunch6WO, ds = dsBunch6WO,
        rddWSrc = rddBunch6, dfWSrc = dfBunch6, dsWSrc = dsBunch6))
    if (debugging) dfSrc = dfSrc(0) :: Nil
    GeneratedDataSetCollection(NumPeople = numPeople, dfSrc = dfSrc)
  }

}