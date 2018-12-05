package DedupePerfTest

import org.apache.spark.rdd.{ RDD }
import org.apache.spark.sql.{ SparkSession, Row, DataFrame, Dataset, Encoder }
import org.apache.spark.sql.types._
import org.apache.log4j.Logger

object Verifier {
  val log = Logger.getLogger(getClass.getName)

  def verifyCorrectnessRdd(
    NumSources: Int, actualNumPeople: Int, dataSize: Long,
    rdd: RDD[Record],
    ss:  SparkSession): Boolean = {
    import ss.implicits._
    verifyCorrectnessDs(NumSources, actualNumPeople, dataSize,
      ss.createDataFrame(rdd).as[Record], ss)
  }

  def verifyCorrectnessDf(
    NumSources: Int, actualNumPeople: Int, dataSize: Long,
    dfDeduped: DataFrame,
    ss:        SparkSession): Boolean = {
    import ss.implicits._
    val ds = dfDeduped.as[Record]
    verifyCorrectnessDs(
      NumSources, actualNumPeople, dataSize,
      ds,
      ss)
  }
  def verifyCorrectnessDs(
    NumSources: Int, actualNumPeople: Int, dataSize: Long,
    dfDeduped: Dataset[Record],
    ss:        SparkSession): Boolean = {
    import ss.implicits._
    val df = dfDeduped.orderBy($"FieldA".cast(IntegerType))
    val deduped = df.collect()
    try {
      val secretKeys = deduped
        .map(x => x.SecretKey.toInt)
        .toSet
      val expectedSecretKeys =
        (1 to actualNumPeople).toSet
      if (secretKeys != expectedSecretKeys) {
        val dmissing = expectedSecretKeys &~ secretKeys
        val dextra = secretKeys &~ expectedSecretKeys
        throw new Exception(f"Missing $dmissing extra $dextra")
      }

      val count = deduped.length
      if (count != actualNumPeople) {
        throw new Exception(f"df.count()=$count != numPeople=$actualNumPeople ")
      }
      val NumBRecords = math.max(1, 2 * actualNumPeople / 100)
      for ((row: Record, index: Int) <- deduped.zipWithIndex) {
        val i: Long = index + 1
        val hashString = RecordMethods.hashName(i)
        if (f"FFFFFFA${i}_${hashString}" != row.FirstName) {
          throw new Exception(f"${i}: FFFFFFA${i}_${hashString} != ${row.FirstName}")
        }
        if (f"LLLLLLA${i}_${hashString}" != row.LastName) {
          throw new Exception(f"${i}: LLLLLLA{i}_${hashString} != ${row.LastName}")
        }
        if (f"${i} Main St" != row.StreetAddress) {
          throw new Exception(f"${i} Main St != row.StreetAddress")
        }
        if ("Plaineville ME" != row.City) {
          throw new Exception("${i}: Plaineville ME != ${row.City}")
        }
        val sZipCode = f"${(i.toInt-1)%100}%05d"
        if (f"${(i.toInt - 1) % 100}%05d" != row.ZipCode) {
          throw new Exception(f"${((i.toInt - 1) % 100) % 05d} != ${row.ZipCode}")
        }
        if (i != row.SecretKey) {
          throw new Exception(f"${i}: ${i} != SecretKey=${row.SecretKey}")
        }
        if (f"${i * 2}" != row.FieldA) {
          throw new Exception(f"${i}: ${i * 2} != FieldA=${row.FieldA}")
        }

        if ((NumSources < 2) || (i > NumBRecords)) {
          if (row.FieldB != null) {
            throw new Exception("${i}: row.FieldB is not None, NumSources=${NumSources}, NumBRecords=${NumBRecords}")
          }
        } else {
          if (f"${i * 3}" != row.FieldB) {
            throw new Exception(f"${i}: ${i * 3} != FieldB=${row.FieldB}")
          }
        }

        if (NumSources < 3) {
          if (row.FieldC != null) {
            throw new Exception("${i}: row.FieldC is not None, NumSources=${NumSources}")
          }
        } else {
          if (f"${i * 5}" != row.FieldC) {
            throw new Exception(f"${i}: ${i * 5} != FieldC=${row.FieldC}")
          }
        }

        if (NumSources < 4) {
          if (row.FieldD != null) {
            throw new Exception("${i}: row.FieldD is not None, NumSources=${NumSources}")
          }
        } else {
          if (f"${i * 7}" != row.FieldD) {
            throw new Exception(f"${i}: ${i * 7} != FieldD=${row.FieldD}")
          }
        }

        if (NumSources < 5) {
          if (row.FieldE != null) {
            throw new Exception("${i}: row.FieldE is not None, NumSources=${NumSources}")
          }
        } else {
          if (f"${i * 11}" != row.FieldE) {
            throw new Exception(f"${i}: ${i * 11} != FieldE=${row.FieldE}")
          }
        }

        if (NumSources < 6) {
          if (row.FieldF != null) {
            throw new Exception(f"${i}: row.FieldF is not None, NumSources=${NumSources}")
          }
        } else {
          if (f"${i * 13}" != row.FieldF) {
            throw new Exception(f"$i: ${i * 13} != FieldF=${row.FieldF}")
          }
        }
      }
    } catch {
      case e: Exception => {
        log.error(e)
        println(f"data error ${e.getMessage}")
        return false
      }
    } finally {
      df.unpersist()
    }
    println("Looking Good!")
    return true
  }
}