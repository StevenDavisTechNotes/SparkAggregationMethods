package DedupePerfTest

import java.security.MessageDigest
import java.math.BigInteger
import org.apache.spark.sql.{ Row }
import org.apache.spark.sql.types._

abstract class TRecord {
  def FirstName: String
  def LastName: String
  def StreetAddress: String
  def City: String
  def ZipCode: String
  def SecretKey: Int
  def FieldA: String
  def FieldB: String
  def FieldC: String
  def FieldD: String
  def FieldE: String
  def FieldF: String
}
final case class Record(
  FirstName:     String,
  LastName:      String,
  StreetAddress: String,
  City:          String,
  ZipCode:       String,
  SecretKey:     Int,
  FieldA:        String,
  FieldB:        String,
  FieldC:        String,
  FieldD:        String,
  FieldE:        String,
  FieldF:        String) extends TRecord
final case class RecordWSrc(
  FirstName:     String,
  LastName:      String,
  StreetAddress: String,
  City:          String,
  ZipCode:       String,
  SecretKey:     Int,
  FieldA:        String,
  FieldB:        String,
  FieldC:        String,
  FieldD:        String,
  FieldE:        String,
  FieldF:        String,
  SourceId:      Int) extends TRecord
final case class RecordWSrcWRowId(
  FirstName:     String,
  LastName:      String,
  StreetAddress: String,
  City:          String,
  ZipCode:       String,
  SecretKey:     Int,
  FieldA:        String,
  FieldB:        String,
  FieldC:        String,
  FieldD:        String,
  FieldE:        String,
  FieldF:        String,
  SourceId:      Int,
  RowId:         Long) extends TRecord
final case class RecordWSrcWBlkKey(
  FirstName:     String,
  LastName:      String,
  StreetAddress: String,
  City:          String,
  ZipCode:       String,
  SecretKey:     Int,
  FieldA:        String,
  FieldB:        String,
  FieldC:        String,
  FieldD:        String,
  FieldE:        String,
  FieldF:        String,
  SourceId:      Int,
  BlockingKey:   String) extends TRecord

object RecordMethods {
  def RecordSparseStruct = StructType(
    StructField("FirstName", StringType, false) ::
      StructField("LastName", StringType, false) ::
      StructField("StreetAddress", StringType, true) ::
      StructField("City", StringType, true) ::
      StructField("ZipCode", StringType, false) ::
      StructField("SecretKey", IntegerType, false) ::
      StructField("FieldA", StringType, true) ::
      StructField("FieldB", StringType, true) ::
      StructField("FieldC", StringType, true) ::
      StructField("FieldD", StringType, true) ::
      StructField("FieldE", StringType, true) ::
      StructField("FieldF", StringType, true) ::
      Nil)
  def RecordSparseStructB = new StructType()
    .add("FirstName", StringType, false)
    .add("LastName", StringType, false)
    .add("StreetAddress", StringType, true)
    .add("City", StringType, true)
    .add("ZipCode", StringType, false)
    .add("SecretKey", IntegerType, false)
    .add("FieldA", StringType, true)
    .add("FieldB", StringType, true)
    .add("FieldC", StringType, true)
    .add("FieldD", StringType, true)
    .add("FieldE", StringType, true)
    .add("FieldF", StringType, true)

  def RecordWSrcStruct = StructType(
    StructField("FirstName", StringType, false) ::
      StructField("LastName", StringType, false) ::
      StructField("StreetAddress", StringType, true) ::
      StructField("City", StringType, true) ::
      StructField("ZipCode", StringType, false) ::
      StructField("SecretKey", IntegerType, false) ::
      StructField("FieldA", StringType, true) ::
      StructField("FieldB", StringType, true) ::
      StructField("FieldC", StringType, true) ::
      StructField("FieldD", StringType, true) ::
      StructField("FieldE", StringType, true) ::
      StructField("FieldF", StringType, true) ::
      StructField("SourceId", IntegerType, false) ::
      Nil)

  val hasher = MessageDigest.getInstance("SHA-512")
  def hashName(i: Long) = {
    val hashArray =
      hasher
        .digest(i.toString.getBytes("UTF-8"))
    val hashString =
      String.format("%0128x", new BigInteger(1, hashArray))
    hashString
  }

  def generateRecord(i: Long, letter: Character) = {
    val hashString = hashName(i)
    Array(
      s"FFFFFF${letter}${i}_${hashString}",
      s"LLLLLL${letter}${i}_${hashString}",
      s"${i} Main St,Plaineville ME",
      f"${(i - 1) % 100}%05d",
      i.toString,
      if (letter == 'A') (i * 2).toString else "",
      if (letter == 'B') (i * 3).toString else "",
      if (letter == 'C') (i * 5).toString else "",
      if (letter == 'D') (i * 7).toString else "",
      if (letter == 'E') (i * 11).toString else "",
      if (letter == 'F') (i * 13).toString else "").mkString(",") + "\n"
  }
  val HeaderRow = Array(
    "FirstName",
    "LastName",
    "StreetAddress",
    "City",
    "ZipCode",
    "SecretKey",
    "FieldA",
    "FieldB",
    "FieldC",
    "FieldD",
    "FieldE",
    "FieldF").mkString(",") + "\n"

  def rowToRecord = (x: Row) => {
    Record(
      FirstName = x.getAs[String]("FirstName"),
      LastName = x.getAs[String]("LastName"),
      StreetAddress = x.getAs[String]("StreetAddress"),
      City = x.getAs[String]("City"),
      ZipCode = x.getAs[String]("ZipCode"),
      SecretKey = x.getAs[Int]("SecretKey"),
      FieldA = x.getAs[String]("FieldA"),
      FieldB = x.getAs[String]("FieldB"),
      FieldC = x.getAs[String]("FieldC"),
      FieldD = x.getAs[String]("FieldD"),
      FieldE = x.getAs[String]("FieldE"),
      FieldF = x.getAs[String]("FieldF"))
  };

  def rowToRecordWSrc = (x: Row) => {
    RecordWSrc(
      FirstName = x.getAs[String]("FirstName"),
      LastName = x.getAs[String]("LastName"),
      StreetAddress = x.getAs[String]("StreetAddress"),
      City = x.getAs[String]("City"),
      ZipCode = x.getAs[String]("ZipCode"),
      SecretKey = x.getAs[Int]("SecretKey"),
      FieldA = x.getAs[String]("FieldA"),
      FieldB = x.getAs[String]("FieldB"),
      FieldC = x.getAs[String]("FieldC"),
      FieldD = x.getAs[String]("FieldD"),
      FieldE = x.getAs[String]("FieldE"),
      FieldF = x.getAs[String]("FieldF"),
      SourceId = x.getAs[Int]("SourceId"))
  };

  def RecordWSrcToRecord(x: RecordWSrc): Record = {
    Record(
      x.FirstName,
      x.LastName,
      x.StreetAddress,
      x.City,
      x.ZipCode,
      x.SecretKey,
      x.FieldA,
      x.FieldB,
      x.FieldC,
      x.FieldD,
      x.FieldE,
      x.FieldF)
  }
}