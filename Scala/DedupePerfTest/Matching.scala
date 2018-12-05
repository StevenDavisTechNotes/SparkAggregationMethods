package DedupePerfTest

import scala.collection.{ mutable }
import util.control.Breaks._
import shapeless._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{ udf }
import org.apache.spark.sql.{ Row }

object Matching {
  val MatchThreshold = 0.9
  // must be 0.4316546762589928 < threshold < 0.9927007299270073 @ 10k

  // http://rosettacode.org/wiki/Levenshtein_distance#Scala
  def distance(s1: String, s2: String): Int = {
    val dist = Array.tabulate(s2.length + 1, s1.length + 1) { (j, i) => if (j == 0) i else if (i == 0) j else 0 }

    @inline
    def minimum(i: Int*): Int = i.min

    for {
      j <- dist.indices.tail
      i <- dist(0).indices.tail
    } dist(j)(i) =
      if (s2(j - 1) == s1(i - 1)) dist(j - 1)(i - 1)
      else minimum(dist(j - 1)(i) + 1, dist(j)(i - 1) + 1, dist(j - 1)(i - 1) + 1)

    dist(s2.length)(s1.length)
  }

  // based on https://stackoverflow.com/questions/955110/similarity-string-comparison-in-java/16018452
  def stringSimilarity(s1: String, s2: String): Double = {
    var longer: String = null
    var shorter: String = null
    if (s1.length() < s2.length) {
      longer = s2; shorter = s1
    } else {
      longer = s1; shorter = s2
    }
    val longerLength = longer.length()
    if (longerLength == 0) { return 1.0; /* both strings are zero length */ }
    return (longerLength - distance(longer, shorter)) / longerLength.toDouble;
  }

  def IsMatch(
    iFirstName: String, jFirstName: String,
    iLastName: String, jLastName: String,
    iZipCode: String, jZipCode: String,
    iSecretKey: Int, jSecretKey: Int): Boolean = {
    val actualRatioFirstName = stringSimilarity(
      iFirstName, jFirstName)
    if (actualRatioFirstName < MatchThreshold) {
      if (iSecretKey == jSecretKey) {
        throw new Exception(f"FirstName non-match for {iSecretKey} with itself {iFirstName} {jFirstName} actualRatioFirstName={actualRatioFirstName}")
      }
      return false
    }
    val actualRatioLastName = stringSimilarity(
      iLastName, jLastName)
    if (actualRatioLastName < MatchThreshold) {
      if (iSecretKey == jSecretKey) {
        throw new Exception(f"LastName non-match for {iSecretKey} with itself {iLastName} {jLastName} with actualRatioLastName={actualRatioLastName}")
      }
      return false
    }
    if (iSecretKey != jSecretKey) {
      throw new Exception(f"""
        False match for {iSecretKey}-{jSecretKey} with itself 
        iFirstName={iFirstName} jFirstName={jFirstName} actualRatioFirstName={actualRatioFirstName} 
        iLastName={iLastName} jLastName={jLastName} actualRatioLastName={actualRatioLastName}""")
    }
    return true
  }

  val MatchSingleName_Returns = BooleanType
  def MatchSingleName(
    lhs: String, rhs: String,
    iSecretKey: Long, jSecretKey: Long): Boolean = {
    val actualRatio = stringSimilarity(lhs, rhs)
    if (actualRatio < MatchThreshold) {
      if (iSecretKey == jSecretKey) {
        throw new Exception(
          f"Name non-match for {iSecretKey} with itself {lhs} {rhs} ratio={actualRatio}")
      }
      return false
    }
    return true
  }

  val FindRecordMatches_RecList_Returns = ArrayType(
    StructType(
      StructField("idLeftVertex", IntegerType, false) ::
        StructField("idRightVertex", IntegerType, false) ::
        Nil))

  case class Edge(
    idLeftVertex:  Int,
    idRightVertex: Int)

  case class Component(
    idComponent:  Int,
    idVertexList: Array[Int])

  def FindRecordMatches_RecList(recordList: Array[RecordWSrc]): List[Edge] = {
    val n = recordList.length
    val edgeList = mutable.ListBuffer[Edge]()
    for (i <- 0 to n - 2) {
      val irow = recordList(i)
      breakable {
        for (j <- i + 1 to n - 1) {
          val jrow = recordList(j)
          if (irow.SourceId == jrow.SourceId) {
            assert(irow.SecretKey != jrow.SecretKey)
          } else if (IsMatch(
            irow.FirstName, jrow.FirstName,
            irow.LastName, jrow.LastName,
            irow.ZipCode, jrow.ZipCode,
            irow.SecretKey, jrow.SecretKey)) {
            edgeList.append(Edge(
              idLeftVertex = i,
              idRightVertex = j))
            assert(irow.SecretKey == jrow.SecretKey)
            break // safe if assuming Commutative and transitive
          } else {
            assert(irow.SecretKey != jrow.SecretKey)
          }
        }
      }
    }
    edgeList.toList
  }

  val FindConnectedComponents_RecList_Returns = ArrayType(
    StructType(
      StructField(
        "idComponent",
        IntegerType,
        false) ::
        StructField(
          "idVertexList",
          ArrayType(
            IntegerType),
          false) :: Nil))

  def FindConnectedComponents_RecList(edgeList: Seq[Edge]): List[Component] = {
    // This is not optimal for large components.  See GraphFrame
    val componentForVertex = new mutable.HashMap[Int, mutable.Set[Int]]
    for (edge <- edgeList) {
      val newComponent = mutable.Set(edge.idLeftVertex, edge.idRightVertex)
      val leftIsKnown = componentForVertex.contains(edge.idLeftVertex)
      val rightIsKnown = componentForVertex.contains(edge.idRightVertex)
      if (!leftIsKnown && !rightIsKnown) {
        componentForVertex(edge.idLeftVertex) = newComponent
        componentForVertex(edge.idRightVertex) = newComponent
      } else {
        if (leftIsKnown) {
          newComponent ++= componentForVertex(edge.idLeftVertex)
        }
        if (rightIsKnown) {
          newComponent ++= componentForVertex(edge.idRightVertex)
        }
        for (vertex <- newComponent) {
          componentForVertex(vertex) = newComponent
        }
      }
    }
    val knownComponents = new mutable.HashSet[Int]()
    val componentList = mutable.ListBuffer[Component]()
    for ((idVertex, component) <- componentForVertex) {
      if (!knownComponents.contains(idVertex)) {
        componentList.append(
          Component(
            idComponent = componentList.length,
            idVertexList = component.toArray.sorted))
        knownComponents ++= component
      }
    }
    componentList.toList
  }

  // MergeItems_RecList
  val MergeItems_RecList_Returns = ArrayType(
    StructType(
      RecordMethods.RecordSparseStruct.fields ++
        Array(
          StructField("SourceId", IntegerType, false),
          StructField("BlockingKey", StringType, false))))
  def MergeItems_RecList(
    blockedDataList:        Array[RecordWSrc],
    connectedComponentList: Seq[Component]): List[Record] = {
    val verticesInAComponent = mutable.HashSet[Int]()
    val returnList = mutable.ListBuffer[Record]()
    for (component <- connectedComponentList) {
      verticesInAComponent ++= component.idVertexList
      val constituentList =
        component.idVertexList.map(i => blockedDataList(i))
      assert(constituentList.length > 1)
      val rec = CombineRowWSrcList(constituentList)
      returnList.append(rec)
    }
    for ((rec, idx) <- blockedDataList.view.zipWithIndex) {
      if (!verticesInAComponent.contains(idx)) {
        returnList.append(RecordMethods.RecordWSrcToRecord(rec))
      }
    }
    returnList.toList
  }

  def ProcessBlock(sourceRecordData: Iterable[RecordWSrc]): List[Record] = {
    var blockedData = sourceRecordData.toArray
    var firstOrderEdges = Matching.FindRecordMatches_RecList(blockedData)
    val connectedComponents = Matching.FindConnectedComponents_RecList(firstOrderEdges)
    firstOrderEdges = null
    Matching.MergeItems_RecList(blockedData, connectedComponents)
  }

  def CombineRowWSrcList(constituentList: Seq[RecordWSrc]): Record = {
    var FirstName: String = null
    var LastName: String = null
    var StreetAddress: String = null
    var City: String = null
    var ZipCode: String = null
    var SecretKey: Int = 0
    var FieldA: String = null
    var FieldB: String = null
    var FieldC: String = null
    var FieldD: String = null
    var FieldE: String = null
    var FieldF: String = null

    assert(constituentList.length > 0)

    {
      val sorted =
        constituentList.sortBy(x => (-
          (if ((x.LastName != null) &&
            (x.LastName.length > 0)) 2 else 0) +
          (if ((x.FirstName != null) &&
            (x.FirstName.length > 0)) 1 else 0),
          x.LastName, x.FirstName))
      if (sorted.isEmpty) {
        throw new Exception("no valid names")
      }
      val best = sorted.head
      FirstName = best.FirstName
      LastName = best.LastName
    }
    {
      val sorted =
        constituentList.sortBy(x => (-
          (if ((x.ZipCode != null) &&
            (x.ZipCode.length > 0)) 4 else 0) +
          (if ((x.City != null) &&
            (x.City.length > 0)) 2 else 0) +
          (if ((x.StreetAddress != null) &&
            (x.StreetAddress.length > 0)) 1 else 0),
          x.LastName, x.FirstName))
      if (sorted.isEmpty) {
        throw new Exception("no valid addresses")
      }
      val best = sorted.head
      StreetAddress = best.StreetAddress
      City = best.City
      ZipCode = best.ZipCode
    }
    {
      var secretKeys = constituentList.map(x => x.SecretKey).distinct
      if (secretKeys.length != 1) {
        throw new Exception(f"Found ${secretKeys.length} distinct keys in a component")
      }
      SecretKey = secretKeys(0)
    }
    FieldA =
      DedupeShared.MinNotNull(constituentList.map(x => x.FieldA))
    FieldB =
      DedupeShared.MinNotNull(constituentList.map(x => x.FieldB))
    FieldC =
      DedupeShared.MinNotNull(constituentList.map(x => x.FieldC))
    FieldD =
      DedupeShared.MinNotNull(constituentList.map(x => x.FieldD))
    FieldE =
      DedupeShared.MinNotNull(constituentList.map(x => x.FieldE))
    FieldF =
      DedupeShared.MinNotNull(constituentList.map(x => x.FieldF))
    Record(
      FirstName,
      LastName,
      StreetAddress,
      City,
      ZipCode,
      SecretKey,
      FieldA,
      FieldB,
      FieldC,
      FieldD,
      FieldE,
      FieldF)
  }
  def CombineRowList(constituentList: Seq[Record]): Record = {
    var FirstName: String = null
    var LastName: String = null
    var StreetAddress: String = null
    var City: String = null
    var ZipCode: String = null
    var SecretKey: Int = 0
    var FieldA: String = null
    var FieldB: String = null
    var FieldC: String = null
    var FieldD: String = null
    var FieldE: String = null
    var FieldF: String = null

    assert(constituentList.length > 0)

    {
      val sorted =
        constituentList.sortBy(x => (-
          (if ((x.LastName != null) &&
            (x.LastName.length > 0)) 2 else 0) +
          (if ((x.FirstName != null) &&
            (x.FirstName.length > 0)) 1 else 0),
          x.LastName, x.FirstName))
      if (sorted.isEmpty) {
        throw new Exception("no valid names")
      }
      val best = sorted.head
      FirstName = best.FirstName
      LastName = best.LastName
    }
    {
      val sorted =
        constituentList.sortBy(x => (-
          (if ((x.ZipCode != null) &&
            (x.ZipCode.length > 0)) 4 else 0) +
          (if ((x.City != null) &&
            (x.City.length > 0)) 2 else 0) +
          (if ((x.StreetAddress != null) &&
            (x.StreetAddress.length > 0)) 1 else 0),
          x.LastName, x.FirstName))
      if (sorted.isEmpty) {
        throw new Exception("no valid addresses")
      }
      val best = sorted.head
      StreetAddress = best.StreetAddress
      City = best.City
      ZipCode = best.ZipCode
    }
    {
      var secretKeys = constituentList.map(x => x.SecretKey).distinct
      if (secretKeys.length != 1) {
        throw new Exception(f"Found ${secretKeys.length} distinct keys in a component")
      }
      SecretKey = secretKeys(0)
    }
    FieldA =
      DedupeShared.MinNotNull(constituentList.map(x => x.FieldA))
    FieldB =
      DedupeShared.MinNotNull(constituentList.map(x => x.FieldB))
    FieldC =
      DedupeShared.MinNotNull(constituentList.map(x => x.FieldC))
    FieldD =
      DedupeShared.MinNotNull(constituentList.map(x => x.FieldD))
    FieldE =
      DedupeShared.MinNotNull(constituentList.map(x => x.FieldE))
    FieldF =
      DedupeShared.MinNotNull(constituentList.map(x => x.FieldF))
    Record(
      FirstName,
      LastName,
      StreetAddress,
      City,
      ZipCode,
      SecretKey,
      FieldA,
      FieldB,
      FieldC,
      FieldD,
      FieldE,
      FieldF)
  }
}