import collections
import hashlib
import os
from dataclasses import dataclass
from typing import List, Optional

import pyspark.sql.functions as func
import pyspark.sql.types as DataTypes
from pyspark.sql import SparkSession


@dataclass
class DedupeDataParameters:
    in_cloud_mode: bool
    NumExecutors: int
    CanAssumeNoDupesPerPartition: bool
    DefaultParallelism: int
    SufflePartitions: int
    test_data_file_location: str


@dataclass(frozen=True)
class DataPoint():
    FirstName: str
    LastName: str
    StreetAddress: Optional[str]
    City: str
    ZipCode: str
    SecretKey: int
    FieldA: Optional[str]
    FieldB: Optional[str]
    FieldC: Optional[str]
    FieldD: Optional[str]
    FieldE: Optional[str]
    FieldF: Optional[str]


# region data structure
RecordSparseStruct = DataTypes.StructType([
    DataTypes.StructField("FirstName", DataTypes.StringType(), False),
    DataTypes.StructField("LastName", DataTypes.StringType(), False),
    DataTypes.StructField("StreetAddress", DataTypes.StringType(), True),
    DataTypes.StructField("City", DataTypes.StringType(), True),
    DataTypes.StructField("ZipCode", DataTypes.StringType(), False),
    DataTypes.StructField("SecretKey", DataTypes.IntegerType(), False),
    DataTypes.StructField("FieldA", DataTypes.StringType(), True),
    DataTypes.StructField("FieldB", DataTypes.StringType(), True),
    DataTypes.StructField("FieldC", DataTypes.StringType(), True),
    DataTypes.StructField("FieldD", DataTypes.StringType(), True),
    DataTypes.StructField("FieldE", DataTypes.StringType(), True),
    DataTypes.StructField("FieldF", DataTypes.StringType(), True),
])
# endregion

GenDataSets = collections.namedtuple("GenDataSets", ['NumPeople', 'DataSets'])
GenDataSet = collections.namedtuple(
    "GenDataSet", ['NumSources', 'DataSize', 'dfSrc'])


# region DoGenData

def nameHash(i):
    return hashlib.sha512(str(i).encode('utf8')).hexdigest()


def line(i, misspelledLetter):
    letter = misspelledLetter
    v = f"""
FFFFFF{letter}{i}_{nameHash(i)},
LLLLLL{letter}{i}_{nameHash(i)},
{i} Main St,Plaineville ME,
{(i-1)%100:05d},
{i},
{i*2 if letter == "A" else ''},
{i*3 if letter == "B" else ''},
{i*5 if letter == "C" else ''},
{i*7 if letter == "D" else ''},
{i*11 if letter == "E" else ''},
{i*13 if letter == "F" else ''}
"""
    v = v.replace("\n", "") + "\n"
    return v


def DoGenData(numPeopleList: List[int], spark: SparkSession, data_params: DedupeDataParameters)->List[GenDataSets]:
    rootPath = data_params.test_data_file_location
    recordAFilename = rootPath + "/FieldA%d.csv"
    recordBFilename = rootPath + "/FieldB%d.csv"
    recordCFilename = rootPath + "/FieldC%d.csv"
    recordDFilename = rootPath + "/FieldD%d.csv"
    recordEFilename = rootPath + "/FieldE%d.csv"
    recordFFilename = rootPath + "/FieldF%d.csv"
    srcDfListList: List[GenDataSets] = []
    for numPeople in numPeopleList:
        if not os.path.isfile(recordFFilename % numPeople):
            with open(recordAFilename % numPeople, "w") as f:
                for i in range(1, numPeople+1):
                    f.write(line(i, 'A'))
            with open(recordBFilename % numPeople, "w") as f:
                for i in range(1, max(1, 2*numPeople//100)+1):
                    f.write(line(i, 'B'))
            with open(recordCFilename % numPeople, "w") as f:
                for i in range(1, numPeople+1):
                    f.write(line(i, 'C'))
            with open(recordDFilename % numPeople, "w") as f:
                for i in range(1, numPeople+1):
                    f.write(line(i, 'D',))
            with open(recordEFilename % numPeople, "w") as f:
                for i in range(1, numPeople+1):
                    f.write(line(i, 'E',))
            with open(recordFFilename % numPeople, "w") as f:
                for i in range(1, numPeople+1):
                    f.write(line(i, 'F',))
        if data_params.CanAssumeNoDupesPerPartition:
            dfA = (spark.read
                   .csv(
                       recordAFilename % numPeople,
                       schema=RecordSparseStruct)
                   .coalesce(1)
                   .withColumn("SourceId", func.lit(0)))
            dfB = (spark.read
                   .csv(
                       recordBFilename % numPeople,
                       schema=RecordSparseStruct)
                   .coalesce(1)
                   .withColumn("SourceId", func.lit(1)))
            dfC = (spark.read
                   .csv(
                       recordCFilename % numPeople,
                       schema=RecordSparseStruct)
                   .coalesce(1)
                   .withColumn("SourceId", func.lit(2)))
            dfD = (spark.read
                   .csv(
                       recordDFilename % numPeople,
                       schema=RecordSparseStruct)
                   .coalesce(1)
                   .withColumn("SourceId", func.lit(3)))
            dfE = (spark.read
                   .csv(
                       recordEFilename % numPeople,
                       schema=RecordSparseStruct)
                   .coalesce(1)
                   .withColumn("SourceId", func.lit(4)))
            dfF = (spark.read
                   .csv(
                       recordFFilename % numPeople,
                       schema=RecordSparseStruct)
                   .coalesce(1)
                   .withColumn("SourceId", func.lit(5)))
        else:
            dfA = (spark.read
                   .csv(
                       recordAFilename % numPeople,
                       schema=RecordSparseStruct)
                   .withColumn("SourceId", func.lit(0)))
            dfB = (spark.read
                   .csv(
                       recordBFilename % numPeople,
                       schema=RecordSparseStruct)
                   .withColumn("SourceId", func.lit(1)))
            dfC = (spark.read
                   .csv(
                       recordCFilename % numPeople,
                       schema=RecordSparseStruct)
                   .withColumn("SourceId", func.lit(2)))
            dfD = (spark.read
                   .csv(
                       recordDFilename % numPeople,
                       schema=RecordSparseStruct)
                   .withColumn("SourceId", func.lit(3)))
            dfE = (spark.read
                   .csv(
                       recordEFilename % numPeople,
                       schema=RecordSparseStruct)
                   .withColumn("SourceId", func.lit(4)))
            dfF = (spark.read
                   .csv(
                       recordFFilename % numPeople,
                       schema=RecordSparseStruct)
                   .withColumn("SourceId", func.lit(5)))
        set1 = dfA.unionAll(dfB)
        set2 = dfA.unionAll(dfB).unionAll(dfC)
        set3 = dfA.unionAll(dfB).unionAll(dfC) \
            .unionAll(dfD).unionAll(dfE).unionAll(dfF)
        if data_params.CanAssumeNoDupesPerPartition is False:  # Scramble
            set1 = set1.repartition(data_params.NumExecutors)
            set2 = set2.repartition(data_params.NumExecutors)
            set3 = set3.repartition(data_params.NumExecutors)
        set1.persist()
        set2.persist()
        set3.persist()
        srcDfListList.append(GenDataSets(numPeople, [
            GenDataSet(2, set1.count(), set1),
            GenDataSet(3, set2.count(), set2),
            GenDataSet(6, set3.count(), set3),]))
    return srcDfListList
# endregion
