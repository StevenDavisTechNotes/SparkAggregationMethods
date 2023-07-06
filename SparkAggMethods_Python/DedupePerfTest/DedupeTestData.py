import collections
import hashlib
import os
from dataclasses import dataclass
from typing import List, Optional

import pyspark.sql.functions as func
import pyspark.sql.types as DataTypes
from pyspark.sql import SparkSession
from .DedupeDataTypes import ExecutionParameters, RecordSparseStruct, DataSetOfSizeOfSources, DataSetsOfSize

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


def DoGenData(
    numPeopleList: List[int],
    spark: SparkSession,
    data_params: ExecutionParameters
) -> List[DataSetsOfSize]:
    rootPath = data_params.test_data_file_location
    recordAFilename = rootPath + "/Dedupe_FieldA%d.csv"
    recordBFilename = rootPath + "/Dedupe_FieldB%d.csv"
    recordCFilename = rootPath + "/Dedupe_FieldC%d.csv"
    recordDFilename = rootPath + "/Dedupe_FieldD%d.csv"
    recordEFilename = rootPath + "/Dedupe_FieldE%d.csv"
    recordFFilename = rootPath + "/Dedupe_FieldF%d.csv"
    srcDfListList: List[DataSetsOfSize] = []
    for numPeople in numPeopleList:
        if not os.path.isfile(recordFFilename % numPeople):
            with open(recordAFilename % numPeople, "w") as f:
                for i in range(1, numPeople + 1):
                    f.write(line(i, 'A'))
            with open(recordBFilename % numPeople, "w") as f:
                for i in range(1, max(1, 2 * numPeople // 100) + 1):
                    f.write(line(i, 'B'))
            with open(recordCFilename % numPeople, "w") as f:
                for i in range(1, numPeople + 1):
                    f.write(line(i, 'C'))
            with open(recordDFilename % numPeople, "w") as f:
                for i in range(1, numPeople + 1):
                    f.write(line(i, 'D',))
            with open(recordEFilename % numPeople, "w") as f:
                for i in range(1, numPeople + 1):
                    f.write(line(i, 'E',))
            with open(recordFFilename % numPeople, "w") as f:
                for i in range(1, numPeople + 1):
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
        data_set_2_sources = dfA.unionAll(dfB)
        data_set_3_sources = dfA.unionAll(dfB).unionAll(dfC)
        data_set_6_sources = dfA.unionAll(dfB).unionAll(dfC) \
            .unionAll(dfD).unionAll(dfE).unionAll(dfF)
        if data_params.CanAssumeNoDupesPerPartition is False:  # Scramble
            data_set_2_sources = data_set_2_sources.repartition(data_params.NumExecutors)
            data_set_3_sources = data_set_3_sources.repartition(data_params.NumExecutors)
            data_set_6_sources = data_set_6_sources.repartition(data_params.NumExecutors)
        data_set_2_sources.persist()
        data_set_3_sources.persist()
        data_set_6_sources.persist()
        srcDfListList.append(DataSetsOfSize(numPeople, [
            DataSetOfSizeOfSources(2, data_set_2_sources.count(), data_set_2_sources),
            DataSetOfSizeOfSources(3, data_set_3_sources.count(), data_set_3_sources),
            DataSetOfSizeOfSources(6, data_set_6_sources.count(), data_set_6_sources),]))
    return srcDfListList
# endregion
