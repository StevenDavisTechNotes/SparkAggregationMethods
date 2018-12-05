#!python
# conda activate pyspark36
# prompt $g
# set PYSPARK_DRIVER_PYTHON=python
# set PYSPARK_DRIVER_PYTHON_OPTS=
# cls && spark-submit --master local[7,0] --deploy-mode client DedupePerfTest.py
# cls && pyspark --conf spark.sql.shuffle.partitions=100 --conf spark.sql.execution.arrow.enabled=true

import gc
import scipy.stats, numpy
import time
import random
from LinearRegression import linear_regression
from pyspark.sql import SparkSession
from pyspark.storagelevel import StorageLevel
import shutil

spark = None
sc = None
log = None

IsCloudMode = False
if IsCloudMode:
    NumExecutors = 40
    CanAssumeNoDupesPerPartition = False
    DefaultParallelism = 2 * NumExecutors
    SufflePartitions = DefaultParallelism
else:
    NumExecutors = 7
    CanAssumeNoDupesPerPartition = False
    DefaultParallelism = 16
    SufflePartitions = 14
MaximumProcessableSegment = pow(10, 5)

def createSparkContext():
    global spark
    spark = SparkSession \
        .builder \
        .appName("SectionPerfTest") \
        .config("spark.sql.shuffle.partitions", SufflePartitions) \
        .config("spark.default.parallelism", DefaultParallelism) \
        .config("spark.ui.enabled", "false") \
        .config("spark.rdd.compress", "false") \
        .config("spark.worker.cleanup.enabled", "true") \
        .config("spark.python.worker.reuse", "false") \
        .config("spark.port.maxRetries","1") \
        .config("spark.rpc.retry.wait","10s") \
        .config("spark.reducer.maxReqsInFlight","1") \
        .config("spark.network.timeout","30s") \
        .config("spark.shuffle.io.maxRetries","10") \
        .config("spark.shuffle.io.retryWait","60s") \
        .config("spark.sql.execution.arrow.enabled", "true") \
        .enableHiveSupport() \
        .getOrCreate()
        # .config("spark.driver.memory", "2g") \
        # .config("spark.executor.memory", "3g") \
        # .config("spark.executor.memoryOverhead", "1g") \
    return spark
def setupSparkContext(in_spark):
    global spark, sc, log
    spark = in_spark
    sc = spark.sparkContext
    sc.setLogLevel("WARN")
    log4jLogger = sc._jvm.org.apache.log4j
    log = log4jLogger.LogManager.getLogger(__name__)
    log.info("script initialized")
    sc.setCheckpointDir("SectionAggCheckpoint")
    return sc, log

# http://codeliberates.blogspot.com/2008/05/detecting-cpuscores-in-python.html
def detectCPUs():
    """
    Detects the number of CPUs on a system. Cribbed from pp.
    """
    # Linux, Unix and MacOS:
    if hasattr(os, "sysconf"):
        if os.sysconf_names.has_key("SC_NPROCESSORS_ONLN"):
            # Linux & Unix:
            ncpus = os.sysconf("SC_NPROCESSORS_ONLN")
            if isinstance(ncpus, int) and ncpus > 0:
                return ncpus
        else: # OSX:
            return int(os.popen2("sysctl -n hw.ncpu")[1].read())
    # Windows:
    if os.environ.has_key("NUMBER_OF_PROCESSORS"):
            ncpus = int(os.environ["NUMBER_OF_PROCESSORS"]);
            if ncpus > 0:
                return ncpus
    return 1 # Default

import datetime as dt
import random
import re
import collections
import math
import os
import hashlib
import pandas as pd
from itertools import chain
import pyspark.sql.functions as func
import pyspark.sql.types as DataTypes
from pyspark.sql.window import Window
from pyspark.sql import Row
from pyspark.sql.functions import pandas_udf, PandasUDFType

#region data structure
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
#endregion

TestMethod = collections.namedtuple("TestMethod", 
    ["name", "interface", "delegate"])
test_method_list = []
def count_iter(iterator):
    count = 0
    for obj in iterator:
        count += 1
    return count

#region DoGenData
def nameHash(i):
    return hashlib.sha512(str(i).encode('utf8')).hexdigest()
#
GenDataSets = collections.namedtuple("GenDataSets", ['NumPeople', 'DataSets'])
GenDataSet = collections.namedtuple("GenDataSet", ['NumSources', 'DataSize', 'dfSrc'])
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
    v = v.replace("\n","") + "\n"
    return v
#
def DoGenData(numPeopleList):
    if IsCloudMode:
        rootPath = "wasb:///sparkperftesting"
    else:
        rootPath = "e:/temp/sparkperftesting"
    recordAFilename = rootPath + "/FieldA%d.csv"
    recordBFilename = rootPath + "/FieldB%d.csv"
    recordCFilename = rootPath + "/FieldC%d.csv"
    recordDFilename = rootPath + "/FieldD%d.csv"
    recordEFilename = rootPath + "/FieldE%d.csv"
    recordFFilename = rootPath + "/FieldF%d.csv"
    srcDfListList = []
    for numPeople in numPeopleList:
        if not os.path.isfile(recordFFilename%numPeople):
            with open(recordAFilename%numPeople, "w") as f:
                for i in range(1,numPeople+1):
                    f.write(line(i,'A'))
            with open(recordBFilename%numPeople, "w") as f:
                for i in range(1,max(1,2*numPeople//100)+1):
                    f.write(line(i,'B'))
            with open(recordCFilename%numPeople, "w") as f:
                for i in range(1,numPeople+1):
                    f.write(line(i,'C'))
            with open(recordDFilename%numPeople, "w") as f:
                for i in range(1,numPeople+1):
                    f.write(line(i,'D',))
            with open(recordEFilename%numPeople, "w") as f:
                for i in range(1,numPeople+1):
                    f.write(line(i,'E',))
            with open(recordFFilename%numPeople, "w") as f:
                for i in range(1,numPeople+1):
                    f.write(line(i,'F',))
        if CanAssumeNoDupesPerPartition:
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
                    recordCFilename%numPeople, 
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
                    recordCFilename%numPeople, 
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
        if not CanAssumeNoDupesPerPartition:
            set1 = set1.repartition(NumExecutors)
            set2 = set2.repartition(NumExecutors)
            set3 = set3.repartition(NumExecutors)            
        set1.persist()
        set2.persist()
        set3.persist()
        srcDfListList.append(GenDataSets(numPeople, [
            GenDataSet(2, set1.count(), set1),
            GenDataSet(3, set2.count(), set2),
            GenDataSet(6, set3.count(), set3),]))
    return srcDfListList
#endregion

#region Shared
def MinNotNull(lst):
    filteredList = [
        x for x in lst if x is not None]
    return min(filteredList) \
        if len(filteredList) > 0 else None
#
def FirstNotNull(lst):
    for x in lst:
        if x is not None:
            return x
    return None
#
# from https://stackoverflow.com/questions/30304810/dataframe-ified-zipwithindex/32741497#32741497
def dfZipWithIndex (df, offset=1, colName="rowId"):
    '''
        Enumerates dataframe rows is native order, like rdd.ZipWithIndex(), but on a dataframe 
        and preserves a schema

        :param df: source dataframe
        :param offset: adjustment to zipWithIndex()'s index
        :param colName: name of the index column
    '''
    #
    new_schema = DataTypes.StructType(
        [DataTypes.StructField(
            colName, DataTypes.LongType(), True)]
            + df.schema.fields)
    #
    zipped_rdd = df.rdd.zipWithIndex()
    #
    new_rdd = zipped_rdd.map(
        lambda kv: ([kv[1] + offset] + list(kv[0])))
    #
    return spark.createDataFrame(new_rdd, new_schema)
#
MatchThreshold = 0.9
# must be 0.4316546762589928 < threshold < 0.9927007299270073 @ 10k
def IsMatch(iFirstName, jFirstName, iLastName, jLastName, iZipCode, jZipCode, iSecretKey, jSecretKey):
    from difflib import SequenceMatcher
    # if iZipCode != jZipCode:
    #     if iSecretKey == jSecretKey:
    #         raise Exception(f"ZipCode non-match for {iSecretKey} with itself: {iZipCode} {jZipCode}")
    #     return False
    actualRatioFirstName = SequenceMatcher(
        None, iFirstName, jFirstName) \
        .ratio()
    if actualRatioFirstName < MatchThreshold:
        if iSecretKey == jSecretKey:
            raise Exception(f"FirstName non-match for {iSecretKey} with itself {iFirstName} {jFirstName} actualRatioFirstName={actualRatioFirstName}")
        return False
    actualRatioLastName = SequenceMatcher(
        None, iLastName, jLastName) \
        .ratio()
    if actualRatioLastName < MatchThreshold:
        if iSecretKey == jSecretKey:
            raise Exception(f"LastName non-match for {iSecretKey} with itself {iLastName} {jLastName} with actualRatioLastName={actualRatioLastName}")
        return False
    if iSecretKey != jSecretKey:
        raise Exception(f"""
        False match for {iSecretKey}-{jSecretKey} with itself 
        iFirstName={iFirstName} jFirstName={jFirstName} actualRatioFirstName={actualRatioFirstName} 
        iLastName={iLastName} jLastName={jLastName} actualRatioLastName={actualRatioLastName}""")
    return True
#
MatchSingleName_Returns = DataTypes.BooleanType()
def MatchSingleName(lhs, rhs, iSecretKey, jSecretKey):
    from difflib import SequenceMatcher
    actualRatio = SequenceMatcher(
        None, lhs, rhs) \
        .ratio()
    if actualRatio < MatchThreshold:
        if iSecretKey == jSecretKey:
            raise Exception(
                f"Name non-match for {iSecretKey} with itself {lhs} {rhs} ratio={actualRatio}")
        return False
    return True
udfMatchSingleName = func.udf(
    MatchSingleName, MatchSingleName_Returns)
#
#endregion
#region Shared blockprocessing
def NestBlocksDataframe(df):
    df = df \
        .withColumn("BlockingKey", 
            func.hash(
                df.ZipCode.cast(DataTypes.IntegerType()),
                func.substring(df.FirstName, 1, 1),
                func.substring(df.LastName, 1, 1)))
    df = df \
        .groupBy(df.BlockingKey) \
        .agg(func.collect_list(func.struct(*df.columns)) \
            .alias("BlockedData"))
    return df
#
def UnnestBlocksDataframe(df):
    df = df \
        .select(func.explode(df.MergedItems).alias("Rows")) \
        .select(func.col("Rows.*")) \
        .drop(func.col("BlockingKey")) \
        .drop(func.col("SourceId"))
    return df
#
def BlockingFunction(x):
    return hash((
        int(x.ZipCode), x.FirstName[0], x.LastName[0]))
#
# FindRecordMatches_RecList
FindRecordMatches_RecList_Returns = DataTypes.ArrayType(
    DataTypes.StructType([
        DataTypes.StructField(
            'idLeftVertex', 
            DataTypes.IntegerType(), False),
        DataTypes.StructField(
            'idRightVertex', 
            DataTypes.IntegerType(), False),
    ]))
def FindRecordMatches_RecList(recordList):
    n = len(recordList)
    edgeList = []
    for i in range(0, n-1):
        irow = recordList[i]
        for j in range(i+1, n):
            jrow = recordList[j]
            if irow.SourceId == jrow.SourceId:
                continue
            if IsMatch(
                irow.FirstName, jrow.FirstName, 
                irow.LastName, jrow.LastName, 
                irow.ZipCode, jrow.ZipCode, 
                irow.SecretKey, jrow.SecretKey):
                edgeList.append(Row(
                    idLeftVertex=i, 
                    idRightVertex=j))
                assert irow.SecretKey == jrow.SecretKey
                break # safe if assuming assocative and transative 
            else :
                assert irow.SecretKey != jrow.SecretKey
    #
    return edgeList
#
# FindConnectedComponents_RecList
FindConnectedComponents_RecList_Returns = DataTypes.ArrayType(
    DataTypes.StructType([
        DataTypes.StructField(
            'idEdge', 
                DataTypes.IntegerType(), 
                False),
        DataTypes.StructField(
            'idVertexList', 
            DataTypes.ArrayType(
                DataTypes.IntegerType()), 
                False),
        ]))
def FindConnectedComponents_RecList(edgeList):
    # This is not optimal for large components.  See GraphFrame
    componentForVertex = dict()
    for edge in edgeList:
        newComponent = {edge.idLeftVertex, edge.idRightVertex}
        leftIsKnown = edge.idLeftVertex in componentForVertex
        rightIsKnown = edge.idRightVertex in componentForVertex
        if not leftIsKnown and not rightIsKnown:
            componentForVertex[edge.idLeftVertex] = newComponent
            componentForVertex[edge.idRightVertex] = newComponent
        else:
            if leftIsKnown:
                newComponent = newComponent \
                    .union(componentForVertex[edge.idLeftVertex])
            if rightIsKnown:
                newComponent = newComponent \
                    .union(componentForVertex[edge.idRightVertex])
            for vertex in newComponent:
                componentForVertex[vertex] = newComponent
    knownComponents = set()
    componentList=[]
    for vertex in componentForVertex:
        if vertex not in knownComponents:
            component = componentForVertex[vertex]
            componentList.append(
                DataTypes.Row(
                    idEdge=len(componentList),
                    idVertexList=sorted(component)
                ))
            for jvertex in component:
                knownComponents.add(jvertex)
    return componentList
#
# MergeItems_RecList
MergeItems_RecList_Returns = DataTypes.ArrayType(
    DataTypes.StructType(
        RecordSparseStruct.fields + 
        [DataTypes.StructField("SourceId", 
            DataTypes.IntegerType(), False),
        DataTypes.StructField("BlockingKey", 
            DataTypes.StringType(), False),]))
def MergeItems_RecList(blockedDataList, connectedComponentList):
    verticesInAComponent = set()
    for component in connectedComponentList:
        verticesInAComponent = verticesInAComponent \
            .union(component.idVertexList)
    returnList = []
    for component in connectedComponentList:
        constituentList = \
            [blockedDataList[i]
            for i in component.idVertexList]
        assert len(constituentList) > 1
        returnList.append(CombineRowList(constituentList))
    for idx, rec in enumerate(blockedDataList):
        if idx in verticesInAComponent:
            continue
        returnList.append(rec)
    return returnList
#
def CombineRowList(constituentList):
    mutableRec = constituentList[0].asDict()
    mutableRec['SourceId'] = None
    bestNumNameParts = 0
    for contributor in constituentList:
        numNameParts = (
        (0 if len(contributor.LastName or "")==0 else 2)+
        (0 if len(contributor.FirstName or "")==0 else 1))
        if ((numNameParts > bestNumNameParts) or
            (mutableRec['LastName'] > contributor.LastName) or
            (mutableRec['FirstName'] > contributor.FirstName)):
            bestNumNameParts = numNameParts
            mutableRec['FirstName'] = contributor.FirstName
            mutableRec['LastName'] = contributor.LastName
    bestNumAddressParts = 0
    for contributor in constituentList:
        numAddressParts = (
        (0 if len(contributor.ZipCode or "")==0 else 4)+
        (0 if len(contributor.City or "")==0 else 2)+
        (0 if len(contributor.StreetAddress or "")==0 else 1))
        if ((numAddressParts > bestNumAddressParts) or
            (mutableRec['LastName'] > contributor.LastName) or
            (mutableRec['FirstName'] > contributor.FirstName)):
            bestNumAddressParts = numAddressParts
            mutableRec['StreetAddress'] = \
                contributor.StreetAddress
            mutableRec['City'] = contributor.City
            mutableRec['ZipCode'] = contributor.ZipCode
    mutableRec['FieldA'] = \
        MinNotNull([x.FieldA for x in constituentList])
    mutableRec['FieldB'] = \
        MinNotNull([x.FieldB for x in constituentList])
    mutableRec['FieldC'] = \
        MinNotNull([x.FieldC for x in constituentList])
    mutableRec['FieldD'] = \
        MinNotNull([x.FieldD for x in constituentList])
    mutableRec['FieldE'] = \
        MinNotNull([x.FieldE for x in constituentList])
    mutableRec['FieldF'] = \
        MinNotNull([x.FieldF for x in constituentList])
    if 'BlockingKey' in mutableRec:
        row = Row(*(
            mutableRec['FirstName'],
            mutableRec['LastName'],
            mutableRec['StreetAddress'],
            mutableRec['City'],
            mutableRec['ZipCode'],
            mutableRec['SecretKey'],
            mutableRec['FieldA'],
            mutableRec['FieldB'],
            mutableRec['FieldC'],
            mutableRec['FieldD'],
            mutableRec['FieldE'],
            mutableRec['FieldF'],
            mutableRec['SourceId'],
            mutableRec['BlockingKey']))
        row.__fields__ = RecordSparseStruct.names + \
            ['SourceId', 'BlockingKey']
    else:
        row = Row(*(
            mutableRec['FirstName'],
            mutableRec['LastName'],
            mutableRec['StreetAddress'],
            mutableRec['City'],
            mutableRec['ZipCode'],
            mutableRec['SecretKey'],
            mutableRec['FieldA'],
            mutableRec['FieldB'],
            mutableRec['FieldC'],
            mutableRec['FieldD'],
            mutableRec['FieldE'],
            mutableRec['FieldF'],
            mutableRec['SourceId']))
        row.__fields__ = RecordSparseStruct.names + \
            ['SourceId']
    return row
#
SinglePass_RecList_DF_Returns = MergeItems_RecList_Returns
def SinglePass_RecList(blockedData):
    firstOrderEdges = FindRecordMatches_RecList(blockedData)
    connectedComponents = FindConnectedComponents_RecList(firstOrderEdges)
    firstOrderEdges = None
    mergedItems = MergeItems_RecList(blockedData, connectedComponents)
    return mergedItems
#
#endregion

#region method_fluent_windows
def method_fluent_windows(dataSize, dfSrc):
    numPartitions = 4 * NumExecutors # cross product
    df = dfZipWithIndex(dfSrc, colName="RowId")
    df = df \
        .withColumn("BlockingKey", 
            func.hash(
                df.ZipCode.cast(DataTypes.IntegerType()),
                func.substring(df.FirstName, 1, 1),
                func.substring(df.LastName, 1, 1)))
    dfBlocked = df \
        .repartition(numPartitions, df.BlockingKey)
    #
    df1 = dfBlocked
    df2 = dfBlocked.select("RowId", "FirstName", "LastName", 
        "BlockingKey", "SecretKey").alias("df2")
    df = df1.alias("df1").join(df2, on="BlockingKey")\
        .filter((func.col("df1.RowId") == func.col("df2.RowId")) | (
        udfMatchSingleName(
            func.col("df1.FirstName"), func.col("df2.FirstName"),
            func.col("df1.SecretKey"), func.col("df2.SecretKey")) &
        udfMatchSingleName(
            func.col("df1.LastName"), func.col("df2.LastName"),
            func.col("df1.SecretKey"), func.col("df2.SecretKey"))))
    df = df \
        .withColumn("ImmediateGroupId", 
            func.least(func.col("df1.RowId"), func.col("df2.RowId")))
    #
    window = Window \
        .partitionBy(df.BlockingKey, func.col("df1.RowId"))
    df = df \
        .withColumn("GroupId", 
            func.min(df.ImmediateGroupId).over(window)) \
        .drop(df.ImmediateGroupId) \
        .filter(func.col("df1.RowId") == func.col("df2.RowId"))
    df = df \
        .select(
            func.col('df1.BlockingKey').alias('BlockingKey'),
            func.col('df1.RowId').alias('RowId'),
            func.col('df1.FirstName').alias('FirstName'),
            func.col('df1.LastName').alias('LastName'),
            func.col('df1.StreetAddress').alias('StreetAddress'),
            func.col('df1.City').alias('City'),
            func.col('df1.ZipCode').alias('ZipCode'),
            func.col('df1.SecretKey').alias('SecretKey'),
            func.col('df1.FieldA').alias('FieldA'),
            func.col('df1.FieldB').alias('FieldB'),
            func.col('df1.FieldC').alias('FieldC'),
            func.col('df1.FieldD').alias('FieldD'),
            func.col('df1.FieldE').alias('FieldE'),
            func.col('df1.FieldF').alias('FieldF'),
            func.col('df1.SourceId').alias('SourceId'),
            df.GroupId) \
        .repartition(numPartitions, df.GroupId)
    df = df \
        .withColumn("NumNames", 
                    func.when(df.FirstName.isNull(),0)
                        .when(func.length(df.FirstName)>0, 1)
                        .otherwise(0) +
                    func.when(df.LastName.isNull(),0)
                        .when(func.length(df.LastName)>0, 2) # precedence
                        .otherwise(0)) \
        .withColumn("NumAddressParts", 
                    func.when(df.StreetAddress.isNull(),0)
                        .when(func.length(df.StreetAddress)>0, 1)
                        .otherwise(0) +
                    func.when(df.City.isNull(),0)
                        .when(func.length(df.City)>0, 1)
                        .otherwise(0) +
                    func.when(df.ZipCode.isNull(),0)
                        .when(func.length(df.ZipCode)>0, 1)
                        .otherwise(0))
    window = Window \
        .partitionBy(df.GroupId) \
        .orderBy(df.NumNames.desc(), df.LastName.asc(), df.FirstName.asc())
    df = df \
        .withColumn("RowIdBestName", func.first(df.RowId).over(window))
    window = Window \
        .partitionBy(df.GroupId) \
        .orderBy(df.NumAddressParts.desc(), df.LastName.asc(), df.FirstName.asc())
    df = df \
        .withColumn("RowIdBestAddr", func.first(df.RowId).over(window))
    df = df \
        .groupBy(df.GroupId) \
        .agg(
            func.max(func.when(df.RowId == df.RowIdBestName, df.FirstName)).alias("FirstName"),
            func.max(func.when(df.RowId == df.RowIdBestName, df.LastName)).alias("LastName"),
            func.max(func.when(df.RowId == df.RowIdBestAddr, df.StreetAddress)).alias("StreetAddress"),
            func.max(func.when(df.RowId == df.RowIdBestAddr, df.City)).alias("City"),
            func.max(func.when(df.RowId == df.RowIdBestAddr, df.ZipCode)).alias("ZipCode"),
            func.max(df.SecretKey).alias("SecretKey"),
            func.min(df.FieldA).alias("FieldA"),
            func.min(df.FieldB).alias("FieldB"),
            func.min(df.FieldC).alias("FieldC"),
            func.min(df.FieldD).alias("FieldD"),
            func.min(df.FieldE).alias("FieldE"),
            func.min(df.FieldF).alias("FieldF")) \
        .drop(df.GroupId)
    return None, df
#
test_method_list.append(TestMethod(
    name='method_fluent_windows', 
    interface='fluent', 
    delegate=method_fluent_windows))
#
#endregion
#region method_fluent_nested_withCol
def method_fluent_nested_withCol(dataSize, df):
    df = NestBlocksDataframe(df)
    df = df \
        .withColumn("FirstOrderEdges", 
                    func.udf(FindRecordMatches_RecList, 
                            FindRecordMatches_RecList_Returns)(
                                df.BlockedData))
    df = df \
        .withColumn("ConnectedComponents", 
                    func.udf(FindConnectedComponents_RecList, 
                            FindConnectedComponents_RecList_Returns)(
                                df.FirstOrderEdges)) \
        .drop(df.FirstOrderEdges)
    df = df \
        .withColumn("MergedItems", 
                    func.udf(MergeItems_RecList, 
                            MergeItems_RecList_Returns)(
                    df.BlockedData, df.ConnectedComponents))
    df = UnnestBlocksDataframe(df)
    return None, df

test_method_list.append(TestMethod(
    name='method_fluent_nested_withCol', 
    interface='fluent', 
    delegate=method_fluent_nested_withCol))
#
#endregion
#region method_fluent_nested_python
def method_fluent_nested_python(dataSize, df):
    df = NestBlocksDataframe(df)
    df = df \
        .withColumn("MergedItems", 
                    func.udf(SinglePass_RecList, 
                            SinglePass_RecList_DF_Returns)(
                                df.BlockedData))
    df = UnnestBlocksDataframe(df)
    return None, df

test_method_list.append(TestMethod(
    name='method_fluent_nested_python', 
    interface='fluent', 
    delegate=method_fluent_nested_python))
#
#endregion
#region method_pandas
def method_pandas(dataSize, dfSrc):
    def findMatches(df):
        toMatch = df[['RowId', 'SourceId', 'FirstName', 'LastName', 'ZipCode', 'SecretKey']]
        toMatchLeft = toMatch \
            .rename(index=str, columns={
                "RowId":"RowIdL", 
                "SourceId":"SourceIdL", 
                'FirstName':'FirstNameL',
                'LastName':'LastNameL',
                'ZipCode':'ZipCodeL',
                'SecretKey':'SecretKeyL'})
        toMatchRight = toMatch \
            .rename(index=str, columns={
                "RowId":"RowIdR", 
                "SourceId":"SourceIdR", 
                'FirstName':'FirstNameR',
                'LastName':'LastNameR',
                'ZipCode':'ZipCodeR',
                'SecretKey':'SecretKeyR'})
        toMatch = None
        matched = toMatchLeft.assign(key=0).merge(toMatchRight.assign(key=0), on="key").drop("key", axis=1)
        toMatchLeft = toMatchRight= None
        matched = matched[matched.apply(lambda x: ((x.RowIdL == x.RowIdR) or (
                    (x.SourceIdL != x.SourceIdR) and
                    (x.ZipCodeL == x.ZipCodeR) and
                    MatchSingleName(x.FirstNameL, x.FirstNameR, x.SecretKeyL, x.SecretKeyR) and
                    MatchSingleName(x.LastNameL, x.LastNameR, x.SecretKeyL, x.SecretKeyR))), axis=1)]
        badMatches = matched[matched.SecretKeyL != matched.SecretKeyR]
        if badMatches.size != 0: 
            raise Exception(f"Bad match in BlockingKey {df['BlockingKey'].iloc[0]}")
        matched = matched[['RowIdL', 'RowIdR']]
        return matched
    #
    def findComponents(matched):
        import networkx
        G1=networkx.Graph()
        G1.add_edges_from([(x[0],x[1]) for x in matched.values])
        return networkx.connected_components(G1)
    #
    def combineComponents(df, connectedComponents):
        def convertStrIntToMin(column):
            lst = column.values.tolist()
            lst = [int(x) for x in lst if x is not None]
            value = MinNotNull(lst)
            value = str(value) if value is not None else None
            return value
        #
        mergedValues = df.head(0)
        for constituentRowIds in connectedComponents:
            # constituentRowIds = list(networkx.connected_components(G1))[0]
            members = df[df.RowId.isin(constituentRowIds)]
            #
            valuedNames = members.copy(deep=False)
            valuedNames['sortOrder'] = \
                valuedNames.apply(lambda x: (
                    -(
                        (2 if len(x['LastName'] or "") > 0 else 0) + 
                        (1 if len(x['FirstName'] or "") > 0 else 0)), 
                x['LastName'], x['FirstName']), axis=1)
            bestNameRec = \
                valuedNames \
                    .sort_values("sortOrder", ascending=True) \
                    .head(1) \
                    [['FirstName', 'LastName']]
            #
            valuedAddresses = members.copy(deep=False)
            valuedAddresses['sortOrder'] = \
                valuedAddresses.apply(lambda x: (
                    -(
                        (1 if len(x['StreetAddress'] or "") > 0 else 0) + 
                        (2 if len(x['City'] or "") > 0 else 0) + 
                        (4 if len(x['ZipCode'] or "") > 0 else 0)), 
                x['LastName'], x['FirstName']), axis=1)
            bestAddressRec = \
                valuedNames \
                    .sort_values("sortOrder", ascending=True) \
                    .head(1) \
                    [['StreetAddress', 'City', 'ZipCode']]
            #
            aggRec = bestNameRec \
                .join(bestAddressRec)
            aggRec['RowId'] = members.RowId.min()
            aggRec['SecretKey'] = members.SecretKey.max()
            # would love to use DataFrame.aggregate, but nullable ints
            aggRec['FieldA'] = convertStrIntToMin(members.FieldA)
            aggRec['FieldB'] = convertStrIntToMin(members.FieldB)
            aggRec['FieldC'] = convertStrIntToMin(members.FieldC)
            aggRec['FieldD'] = convertStrIntToMin(members.FieldD)
            aggRec['FieldE'] = convertStrIntToMin(members.FieldE)
            aggRec['FieldF'] = convertStrIntToMin(members.FieldF)
            mergedValues = pd.concat([mergedValues, aggRec], sort=False)
        #
        mergedValues = mergedValues \
            .drop(['RowId', 'SourceId', 'BlockingKey'], axis=1) \
            .reset_index(drop=True)
        return mergedValues
    #
    numPartitions = NumExecutors
    df = dfZipWithIndex(dfSrc, colName="RowId")
    df = df \
        .withColumn("BlockingKey", 
            func.hash(
                df.ZipCode.cast(DataTypes.IntegerType()),
                func.substring(df.FirstName, 1, 1),
                func.substring(df.LastName, 1, 1)))
    df = df \
        .repartition(numPartitions, df.BlockingKey)
    @pandas_udf(RecordSparseStruct, PandasUDFType.GROUPED_MAP)
    def inner_agg_method(dfGroup):
        matched = findMatches(dfGroup)
        connectedComponents = findComponents(matched)
        mergedValue = combineComponents(dfGroup, connectedComponents)
        return mergedValue
    df = df \
        .groupby(df.BlockingKey).apply(inner_agg_method)
    return None, df
#
test_method_list.append(TestMethod(
    name='method_pandas', 
    interface='pandas', 
    delegate=method_pandas))
#
#endregion
#region method_rdd_groupby
def method_rdd_groupby(dataSize, dfSrc):
    numPartitions = NumExecutors
    rdd = dfSrc.rdd \
        .groupBy(BlockingFunction, numPartitions) \
        .flatMapValues(lambda iter: 
            SinglePass_RecList(list(iter))) \
        .values()
    return rdd, None
#
test_method_list.append(TestMethod(
    name='method_rdd_groupby', 
    interface='rdd', 
    delegate=method_rdd_groupby))
#
#endregion
#region method_rdd_reduce
def method_rdd_reduce(dataSize, dfSrc):
    def appendRowToListDisjoint(lrows, rrow):
        lrows.append(rrow)
    def appendRowToListMixed(lrows, rrow):
        nInitialLRows = len(lrows) # no need to test for matches in r
        found = False
        for lindex in range(0,nInitialLRows):
            lrow = lrows[lindex]
            if not IsMatch(
                lrow.FirstName, rrow.FirstName, 
                lrow.LastName, rrow.LastName, 
                lrow.ZipCode, rrow.ZipCode, 
                lrow.SecretKey, rrow.SecretKey):
                continue
            lrows[lindex] = CombineRowList([lrow, rrow])
            found = True
            break
        if not found:
            lrows.append(rrow)
        return lrows
    #
    def CombineRowLists(lrows, rrows):
        nInitialLRows = len(lrows) # no need to test for matches in r
        for rindex, rrow in enumerate(rrows):
            found = False
            for lindex in range(0,nInitialLRows):
                lrow = lrows[lindex]
                if not IsMatch(
                    lrow.FirstName, rrow.FirstName, 
                    lrow.LastName, rrow.LastName, 
                    lrow.ZipCode, rrow.ZipCode, 
                    lrow.SecretKey, rrow.SecretKey):
                    continue
                lrows[lindex] = CombineRowList([lrow, rrow])
                found = True
                break
            if not found:
                lrows.append(rrow)
        return lrows
    #
    from itertools import chain
    numPartitions = NumExecutors
    appendRowToList = appendRowToListDisjoint \
        if CanAssumeNoDupesPerPartition \
        else appendRowToListMixed
    rdd = dfSrc.rdd \
        .keyBy(BlockingFunction) \
        .combineByKey(
                lambda x:[x],
                appendRowToList,
                CombineRowLists, 
                numPartitions) \
        .mapPartitionsWithIndex(
            lambda index, iterator: chain.from_iterable(map(
                lambda kv: (x for x in kv[1]), 
                iterator
            ))
        )
    return rdd, None
#
test_method_list.append(TestMethod(
    name='method_rdd_reduce', 
    interface='rdd', 
    delegate=method_rdd_reduce))
#
#endregion
#region method_rdd_mappart
def method_rdd_mappart(dataSize, dfSrc):
    def AddRowToRowList(rows, jrow):
        found = False
        for index, irow in enumerate(rows):
            if not IsMatch(
                irow.FirstName, jrow.FirstName, 
                irow.LastName, jrow.LastName, 
                irow.ZipCode, jrow.ZipCode, 
                irow.SecretKey, jrow.SecretKey):
                continue
            rows[index] = CombineRowList([irow, jrow])
            found = True
            break
        if not found:
            rows.append(jrow)
        return rows
    #
    def core_mappart(iterator):
        store = {}
        for kv in iterator:
            key = kv[0]
            row = kv[1]
            bucket = store[key] if key in store else []
            store[key] = AddRowToRowList(bucket, row)
        for bucket in store.values():
            for row in bucket:
                yield row
    #        
    rdd = dfSrc.rdd \
        .keyBy(BlockingFunction) \
        .partitionBy(NumExecutors) \
        .mapPartitions(core_mappart)
    return rdd, None
#
test_method_list.append(TestMethod(
    name='method_rdd_mappart', 
    interface='rdd', 
    delegate=method_rdd_mappart))
#
#endregion

def arrangeFieldOrder(rec):
    row = Row(*(
        rec.FirstName,
        rec.LastName,
        rec.StreetAddress,
        rec.City,
        rec.ZipCode,
        rec.SecretKey,
        rec.FieldA,
        rec.FieldB,
        rec.FieldC,
        rec.FieldD,
        rec.FieldE,
        rec.FieldF))
    row.__fields__ = RecordSparseStruct.names
    return row
#
def verifyCorrectnessRdd(NumSources, actualNumPeople, dataSize, rdd):
    rdd = rdd \
        .map(arrangeFieldOrder)
    df = spark.createDataFrame(rdd, schema=RecordSparseStruct)
    return verifyCorrectnessDf(NumSources, actualNumPeople, dataSize, df)
#
def verifyCorrectnessDf(NumSources, actualNumPeople, dataSize, df):
    df = df.orderBy(df.FieldA.cast(DataTypes.IntegerType()))
    df.cache()
    #
    try:
        secretKeys = set(x.SecretKey for x in df.select(df.SecretKey).collect())
        expectedSecretKeys = set(range(1,actualNumPeople+1))
        if secretKeys != expectedSecretKeys:
            dmissing = expectedSecretKeys - secretKeys
            dextra = secretKeys - expectedSecretKeys
            raise Exception(f"Missing {dmissing} extra {dextra}")
        #
        count = df.count()
        if count != actualNumPeople:
            raise Exception(f"df.count()({count}) != numPeople({actualNumPeople}) ")
        NumBRecords = max(1,2*actualNumPeople//100)
        for index, row in enumerate(df.toLocalIterator()):
            i = index + 1
            if f"FFFFFFA{i}_{nameHash(i)}" != row.FirstName:
                raise Exception(f"{i}: FFFFFFA{i}_{nameHash(i)} != {row.FirstName}")
            if f'LLLLLLA{i}_{nameHash(i)}' != row.LastName:
                raise Exception(f'{i}: LLLLLLA{i}_{nameHash(i)} != {row.LastName}')
            if f'{i} Main St' != row.StreetAddress:
                raise Exception(f'{i} Main St != row.StreetAddress')
            if 'Plaineville ME' != row.City:
                raise Exception('{i}: Plaineville ME != {row.City}')
            if f'{(i-1)%100:05d}' != row.ZipCode:
                raise Exception(f'{(i-1)%100:05d} != {row.ZipCode}')
            if i != row.SecretKey:
                raise Exception(f'{i}: {i} != SecretKey={row.SecretKey}')
            if f'{i*2}' != row.FieldA:
                raise Exception(f'{i}: {i*2} != FieldA={row.FieldA}')
            #
            if (NumSources < 2) or (i > NumBRecords):
                if row.FieldB is not None:
                    raise Exception("{i}: row.FieldB is not None, NumSources={NumSources}, NumBRecords={NumBRecords}")
            else:
                if f'{i*3}' != row.FieldB:
                    raise Exception(f'{i}: {i*3} != FieldB={row.FieldB}')
            #
            if NumSources < 3:
                if row.FieldC is not None:
                    raise Exception("{i}: row.FieldC is not None, NumSources={NumSources}")
            else:
                if f'{i*5}' != row.FieldC:
                    raise Exception(f'{i}: {i*5} != FieldC={row.FieldC}')
            #
            if NumSources < 4:
                if row.FieldD is not None:
                    raise Exception("{i}: row.FieldD is not None, NumSources={NumSources}")
            else:
                if f'{i*7}' != row.FieldD:
                    raise Exception(f'{i}: {i*7} != FieldD={row.FieldD}')
            #
            if NumSources < 5:
                if row.FieldE is not None:
                    raise Exception("{i}: row.FieldE is not None, NumSources={NumSources}")
            else:
                if f'{i*11}' != row.FieldE:
                    raise Exception(f'{i}: {i*11} != FieldE={row.FieldE}')
            #
            if NumSources < 6:
                if row.FieldF is not None:
                    raise Exception("{i}: row.FieldF is not None, NumSources={NumSources}")
            else:
                if f'{i*13}' != row.FieldF:
                    raise Exception(f'{i}: {i*13} != FieldF={row.FieldF}')
        # for
    except Exception as exception:
        log.exception(exception)
        print("data error")
        return False
    #
    print("Looking Good!")
    return True
#
def count_in_a_partition(idx, iterator):
    yield idx, sum(1 for _ in iterator)
#
def printPartitionDistribution(rddout, dfout):
    print("records per partition ", 
            (rddout or dfout.rdd) \
                .mapPartitionsWithIndex(count_in_a_partition) \
                .collect())
    # .toDF("partition_number","number_of_records") \
#
RunResult = collections.namedtuple("RunResult", ["numSources", "actualNumPeople", "dataSize", "dataSizeExp", "elapsedTime", "foundNumPeople", "IsCloudMode", "CanAssumeNoDupesPerPartition"])
def runtests(srcDfListList):
    NumRunsPer = 30
    #
    test_run_itinerary = []
    for testMethod in test_method_list:
        print("testMethod=", testMethod.name)
        # if testMethod.name in ('method_fluent_nested', 'method_pandas', ):
        #     continue
        # if testMethod.name in ('method_rdd_reduce', ):
        #     continue
        for srcDfList in srcDfListList:
            numPeople = srcDfList.NumPeople
            print("numPeople=", numPeople)
            for sizeObj in srcDfList.DataSets:
                NumSources, dataSize, df = sizeObj.NumSources, sizeObj.DataSize, sizeObj.dfSrc
                # if numPeople > 1000:
                #     continue
                # if dataSize < 50200:
                #     continue
                # if testMethod.name == 'method_fluent_nested_python':
                #     if numPeople
                # if numPeople == 10000 and dataSize == 50200:
                #     continue
                print("dataSize=", dataSize)
                test_run_itinerary.extend((testMethod, (NumSources, numPeople, dataSize, df)) for i in range(0, NumRunsPer))
    random.seed(dt.datetime.now())
    random.shuffle(test_run_itinerary)
    #
    test_runs = {}
    with open('Results/dedupe_runs_w_cloud.csv', 'a') as f:
        f.write(",".join((
            " result", 
            "method", "interface", "numSources", 
            "dataSize", "dataSizeExp", "actualNumPeople", 
            "elapsedTime", "foundNumPeople", 
            "IsCloudMode", "CanAssumeNoDupesPerPartition"))+"\n")
        for index, (test_method, (NumSources, actualNumPeople, dataSize, df)) in enumerate(test_run_itinerary):
            log.info("Working on %d of %d"%(index, len(test_run_itinerary)))
            startedTime = time.time()
            print("Working on %s %d %d"%(test_method.name, actualNumPeople, dataSize))
            # f.write("Working on %s %d %d %d\n"%(test_method.name, actualNumPeople, dataSize, round(math.log10(dataSize))))
            # f.flush()
            try:
                rddout, dfout = test_method.delegate(dataSize, df)
                if rddout is not None:
                    print(f"NumPartitions={rddout.getNumPartitions()}")
                    foundNumPeople = count_iter(rddout.toLocalIterator())
                elif dfout is not None:
                    print(f"NumPartitions={dfout.rdd.getNumPartitions()}")
                    foundNumPeople = count_iter(dfout.toLocalIterator())
                else:
                    raise Exception("not returning anything")
            except Exception as exception:
                rddout = dfout = None
                log.exception(exception)
            elapsedTime = time.time()-startedTime
            # if rddout is not None or dfout is not None:
            #     print("records per partition ", 
            #         (rddout or dfout.rdd) \
            #             .mapPartitionsWithIndex(count_in_a_partition) \
            #             .collect())
            result = RunResult(
                numSources=NumSources,
                actualNumPeople=actualNumPeople,
                dataSize=dataSize,
                dataSizeExp=round(math.log10(dataSize)),
                elapsedTime=elapsedTime,
                foundNumPeople=foundNumPeople,
                IsCloudMode=IsCloudMode,
                CanAssumeNoDupesPerPartition=CanAssumeNoDupesPerPartition)
            success = True
            if foundNumPeople != actualNumPeople:
                failure = True
                f.write("failure,%s,%s,%d,%d,%d,%d,%f,%d\n"%(
                    test_method.name, test_method.interface, 
                    result.numSources, result.dataSize, result.dataSizeExp, result.actualNumPeople, 
                    result.elapsedTime, result.foundNumPeople))
                print("failure,%s,%s,%d,%d,%d,%d,%f,%d\n"%(test_method.name, test_method.interface, result.numSources, result.dataSize, result.dataSizeExp, result.actualNumPeople, result.elapsedTime, result.foundNumPeople))
                continue
            if rddout is not None:
                success = verifyCorrectnessRdd(NumSources, actualNumPeople, dataSize, rddout)
            elif dfout is not None:
                success = verifyCorrectnessDf(NumSources, actualNumPeople, dataSize, dfout)
            else:
                success = False
            if test_method.name not in test_runs:
                test_runs[test_method.name] = []
            test_runs[test_method.name].append(result)
            f.write("%s,%s,%s,%d,%d,%d,%d,%f,%d,%s,%s\n"%(
                "success" if success else "failure", 
                test_method.name, test_method.interface, result.numSources, 
                result.dataSize, result.dataSizeExp, result.actualNumPeople, 
                result.elapsedTime, result.foundNumPeople,
                "TRUE" if result.IsCloudMode else "FALSE", 
                "TRUE" if result.CanAssumeNoDupesPerPartition else "FALSE"))
            f.flush()
            print("Took %f secs"%elapsedTime)
            rdd = None
            gc.collect()
            time.sleep(1)
            print("")
    #
def DoAnalysis():
    test_runs = {}
    with open('Results/dedupe_runs_tail.csv', 'r') as f, \
        open('Results/temp.csv', 'w') as fout:
        for textline in f:
            if textline.startswith("Working"):
                print("Excluding line: "+textline)
                continue
            if textline.startswith("#"):
                print("Excluding line: "+textline)
                continue
            if textline.find(',') < 0:
                print("Excluding line: "+textline)
                continue
            fields = textline.rstrip().split(',')
            # if len(fields) == 6:
            #     fields.append(None)
            test_status, test_method_name, test_method_interface, \
                result_numSources, \
                result_dataSize, result_dataSizeExp, \
                result_actualNumPeople, \
                result_elapsedTime, result_foundNumPeople = tuple(fields)
            if test_status != 'success':
                print("Excluding line: "+textline)
                continue
            result_numSources = int(result_numSources)
            result_dataSize = int(result_dataSize)
            result_dataSizeExp = int(result_dataSizeExp)
            result_actualNumPeople = int(result_actualNumPeople)
            result_elapsedTime = float(result_elapsedTime)
            result_foundNumPeople = int(result_foundNumPeople)
            result = RunResult(
                numSources=result_numSources,
                actualNumPeople=result_actualNumPeople,
                dataSize=result_dataSize,
                dataSizeExp=result_dataSizeExp,
                elapsedTime=result_elapsedTime,
                foundNumPeople=result_foundNumPeople)
            if test_method_name not in test_runs:
                test_runs[test_method_name] = []
            test_runs[test_method_name].append(result)
            fout.write("%s,%s,%d,%d,%d,%d,%f\n"%(
                test_method_name, test_method_interface, 
                result.numSources, result.actualNumPeople,
                result.dataSize, result.dataSizeExp, 
                result.elapsedTime))
    # print(test_runs)
    if len(test_runs) < 1:
        print("no tests")
        return
    if min([len(x) for x in test_runs.values()]) < 10:
        print("not enough data ", [len(x) for x in test_runs.values()])
        return
    TestRegression = collections.namedtuple("TestRegression", 
        ["name", "interface", "run_count",
        "b0", "b0_low", "b0_high",
        "b1", "b1_low", "b1_high",
        "s2", "s2_low", "s2_high"])
    summary_status = ''
    regression_status = ''
    if True:
        test_results = []
        confidence = 0.95
        regression_status += ("%s,%s,%s,"+"%s,%s,%s,"+"%s,%s,%s,"+"%s,%s,%s\n")%(
            'RawMethod', 'interface', 'run_count',
            'b0', 'b0 lo', 'b0 hi',
            'b1M', 'b1M lo', 'b1M hi',
            's2', 's2 lo', 's2 hi')
        summary_status += ("%s,%s,%s,"+"%s,%s,%s,"+"%s,%s\n")% (
            'RawMethod', 'interface', 'run_count',
            'DataSize', 'mean', 'stdev', 
            'rl', 'rh')
        for name in test_runs:
            print("Looking to analyze %s"%name)
            test_method = [x for x in test_method_list if x.name == name][0]
            times = test_runs[name]
            size_values = set(x.dataSize for x in times)
            for dataSize in size_values:
                ar = [x.elapsedTime for x in times if x.dataSize == dataSize]
                mean = numpy.mean(ar)
                stdev = numpy.std(ar, ddof=1)
                rl, rh = scipy.stats.norm.interval(confidence, loc=mean, scale=stdev/math.sqrt(len(ar)))
                summary_status += ("%s,%s,"+"%d,%d,"+"%f,%f,%f,%f\n")%(
                    name, test_method.interface, 
                    len(ar), dataSize, 
                    mean, stdev, rl, rh
                )
            x_values = [math.log10(x.dataSize) for x in times]
            y_values = [math.log10(x.elapsedTime) for x in times]
            (b0, (b0_low, b0_high)), (b1, (b1_low,b1_high)), (s2, (s2_low,s2_high)) = \
                linear_regression(x_values, y_values, confidence)
            result = TestRegression(
                name=test_method.name,
                interface=test_method.interface,
                run_count=len(times),
                b0=b0,
                b0_low=b0_low,
                b0_high=b0_high,
                b1=b1,
                b1_low=b1_low,
                b1_high=b1_high,
                s2=s2,
                s2_low=s2_low,
                s2_high=s2_high
                )
            test_results.append(result)
            regression_status += ("%s,%s,%d,"+"%f,%f,%f,"+"%f,%f,%f,"+"%f,%f,%f\n")%(
                test_method.name, test_method.interface, result.run_count,
                result.b0, result.b0_low, result.b0_high,
                result.b1*1e+6, result.b1_low*1e+6, result.b1_high*1e+6,
                result.s2, result.s2_low, result.s2_high)
    with open('Results/dedupe_results.csv', 'w') as f:
        f.write(summary_status)
        f.write("\n")
        f.write(regression_status)
        f.write("\n")
#
def isNotebook():
    try:
        shell = get_ipython().__class__.__name__
        if shell == 'ZMQInteractiveShell':
            return True   # Jupyter notebook or qtconsole
        elif shell == 'TerminalInteractiveShell':
            return False  # Terminal running IPython
        else:
            return False  # Other type (?)
    except NameError:
        return False      # Probably standard Python interpreter
#
testDataSizes = [10**1,10**2,10**3,10**4,]
# testDataSizes = [10**3,]
# testDataSizes = [10**4,10**5,]
if __name__ == "__main__" and not isNotebook():
    spark = createSparkContext()
    sc, log = setupSparkContext(spark)
    srcDfListList = DoGenData(testDataSizes)
    runtests(srcDfListList)
    # DoAnalysis()
