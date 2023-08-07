
from typing import Dict, List, Optional, TypeVar, Union

import pyspark.sql.functions as func
import pyspark.sql.types as DataTypes
from pyspark.sql import DataFrame as spark_DataFrame

from DedupePerfTest.DedupeDataTypes import RecordSparseStruct

MatchThreshold = 0.9
# must be 0.4316546762589928 < threshold < 0.9927007299270073 @ 10k

# region Shared


T = TypeVar('T', bound=Union[float, str])


def MinNotNull(
        lst: List[Optional[T]]
) -> Optional[T]:
    filteredList: List[T] = [
        x for x in lst if x is not None]
    return min(filteredList) \
        if len(filteredList) > 0 else None


def FirstNotNull(
        lst: List[Optional[T]]
) -> Optional[T]:
    for x in lst:
        if x is not None:
            return x
    return None
# endregion
# region IsMatch


def IsMatch(
        iFirstName: str,
        jFirstName: str,
        iLastName: str,
        jLastName: str,
        iZipCode: str,
        jZipCode: str,
        iSecretKey: int,
        jSecretKey: int,
) -> bool:
    from difflib import SequenceMatcher
    actualRatioFirstName = SequenceMatcher(
        None, iFirstName, jFirstName) \
        .ratio()
    if actualRatioFirstName < MatchThreshold:
        if iSecretKey == jSecretKey:
            raise Exception(
                "FirstName non-match "
                f"for {iSecretKey} with itself "
                f"{iFirstName} {jFirstName} with "
                f"actualRatioFirstName={actualRatioFirstName}")
        return False
    actualRatioLastName = SequenceMatcher(
        None, iLastName, jLastName) \
        .ratio()
    if actualRatioLastName < MatchThreshold:
        if iSecretKey == jSecretKey:
            raise Exception(
                "LastName non-match "
                f"for {iSecretKey} with itself "
                f"{iLastName} {jLastName} with "
                f"actualRatioLastName={actualRatioLastName}")
        return False
    if iSecretKey != jSecretKey:
        raise Exception(f"""
        False match for {iSecretKey}-{jSecretKey} with itself
        iFirstName={iFirstName} jFirstName={jFirstName} actualRatioFirstName={actualRatioFirstName}
        iLastName={iLastName} jLastName={jLastName} actualRatioLastName={actualRatioLastName}""")
    return True


def MatchSingleName(
        lhs: str,
        rhs: str,
        iSecretKey: int,
        jSecretKey: int,
) -> bool:
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


MatchSingleName_Returns = DataTypes.BooleanType()

udfMatchSingleName = func.udf(
    MatchSingleName, MatchSingleName_Returns)
# endregion
# region Shared blockprocessing


def NestBlocksDataframe(
        df: spark_DataFrame,
        grouped_num_partitions: int,
) -> spark_DataFrame:
    df = df \
        .withColumn("BlockingKey",
                    func.hash(
                        df.ZipCode.cast(DataTypes.IntegerType()),
                        func.substring(df.FirstName, 1, 1),
                        func.substring(df.LastName, 1, 1)))
    df = (
        df
        .repartition(grouped_num_partitions, df.BlockingKey)
        .groupBy(df.BlockingKey)
        .agg(func.collect_list(func.struct(*df.columns))
             .alias("BlockedData"))
    )
    return df


def UnnestBlocksDataframe(
        df: spark_DataFrame,
) -> spark_DataFrame:
    df = (
        df
        .select(func.explode(df.MergedItems).alias("Rows"))
        .select(func.col("Rows.*"))
        .drop(func.col("BlockingKey"))
        .drop(func.col("SourceId"))
    )
    return df


def BlockingFunction(
        x: DataTypes.Row
) -> int:
    return hash((
        int(x.ZipCode), x.FirstName[0], x.LastName[0]))


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


def FindRecordMatches_RecList(
        recordList: List[DataTypes.Row],
) -> List[DataTypes.Row]:
    n = len(recordList)
    edgeList = []
    for i in range(0, n - 1):
        irow = recordList[i]
        for j in range(i + 1, n):
            jrow = recordList[j]
            if irow.SourceId == jrow.SourceId:
                continue
            if IsMatch(
                    irow.FirstName, jrow.FirstName,
                    irow.LastName, jrow.LastName,
                    irow.ZipCode, jrow.ZipCode,
                    irow.SecretKey, jrow.SecretKey):
                edgeList.append(DataTypes.Row(
                    idLeftVertex=i,
                    idRightVertex=j))
                assert irow.SecretKey == jrow.SecretKey
                break  # safe if assuming assocative and transative
            else:
                assert irow.SecretKey != jrow.SecretKey

    return edgeList


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


def FindConnectedComponents_RecList(
        edgeList: List,
) -> List[DataTypes.Row]:
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
    componentList = []
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


# MergeItems_RecList
MergeItems_RecList_Returns = DataTypes.ArrayType(
    DataTypes.StructType(
        RecordSparseStruct.fields +
        [DataTypes.StructField("SourceId",
                               DataTypes.IntegerType(), False),
         DataTypes.StructField("BlockingKey",
                               DataTypes.StringType(), False),]))


def MergeItems_RecList(
        blockedDataList: List[DataTypes.Row],
        connectedComponentList: List[DataTypes.Row],
) -> List[DataTypes.Row]:
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


def CombineRowList(
        constituentList: List[DataTypes.Row],
) -> DataTypes.Row:
    mutableRec: Dict[str, Union[str, int, None]] = constituentList[0].asDict()
    mutableRec['SourceId'] = None
    bestNumNameParts = 0
    for contributor in constituentList:
        numNameParts = (
            (0 if len(contributor.LastName or "") == 0 else 2) +
            (0 if len(contributor.FirstName or "") == 0 else 1))
        if ((numNameParts > bestNumNameParts) or
            (mutableRec['LastName'] > contributor.LastName) or
                (mutableRec['FirstName'] > contributor.FirstName)):
            bestNumNameParts = numNameParts
            mutableRec['FirstName'] = contributor.FirstName
            mutableRec['LastName'] = contributor.LastName
    bestNumAddressParts = 0
    for contributor in constituentList:
        numAddressParts = (
            (0 if len(contributor.ZipCode or "") == 0 else 4) +
            (0 if len(contributor.City or "") == 0 else 2) +
            (0 if len(contributor.StreetAddress or "") == 0 else 1))
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
        row = DataTypes.Row(*(
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
            mutableRec['BlockingKey']))  # type: ignore
        row.__fields__ = RecordSparseStruct.names + \
            ['SourceId', 'BlockingKey']
    else:
        row = DataTypes.Row(*(
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
            mutableRec['SourceId']))  # type: ignore
        row.__fields__ = RecordSparseStruct.names + \
            ['SourceId']
    return row


SinglePass_RecList_DF_Returns = MergeItems_RecList_Returns


def SinglePass_RecList(
        blockedData: List[DataTypes.Row],
) -> List[DataTypes.Row]:
    firstOrderEdges = FindRecordMatches_RecList(blockedData)
    connectedComponents = FindConnectedComponents_RecList(firstOrderEdges)
    firstOrderEdges = None
    mergedItems = MergeItems_RecList(blockedData, connectedComponents)
    return mergedItems

# endregion
