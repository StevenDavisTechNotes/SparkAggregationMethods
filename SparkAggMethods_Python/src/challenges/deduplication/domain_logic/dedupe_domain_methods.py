
from typing import TypeVar

import pyspark.sql.functions as func
import pyspark.sql.types as DataTypes
from pyspark.sql import DataFrame as PySparkDataFrame

from src.challenges.deduplication.dedupe_test_data_types import \
    RecordSparseStruct

MatchThreshold = 0.9
# must be 0.4316546762589928 < threshold < 0.9927007299270073 @ 10k

# region Shared


T = TypeVar('T', bound=float | str)


def min_not_null(
        lst: list[T | None]
) -> T | None:
    filtered_list: list[T] = [
        x for x in lst if x is not None]
    return min(filtered_list) \
        if len(filtered_list) > 0 else None


def first_not_null(
        lst: list[T | None]
) -> T | None:
    for x in lst:
        if x is not None:
            return x
    return None
# endregion
# region IsMatch


def is_match(
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


def match_single_name(
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
    match_single_name, MatchSingleName_Returns)
# endregion
# region Shared block processing


def nest_blocks_dataframe(
        df: PySparkDataFrame,
        grouped_num_partitions: int,
) -> PySparkDataFrame:
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


def unnest_blocks_dataframe(
        df: PySparkDataFrame,
) -> PySparkDataFrame:
    df = (
        df
        .select(func.explode(df.MergedItems).alias("Rows"))
        .select(func.col("Rows.*"))
        .drop(func.col("BlockingKey"))
        .drop(func.col("SourceId"))
    )
    return df


def blocking_function(
        x: DataTypes.Row
) -> int:
    return hash((
        int(x.ZipCode), x.FirstName[0], x.LastName[0]))


FindRecordMatches_RecList_Returns = DataTypes.ArrayType(
    DataTypes.StructType([
        DataTypes.StructField(
            'idLeftVertex',
            DataTypes.IntegerType(), False),
        DataTypes.StructField(
            'idRightVertex',
            DataTypes.IntegerType(), False),
    ]))


def find_record_matches_rec_list(
        recordList: list[DataTypes.Row],
) -> list[DataTypes.Row]:
    n = len(recordList)
    edgeList: list[DataTypes.Row] = []
    for i in range(0, n - 1):
        i_row = recordList[i]
        for j in range(i + 1, n):
            j_row = recordList[j]
            if i_row.SourceId == j_row.SourceId:
                continue
            if is_match(
                    i_row.FirstName, j_row.FirstName,
                    i_row.LastName, j_row.LastName,
                    i_row.ZipCode, j_row.ZipCode,
                    i_row.SecretKey, j_row.SecretKey):
                edgeList.append(DataTypes.Row(
                    idLeftVertex=i,
                    idRightVertex=j))
                assert i_row.SecretKey == j_row.SecretKey
                break  # safe if assuming associative and transitive
            else:
                assert i_row.SecretKey != j_row.SecretKey

    return edgeList


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


def find_connected_components_rec_list(
        edgeList: list[DataTypes.Row],
) -> list[DataTypes.Row]:
    # This is not optimal for large components.  See GraphFrame
    componentForVertex: dict[int, set[int]] = dict()
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
    knownComponents: set[int] = set()
    componentList: list[DataTypes.Row] = []
    for vertex in componentForVertex:
        if vertex not in knownComponents:
            component = componentForVertex[vertex]
            componentList.append(
                DataTypes.Row(
                    idEdge=len(componentList),
                    idVertexList=sorted(component)
                ))
            for j_vertex in component:
                knownComponents.add(j_vertex)
    return componentList


MergeItems_RecList_Returns = DataTypes.ArrayType(
    DataTypes.StructType(
        RecordSparseStruct.fields +
        [DataTypes.StructField("SourceId",
                               DataTypes.IntegerType(), False),
         DataTypes.StructField("BlockingKey",
                               DataTypes.StringType(), False),]))


def merge_items_rec_list(
        blockedDataList: list[DataTypes.Row],
        connectedComponentList: list[DataTypes.Row],
) -> list[DataTypes.Row]:
    verticesInAComponent: set[int] = set()
    for component in connectedComponentList:
        verticesInAComponent = verticesInAComponent \
            .union(component.idVertexList)
    returnList: list[DataTypes.Row] = []
    for component in connectedComponentList:
        constituentList: list[DataTypes.Row] = \
            [blockedDataList[i]
             for i in component.idVertexList]
        assert len(constituentList) > 1
        returnList.append(combine_row_list(constituentList))
    for idx, rec in enumerate(blockedDataList):
        if idx in verticesInAComponent:
            continue
        returnList.append(rec)
    return returnList


def combine_row_list(
        constituentList: list[DataTypes.Row],
) -> DataTypes.Row:
    mutableRec: dict[str, str | int | None] = constituentList[0].asDict()
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
        min_not_null([x.FieldA for x in constituentList])
    mutableRec['FieldB'] = \
        min_not_null([x.FieldB for x in constituentList])
    mutableRec['FieldC'] = \
        min_not_null([x.FieldC for x in constituentList])
    mutableRec['FieldD'] = \
        min_not_null([x.FieldD for x in constituentList])
    mutableRec['FieldE'] = \
        min_not_null([x.FieldE for x in constituentList])
    mutableRec['FieldF'] = \
        min_not_null([x.FieldF for x in constituentList])
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


def single_pass_rec_list(
        blockedData: list[DataTypes.Row],
) -> list[DataTypes.Row]:
    firstOrderEdges = find_record_matches_rec_list(blockedData)
    connectedComponents = find_connected_components_rec_list(firstOrderEdges)
    firstOrderEdges = None
    mergedItems = merge_items_rec_list(blockedData, connectedComponents)
    return mergedItems

# endregion
