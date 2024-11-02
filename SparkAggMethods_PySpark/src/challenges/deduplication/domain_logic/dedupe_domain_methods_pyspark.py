import pyspark.sql.functions as func
import pyspark.sql.types as DataTypes
from pyspark.sql import DataFrame as PySparkDataFrame
from spark_agg_methods_common_python.challenges.deduplication.domain_logic.dedupe_domain_methods import (
    is_match, match_single_name, min_not_null,
)

from src.challenges.deduplication.dedupe_test_data_types_pyspark import RecordSparseStruct

MATCH_THRESHOLD = 0.9
# must be 0.4316546762589928 < MATCH_THRESHOLD < 0.9927007299270073 @ 10k


MatchSingleName_Returns = DataTypes.BooleanType()

udfMatchSingleName = func.udf(
    match_single_name, MatchSingleName_Returns)


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
