from typing import List

import pandas as pd
import pyspark.sql.functions as func
import pyspark.sql.types as DataTypes
from pyspark.sql import DataFrame as spark_DataFrame

from Utils.SparkUtils import TidySparkSession, dfZipWithIndex

from ..DedupeDomain import MatchSingleName, MinNotNull
from ..DedupeDataTypes import DataSetOfSizeOfSources, ExecutionParameters, RecordSparseStruct

# region method_pandas


def method_pandas(
    spark_session: TidySparkSession,
    data_params: ExecutionParameters,
    data_set: DataSetOfSizeOfSources,
):
    dfSrc = data_set.df

    def findMatches(df: pd.DataFrame) -> pd.DataFrame:
        toMatch = df[['RowId', 'SourceId', 'FirstName',
                      'LastName', 'ZipCode', 'SecretKey']]
        toMatchLeft = toMatch \
            .rename(index=str, columns={
                "RowId": "RowIdL",
                "SourceId": "SourceIdL",
                'FirstName': 'FirstNameL',
                'LastName': 'LastNameL',
                'ZipCode': 'ZipCodeL',
                'SecretKey': 'SecretKeyL'})
        toMatchRight = toMatch \
            .rename(index=str, columns={
                "RowId": "RowIdR",
                "SourceId": "SourceIdR",
                'FirstName': 'FirstNameR',
                'LastName': 'LastNameR',
                'ZipCode': 'ZipCodeR',
                'SecretKey': 'SecretKeyR'})
        toMatch = None
        matched = toMatchLeft.assign(key=0).merge(
            toMatchRight.assign(key=0), on="key").drop("key", axis=1)
        toMatchLeft = toMatchRight = None
        matched = matched[
            matched.apply(lambda x: ((x.RowIdL == x.RowIdR) or (
                (x.SourceIdL != x.SourceIdR) and
                (x.ZipCodeL == x.ZipCodeR) and
                MatchSingleName(x.FirstNameL, x.FirstNameR, x.SecretKeyL, x.SecretKeyR) and
                MatchSingleName(x.LastNameL, x.LastNameR, x.SecretKeyL, x.SecretKeyR))), axis=1)]
        badMatches = matched[matched.SecretKeyL != matched.SecretKeyR]
        if badMatches.size != 0:
            raise Exception(
                f"Bad match in BlockingKey {df['BlockingKey'].iloc[0]}")
        matched = matched[['RowIdL', 'RowIdR']]
        return matched

    def findComponents(matched: pd.DataFrame) -> List[str]:
        import networkx
        G1 = networkx.Graph()
        G1.add_edges_from([(x[0], x[1]) for x in matched.values])
        return list(networkx.connected_components(G1))

    def combineComponents(df: pd.DataFrame, connectedComponents: List[str]):
        def convertStrIntToMin(column: pd.Series) -> str | None:
            lst = column.values.tolist()
            lst = [int(x) for x in lst if x is not None]
            value = MinNotNull(lst)
            value = str(value) if value is not None else None
            return value

        mergedValues = df.head(0)
        for constituentRowIds in connectedComponents:
            # constituentRowIds = list(networkx.connected_components(G1))[0]
            members = df[df.RowId.isin(constituentRowIds)]

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
                .head(1)[['FirstName', 'LastName']]

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
                .head(1)[['StreetAddress', 'City', 'ZipCode']]

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

        mergedValues = mergedValues \
            .drop(['RowId', 'SourceId', 'BlockingKey'], axis=1) \
            .reset_index(drop=True)
        return mergedValues

    spark = spark_session.spark
    numPartitions = data_params.NumExecutors
    df: spark_DataFrame = dfZipWithIndex(dfSrc, spark=spark, colName="RowId")
    df = (
        df
        .withColumn(
            "BlockingKey",
            func.hash(
                df.ZipCode.cast(DataTypes.IntegerType()),
                func.substring(df.FirstName, 1, 1),
                func.substring(df.LastName, 1, 1)))
        .repartition(numPartitions, df.BlockingKey))

    def inner_agg_method(dfGroup: pd.DataFrame) -> pd.DataFrame:
        matched = findMatches(dfGroup)
        connectedComponents = findComponents(matched)
        mergedValue = combineComponents(dfGroup, connectedComponents)
        return mergedValue
    df = (
        df
        .groupby(df.BlockingKey)
        .applyInPandas(inner_agg_method, RecordSparseStruct)
    )
    return None, df


# endregion
