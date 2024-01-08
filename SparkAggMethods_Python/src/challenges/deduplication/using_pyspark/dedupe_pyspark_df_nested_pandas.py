import pandas as pd
import pyspark.sql.functions as func
import pyspark.sql.types as DataTypes
from pyspark.sql import DataFrame as spark_DataFrame

from challenges.deduplication.dedupe_test_data_types import (
    DataSet, ExecutionParameters, PysparkPythonPendingAnswerSet,
    RecordSparseStruct)
from challenges.deduplication.domain_logic.dedupe_domain_methods import \
    match_single_name
from utils.spark_helpers import zip_dataframe_with_index
from utils.tidy_spark_session import TidySparkSession


def dedupe_pyspark_df_nested_pandas(
        spark_session: TidySparkSession,
        data_params: ExecutionParameters,
        data_set: DataSet,
) -> PysparkPythonPendingAnswerSet:
    dfSrc = data_set.df
    if data_set.data_size > 50200:
        return PysparkPythonPendingAnswerSet(feasible=False)

    spark = spark_session.spark
    numPartitions = data_params.NumExecutors
    df: spark_DataFrame = zip_dataframe_with_index(dfSrc, spark=spark, colName="RowId")
    df = df.withColumn(
        "BlockingKey",
        func.hash(
            df.ZipCode.cast(DataTypes.IntegerType()),
            func.substring(df.FirstName, 1, 1),
            func.substring(df.LastName, 1, 1)))
    df = df.repartition(numPartitions, df.BlockingKey)

    df = (
        df
        .repartition(data_set.grouped_num_partitions, df.BlockingKey)
        .groupBy(df.BlockingKey)
        .applyInPandas(inner_agg_method, RecordSparseStruct)
    )
    return PysparkPythonPendingAnswerSet(spark_df=df)


def inner_agg_method(
        dfGroup: pd.DataFrame,
) -> pd.DataFrame:
    matched = find_matches(dfGroup)
    connectedComponents = find_components(matched)
    mergedValue = combine_components(dfGroup, connectedComponents)
    return mergedValue


def find_matches(
        df: pd.DataFrame,
) -> pd.DataFrame:
    toMatch = df[['RowId', 'SourceId', 'FirstName',
                  'LastName', 'ZipCode', 'SecretKey']]
    toMatchLeft = (
        toMatch
        .rename(  # pyright: ignore[reportGeneralTypeIssues]
            index=str,
            columns={
                "RowId": "RowIdL",
                "SourceId": "SourceIdL",
                'FirstName': 'FirstNameL',
                'LastName': 'LastNameL',
                'ZipCode': 'ZipCodeL',
                'SecretKey': 'SecretKeyL'
            }))
    toMatchRight = (
        toMatch
        .rename(  # pyright: ignore[reportGeneralTypeIssues]
            index=str,
            columns={
                "RowId": "RowIdR",
                "SourceId": "SourceIdR",
                'FirstName': 'FirstNameR',
                'LastName': 'LastNameR',
                'ZipCode': 'ZipCodeR',
                'SecretKey': 'SecretKeyR'
            }))
    toMatch = None
    matched: pd.DataFrame
    matched = toMatchLeft.assign(key=0).merge(
        toMatchRight.assign(key=0), on="key").drop("key", axis=1)
    toMatchLeft = toMatchRight = None

    def match_pred(x):
        return ((x.RowIdL == x.RowIdR) or (
            (x.SourceIdL != x.SourceIdR) and
            (x.ZipCodeL == x.ZipCodeR) and
            match_single_name(x.FirstNameL, x.FirstNameR, x.SecretKeyL, x.SecretKeyR) and
            match_single_name(x.LastNameL, x.LastNameR, x.SecretKeyL, x.SecretKeyR)))
    matched = matched.loc[matched.apply(match_pred, axis=1)]
    badMatches = matched.loc[matched.SecretKeyL != matched.SecretKeyR]
    if badMatches.size != 0:
        raise Exception(
            f"Bad match in BlockingKey {df['BlockingKey'].iloc[0]}")
    matched = matched.loc[:, ['RowIdL', 'RowIdR']]
    return matched


def find_components(
        matched: pd.DataFrame,
) -> list[str]:
    import networkx
    G1 = networkx.Graph()
    G1.add_edges_from([(x[0], x[1]) for x in matched.values])
    return list(networkx.connected_components(G1))


def combine_components(
        df: pd.DataFrame,
        connectedComponents: list[str],
) -> pd.DataFrame:
    def convert_str_int_to_min(
            column: pd.Series,
    ) -> str | None:
        lst = column.values.tolist()
        lst = [int(x) for x in lst if x is not None]
        value = min(lst) if len(lst) > 0 else None
        value = str(value) if value is not None else None
        return value

    mergedValues = df.head(0)
    for constituentRowIds in connectedComponents:
        members: pd.DataFrame = df.loc[df.RowId.isin(constituentRowIds)]

        valuedNames: pd.DataFrame = members.copy(deep=False)
        valuedNames['sortOrder'] = \
            valuedNames.apply(lambda x: (
                -(
                    (2 if len(x['LastName'] or "") > 0 else 0) +
                    (1 if len(x['FirstName'] or "") > 0 else 0)),
                x['LastName'], x['FirstName']), axis=1)
        bestNameRec = (
            valuedNames
            .sort_values(by='sortOrder', ascending=True)  # pyright: ignore[reportGeneralTypeIssues)]
            .head(1)
            .loc[:, ['FirstName', 'LastName']]
        )

        valuedAddresses = members.copy(deep=False)
        valuedAddresses['sortOrder'] = \
            valuedAddresses.apply(lambda x: (
                -(
                    (1 if len(x['StreetAddress'] or "") > 0 else 0) +
                    (2 if len(x['City'] or "") > 0 else 0) +
                    (4 if len(x['ZipCode'] or "") > 0 else 0)),
                x['LastName'], x['FirstName']), axis=1)
        bestAddressRec = (
            valuedNames
            .sort_values(by='sortOrder', ascending=True)  # pyright: ignore[reportGeneralTypeIssues)]
            .head(1)
            .loc[:, ['StreetAddress', 'City', 'ZipCode']]
        )

        aggRec = bestNameRec \
            .join(bestAddressRec)
        aggRec['RowId'] = members.RowId.min()
        aggRec['SecretKey'] = members.SecretKey.max()
        # would love to use DataFrame.aggregate, but nullable ints are not supported
        aggRec['FieldA'] = convert_str_int_to_min(members.FieldA)
        aggRec['FieldB'] = convert_str_int_to_min(members.FieldB)
        aggRec['FieldC'] = convert_str_int_to_min(members.FieldC)
        aggRec['FieldD'] = convert_str_int_to_min(members.FieldD)
        aggRec['FieldE'] = convert_str_int_to_min(members.FieldE)
        aggRec['FieldF'] = convert_str_int_to_min(members.FieldF)
        mergedValues = pd.concat([mergedValues, aggRec], sort=False)

    mergedValues = mergedValues \
        .drop(['RowId', 'SourceId', 'BlockingKey'], axis=1) \
        .reset_index(drop=True)
    return mergedValues
