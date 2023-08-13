from typing import List, Tuple

import pyspark.sql.types as DataTypes
from pyspark import RDD
from pyspark.sql import DataFrame as spark_DataFrame
from pyspark.sql import Row
from pyspark.sql.window import Window
import pyspark.sql.functions as func

from Utils.TidySparkSession import TidySparkSession

from SectionPerfTest.SectionLogic import parseLineToRow, rowToStudentSummary
from SectionPerfTest.SectionTypeDefs import (
    DataSet, SparseLineSchema, StudentSummary)


def section_join_groupby(
        spark_session: TidySparkSession,
        data_set: DataSet,
) -> Tuple[List[StudentSummary] | None, RDD | None, spark_DataFrame | None]:
    sc = spark_session.spark_context
    spark = spark_session.spark
    sectionMaximum = data_set.data.section_maximum

    rdd = sc.textFile(data_set.data.test_filepath, data_set.data.target_num_partitions)

    NumRows = rdd.count()
    rdd = (
        rdd
        .zipWithIndex()
        .map(lambda x: withIndexColumn(x[1], parseLineToRow(x[0]))))
    SparseLineWithLineNoSchema = DataTypes.StructType([
        DataTypes.StructField("LineNumber", DataTypes.IntegerType(), True)] +
        SparseLineSchema.fields)
    df = spark.createDataFrame(rdd, SparseLineWithLineNoSchema)
    dfStudentHeaders = (
        df
        .filter(df.StudentId.isNotNull()))
    window = (
        Window
        .orderBy(df.LineNumber)
        .rowsBetween(1, 1))
    dfStudentHeaders = (
        dfStudentHeaders
        .withColumn("FirstSHLineNumber", dfStudentHeaders.LineNumber)
        .withColumn("NextSHLineNumber", func.lead(dfStudentHeaders.LineNumber).over(window))
        .select('StudentId', 'StudentName', 'FirstSHLineNumber', 'NextSHLineNumber'))
    dfStudentHeaders = (
        dfStudentHeaders
        .na.fill({"NextSHLineNumber": NumRows}))
    df = (
        df
        .drop('StudentId', 'StudentName')
        .join(dfStudentHeaders,
              (dfStudentHeaders.FirstSHLineNumber <= df.LineNumber) &
              (dfStudentHeaders.NextSHLineNumber > df.LineNumber)
              )
        .drop('FirstSHLineNumber', 'NextSHLineNumber'))
    window = (
        Window
        .partitionBy(df.StudentId)
        .orderBy(df.LineNumber)
        .rowsBetween(-sectionMaximum, sectionMaximum))
    df = (
        df
        .withColumn('LastMajor', func.last(df.Major).over(window)))
    df = (
        df
        .groupBy(df.StudentId, df.Dept)
        .agg(
            func.max(df.StudentName).alias('StudentName'),
            func.count(df.LineNumber).alias('SourceLines'),
            func.first(df.LastMajor).alias('LastMajor'),
            func.sum(df.ClassCredits).alias('DeptCredits'),
            func.sum(df.ClassCredits *
                     df.ClassGrade).alias('DeptWeightedGradeTotal')
        ))
    df = (
        df
        .groupBy(df.StudentId)
        .agg(
            func.max(df.StudentName).alias('StudentName'),
            func.sum(df.SourceLines).alias('SourceLines'),
            func.first(df.LastMajor).alias('Major'),
            func.sum(df.DeptCredits).alias('TotalCredits'),
            func.sum(df.DeptWeightedGradeTotal).alias('WeightedGradeTotal'),
            func.sum(func.when(df.Dept == df.LastMajor, df.DeptCredits)).alias('MajorCredits'),
            func.sum(func.when(df.Dept == df.LastMajor, df.DeptWeightedGradeTotal)).alias(
                'MajorWeightedGradeTotal')
        ))
    df = (
        df
        .fillna({'MajorCredits': 0, 'MajorWeightedGradeTotal': 0}))
    df = (
        df
        .withColumn(
            'GPA',
            df.WeightedGradeTotal
            / func.when(df.TotalCredits > 0, df.TotalCredits).otherwise(1))
        .drop(df.WeightedGradeTotal)
        .drop(df.TotalCredits)
        .withColumn(
            'MajorGPA',
            df.MajorWeightedGradeTotal
            / func.when(df.MajorCredits > 0, df.MajorCredits).otherwise(1))
        .drop(df.MajorWeightedGradeTotal)
        .drop(df.MajorCredits)
    )
    rdd = df.rdd.map(rowToStudentSummary)
    return None, rdd, None


def withIndexColumn(
        lineNumber: int,
        row: Row,
) -> Row:
    # Warning, order of fields must match SparseLineSchema
    return Row(
        LineNumber=lineNumber,
        Type=row.Type,
        StudentId=row.StudentId,
        StudentName=row.StudentName,
        Date=row.Date,
        WasAbroad=row.WasAbroad,
        Dept=row.Dept,
        ClassCredits=row.ClassCredits,
        ClassGrade=row.ClassGrade,
        Major=row.Major,
        TriGPA=row.TriGPA,
        TriCredits=row.TriCredits,
    )
