from typing import List, Tuple

import pyspark.sql.types as DataTypes
from pyspark import RDD
from pyspark.sql import DataFrame as spark_DataFrame
from pyspark.sql import Row
from pyspark.sql.window import Window
import pyspark.sql.functions as func

from Utils.SparkUtils import TidySparkSession

from ..SectionLogic import parseLineToRow, rowToStudentSummary
from ..SectionRunResult import MaximumProcessableSegment, NumExecutors
from ..SectionTypeDefs import (
    DataSetDescription, SparseLineSchema, StudentSummary)


def method_join_groupby(
    spark_session: TidySparkSession, data_set: DataSetDescription
) -> Tuple[List[StudentSummary] | None, RDD | None, spark_DataFrame | None]:
    sc = spark_session.spark_context
    spark = spark_session.spark
    sectionMaximum = data_set.sectionMaximum

    TargetNumPartitions = max(
        NumExecutors, (data_set.dataSize + MaximumProcessableSegment - 1) // MaximumProcessableSegment)
    rdd = sc.textFile(data_set.filename, TargetNumPartitions)

    def withIndexColumn(lineNumber, row):
        return Row(
            LineNumber=lineNumber,
            ClassCredits=row.ClassCredits,
            ClassGrade=row.ClassGrade,
            Date=row.Date,
            Dept=row.Dept,
            Major=row.Major,
            StudentId=row.StudentId,
            StudentName=row.StudentName,
            TriCredits=row.TriCredits,
            TriGPA=row.TriGPA,
            Type=row.Type,
            WasAbroad=row.WasAbroad)
    #
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