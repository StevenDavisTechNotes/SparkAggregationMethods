import pyspark.sql.functions as func
import pyspark.sql.types as DataTypes
from pyspark.sql import Row
from pyspark.sql.window import Window

from challenges.sectional.domain_logic.section_data_parsers import (
    parse_line_to_row, row_to_student_summary)
from challenges.sectional.section_record_runs import \
    MAXIMUM_PROCESSABLE_SEGMENT_EXPONENT
from challenges.sectional.section_test_data_types import (
    DataSet, SparseLineSchema, TChallengePendingAnswerPythonPyspark)
from utils.tidy_spark_session import TidySparkSession


def section_join_groupby(
        spark_session: TidySparkSession,
        data_set: DataSet,
) -> TChallengePendingAnswerPythonPyspark:
    if data_set.description.num_students > pow(10, MAXIMUM_PROCESSABLE_SEGMENT_EXPONENT - 1):
        return "infeasible"
    sc = spark_session.spark_context
    spark = spark_session.spark
    sectionMaximum = data_set.data.section_maximum

    rdd = sc.textFile(data_set.data.test_filepath, data_set.data.target_num_partitions)

    NumRows = rdd.count()
    rdd = (
        rdd
        .zipWithIndex()
        .map(lambda x: with_index_column(x[1], parse_line_to_row(x[0]))))
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
        .sort(df.StudentId)
    )
    rdd = df.rdd.map(row_to_student_summary)
    return rdd


def with_index_column(
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
