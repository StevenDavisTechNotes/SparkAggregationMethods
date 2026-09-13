import pyspark.sql.functions as func
from pyspark.sql import DataFrame as PySparkDataFrame
from pyspark.sql.window import Window

from src.challenges.sectional.section_test_data_types_pyspark import (
    SectionExecutionParametersPyspark,
)


def section_pyspark_rdd_prep_shared(
        dfSrc: PySparkDataFrame,
        exec_params: SectionExecutionParametersPyspark,
        sectionMaximum: int,
) -> PySparkDataFrame:
    df = dfSrc
    window = (
        Window
        .partitionBy(df.SectionId)
        .orderBy(df.LineNumber)
        .rowsBetween(-sectionMaximum, sectionMaximum))
    df = df \
        .withColumn('LastMajor', func.last(df.Major).over(window))
    df = (
        df
        .groupBy(df.SectionId, df.Dept)
        .agg(
            func.max(df.StudentId).alias('StudentId'),
            func.max(df.StudentName).alias('StudentName'),
            func.count(df.LineNumber).alias('SourceLines'),
            func.first(df.LastMajor).alias('LastMajor'),
            func.sum(df.ClassCredits).alias('DeptCredits'),
            func.sum(df.ClassCredits *
                     df.ClassGrade).alias('DeptWeightedGradeTotal')
        ))
    df = (
        df
        .groupBy(df.SectionId)
        .agg(
            func.max(df.StudentId).alias('StudentId'),
            func.max(df.StudentName).alias('StudentName'),
            func.sum(df.SourceLines).alias('SourceLines'),
            func.first(df.LastMajor).alias('Major'),
            func.sum(df.DeptCredits).alias('TotalCredits'),
            func.sum(df.DeptWeightedGradeTotal).alias('WeightedGradeTotal'),
            func.sum(func.when(df.Dept == df.LastMajor,
                     df.DeptCredits)).alias('MajorCredits'),
            func.sum(func.when(df.Dept == df.LastMajor, df.DeptWeightedGradeTotal)).alias(
                'MajorWeightedGradeTotal')
        ))
    df = df \
        .fillna({'MajorCredits': 0, 'MajorWeightedGradeTotal': 0})
    df = (
        df
        .drop(df.SectionId)
        .withColumn('GPA', df.WeightedGradeTotal / func.when(df.TotalCredits > 0, df.TotalCredits).otherwise(1))
        .drop(df.WeightedGradeTotal)
        .drop(df.TotalCredits)
        .withColumn('MajorGPA',
                    df.MajorWeightedGradeTotal
                    / func.when(df.MajorCredits > 0,
                                df.MajorCredits).otherwise(1))
        .drop(df.MajorWeightedGradeTotal)
        .drop(df.MajorCredits)
        .sort(df.StudentId)
    )
    return df
