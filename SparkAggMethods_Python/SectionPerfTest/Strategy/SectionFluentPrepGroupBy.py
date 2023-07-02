from typing import List, Tuple

import pyspark.sql.types as DataTypes
from pyspark import RDD
from pyspark.sql import DataFrame as spark_DataFrame
from pyspark.sql import Row

from Utils.SparkUtils import TidySparkSession

from ..SectionLogic import (
    identifySectionUsingIntermediateFile, method_prep_groupby_core, rowToStudentSummary)
from ..SectionRunResult import MaximumProcessableSegment, NumExecutors
from ..SectionTypeDefs import (
    DataSetDescription,
    SparseLineSchema,
    StudentSummary)


def method_prep_groupby(
    spark_session: TidySparkSession, data_set: DataSetDescription
) -> Tuple[List[StudentSummary] | None, RDD | None, spark_DataFrame | None]:
    sc = spark_session.spark_context
    spark = spark_session.spark
    sectionMaximum = data_set.sectionMaximum
    dataSize = data_set.dataSize
    filename = data_set.filename

    SparseLineWithSectionIdLineNoSchema = DataTypes.StructType([
        DataTypes.StructField("SectionId", DataTypes.IntegerType(), True),
        DataTypes.StructField("LineNumber", DataTypes.IntegerType(), True)] +
        SparseLineSchema.fields)

    def parseLineToRowWithLineNo(arg):
        lineNumber = int(arg[1])
        line = arg[0]
        fields = line.split(',')
        sectionId = int(fields[0])
        fields = fields[1:]
        lineType = fields[0]
        if lineType == 'S':
            return Row(
                SectionId=sectionId, LineNumber=lineNumber,
                Type=fields[0],
                StudentId=int(fields[1]), StudentName=fields[2],
                Date=None, WasAbroad=None,
                Dept=None, ClassCredits=None, ClassGrade=None,
                Major=None, TriGPA=None, TriCredits=None)
        if lineType == 'TH':
            return Row(
                SectionId=sectionId, LineNumber=lineNumber,
                Type=fields[0],
                StudentId=None, StudentName=None,
                Date=fields[1], WasAbroad=(fields[2] == 'True'),
                Dept=None, ClassCredits=None, ClassGrade=None,
                Major=None, TriGPA=None, TriCredits=None)
        if lineType == 'C':
            return Row(
                SectionId=sectionId, LineNumber=lineNumber,
                Type=fields[0],
                StudentId=None, StudentName=None,
                Date=None, WasAbroad=None,
                Dept=int(fields[1]), ClassCredits=int(fields[2]), ClassGrade=int(fields[3]),
                Major=None, TriGPA=None, TriCredits=None)
        if lineType == 'TF':
            return Row(
                SectionId=sectionId, LineNumber=lineNumber,
                Type=fields[0],
                StudentId=None, StudentName=None,
                Date=None, WasAbroad=None,
                Dept=None, ClassCredits=None, ClassGrade=None,
                Major=int(fields[1]), TriGPA=float(fields[2]), TriCredits=int(fields[3]))
        raise Exception("Malformed data " + line)
    TargetNumPartitions = max(
        NumExecutors, (dataSize + MaximumProcessableSegment - 1) // MaximumProcessableSegment)
    interFileName = identifySectionUsingIntermediateFile(filename)
    rdd = sc.textFile(interFileName, TargetNumPartitions) \
        .zipWithIndex() \
        .map(parseLineToRowWithLineNo)
    df = spark.createDataFrame(rdd, SparseLineWithSectionIdLineNoSchema)
    df = method_prep_groupby_core(df, sectionMaximum)
    rdd = df.rdd.map(rowToStudentSummary)
    return None, rdd, None
