import pyspark.sql.types as DataTypes
from pyspark.sql import Row
from spark_agg_methods_common_python.challenges.sectional.domain_logic.section_data_parsers import (
    identify_section_using_intermediate_file,
)

from src.challenges.sectional.domain_logic.section_data_parsers_pyspark import (
    row_to_student_summary,
)
from src.challenges.sectional.section_test_data_types_pyspark import (
    SectionDataSetPyspark, SectionExecutionParametersPyspark, SparseLineSchema,
    TChallengePythonPysparkAnswer,
)
from src.challenges.sectional.strategies.using_pyspark.section_pyspark_rdd_prep_shared import (
    section_pyspark_rdd_prep_shared,
)
from src.utils.tidy_session_pyspark import TidySparkSession


def section_pyspark_df_prep_txt(
        spark_session: TidySparkSession,
        exec_params: SectionExecutionParametersPyspark,
        data_set: SectionDataSetPyspark,
) -> TChallengePythonPysparkAnswer:
    if data_set.data_description.num_students > pow(10, 8-1):
        return "infeasible", "Times out"
    sc = spark_session.spark_context
    spark = spark_session.spark
    section_maximum = data_set.section_maximum
    filename = data_set.source_data_file_path

    SparseLineWithSectionIdLineNoSchema = DataTypes.StructType([
        DataTypes.StructField("SectionId", DataTypes.IntegerType(), True),
        DataTypes.StructField("LineNumber", DataTypes.IntegerType(), True)] +
        SparseLineSchema.fields)

    interFileName = identify_section_using_intermediate_file(filename)
    rdd = sc.textFile(interFileName, data_set.target_num_partitions) \
        .zipWithIndex() \
        .map(parse_line_to_row_with_line_no)
    df = spark.createDataFrame(rdd, SparseLineWithSectionIdLineNoSchema)
    df = section_pyspark_rdd_prep_shared(df, exec_params, section_maximum)
    rdd = (
        df.rdd
        .map(row_to_student_summary)
        .sortBy(keyfunc=lambda x: x.StudentId)  # pyright: ignore[reportArgumentType]
    )
    return rdd


def parse_line_to_row_with_line_no(
        arg: tuple[str, int],
) -> Row:
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
