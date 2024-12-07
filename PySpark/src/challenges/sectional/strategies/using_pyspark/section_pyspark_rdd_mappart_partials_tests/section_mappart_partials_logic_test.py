from typing import Iterable

import pytest
from pyspark.sql import SparkSession
from spark_agg_methods_common_python.challenges.sectional.section_test_data_types import (
    ClassLine, LabeledTypedRow, StudentHeader, StudentSummary, TrimesterFooter,
    TrimesterHeader, TypedLine,
)
from spark_agg_methods_common_python.utils.printer import (
    print_object_as_python_literal,
)

from src.challenges.sectional.domain_logic.section_snippet_subtotal_type import (
    CompletedStudent,
)
from src.challenges.sectional.section_runner_pyspark import spark_configs
from src.challenges.sectional.strategies.using_pyspark.section_pyspark_rdd_mappart_partials import (
    section_mappart_partials_logic,
)
from src.utils.tidy_session_pyspark import (
    SPARK_SCRATCH_FOLDER, open_spark_session,
)


@pytest.fixture
def student_history() -> list[TypedLine]:
    return [
        StudentHeader(
            StudentId=1,
            StudentName='John1'
        ),
        TrimesterHeader(
            Date='2017-02-01',
            WasAbroad=False
        ),
        ClassLine(Dept=3, Credits=1, Grade=1),
        ClassLine(Dept=0, Credits=4, Grade=1),
        ClassLine(Dept=2, Credits=3, Grade=5),
        ClassLine(Dept=3, Credits=2, Grade=4),
        TrimesterFooter(
            Major=3,
            GPA=2.5454545454545454,
            Credits=11
        ),
        TrimesterHeader(
            Date='2017-03-01',
            WasAbroad=False
        ),
        ClassLine(Dept=1, Credits=1, Grade=5),
        ClassLine(Dept=2, Credits=1, Grade=2),
        ClassLine(Dept=2, Credits=3, Grade=2),
        ClassLine(Dept=0, Credits=4, Grade=4),
        TrimesterFooter(
            Major=1,
            GPA=2.230769230769231,
            Credits=13
        ),
        TrimesterHeader(
            Date='2017-04-01',
            WasAbroad=False
        ),
        ClassLine(Dept=2, Credits=3, Grade=5),
        ClassLine(Dept=0, Credits=1, Grade=2),
        ClassLine(Dept=0, Credits=2, Grade=5),
        ClassLine(Dept=0, Credits=3, Grade=5),
        TrimesterFooter(
            Major=1,
            GPA=2.4705882352941178,
            Credits=17
        ),
        TrimesterHeader(
            Date='2017-05-01',
            WasAbroad=False
        ),
        ClassLine(Dept=3, Credits=1, Grade=4),
        ClassLine(Dept=1, Credits=1, Grade=5),
        ClassLine(Dept=3, Credits=3, Grade=5),
        ClassLine(Dept=2, Credits=2, Grade=2),
        TrimesterFooter(
            Major=1,
            GPA=1.75,
            Credits=16
        ),
        TrimesterHeader(
            Date='2017-06-01',
            WasAbroad=False
        ),
        ClassLine(Dept=2, Credits=2, Grade=2),
        ClassLine(Dept=3, Credits=2, Grade=3),
        ClassLine(Dept=3, Credits=4, Grade=3),
        ClassLine(Dept=3, Credits=1, Grade=3),
        TrimesterFooter(
            Major=1,
            GPA=2.272727272727273,
            Credits=11
        ),
        TrimesterHeader(
            Date='2017-07-01',
            WasAbroad=False
        ),
        ClassLine(Dept=3, Credits=1, Grade=3),
        ClassLine(Dept=3, Credits=2, Grade=5),
        ClassLine(Dept=0, Credits=2, Grade=1),
        ClassLine(Dept=1, Credits=2, Grade=4),
        TrimesterFooter(
            Major=1,
            GPA=1.7692307692307692,
            Credits=13
        ),
        TrimesterHeader(
            Date='2017-08-01',
            WasAbroad=False
        ),
        ClassLine(Dept=3, Credits=3, Grade=2),
        ClassLine(Dept=0, Credits=4, Grade=4),
        ClassLine(Dept=1, Credits=3, Grade=5),
        ClassLine(Dept=2, Credits=3, Grade=2),
        TrimesterFooter(
            Major=1,
            GPA=3.3076923076923075,
            Credits=13
        ),
        TrimesterHeader(
            Date='2017-09-01',
            WasAbroad=False
        ),
        ClassLine(Dept=2, Credits=3, Grade=2),
        ClassLine(Dept=3, Credits=4, Grade=3),
        ClassLine(Dept=2, Credits=2, Grade=1),
        ClassLine(Dept=1, Credits=4, Grade=5),
        TrimesterFooter(
            Major=1,
            GPA=3.6363636363636362,
            Credits=11
        )
    ]


@pytest.fixture
def student_summary():
    return StudentSummary(
        StudentId=1,
        StudentName='John1',
        SourceLines=49,
        GPA=3.3506493506493507,
        Major=1,
        MajorGPA=4.818181818181818
    )


@pytest.fixture(scope="session")
def spark() -> Iterable[SparkSession]:
    enable_hive_support = False
    tidy_spark_session \
        = open_spark_session(
            spark_configs(1), enable_hive_support,
            SPARK_SCRATCH_FOLDER, 1)
    spark = tidy_spark_session.spark_session
    yield spark
    spark.stop()


class test_section_mappart_partials_logic:
    def test_one_student_pass_1_w_leader_trailer(
            self,
            student_history: list[TypedLine],
            student_summary: CompletedStudent,
            spark: SparkSession,
    ) -> None:
        sc = spark.sparkContext
        rdd_orig = (
            sc.parallelize(student_history, numSlices=1)
            .zipWithIndex()
            .map(lambda pair:
                 LabeledTypedRow(
                     Index=pair[1],
                     Value=pair[0]))
        )
        rdd = section_mappart_partials_logic(
            sc=sc,
            rdd_orig=rdd_orig,
            default_parallelism=1,
            maximum_processable_segment=10**5,
        )
        result = rdd.collect()
        print_object_as_python_literal(result)
        assert result == [student_summary]

    def test_one_student_split_pass2_w_leader_trailer(
            self,
            student_history: list[TypedLine],
            student_summary: CompletedStudent,
            spark: SparkSession,
    ) -> None:
        # split from 1 row in the first segment
        # to only 1 row in the second segment
        for num_rows_in_first_segment in range(1, len(student_history)):
            sc = spark.sparkContext
            rdd_orig = (
                sc.parallelize(student_history[0:num_rows_in_first_segment], numSlices=1)
                .union(
                    sc.parallelize(student_history[num_rows_in_first_segment:], numSlices=1)
                )
                .zipWithIndex()
                .map(lambda pair: LabeledTypedRow(Index=pair[1], Value=pair[0]))
            )
            rdd = section_mappart_partials_logic(
                sc=sc,
                rdd_orig=rdd_orig,
                default_parallelism=1,
                maximum_processable_segment=10**5,
            )
            result = rdd.collect()
            print_object_as_python_literal(result)
            assert result == [student_summary]
