from spark_agg_methods_common_python.challenges.sectional.section_test_data_types import (
    ClassLine, StudentHeader, TrimesterFooter, TrimesterHeader, TypedLine,
)
from spark_agg_methods_common_python.utils.printer import (
    print_object_as_python_literal,
)

from src.challenges.sectional.strategies.using_pyspark.section_pyspark_rdd_prep_mappart import (
    parse_line_to_types_with_line_no,
)


def test_nominal():
    prepped_data = """
9,S,10,John10
9,TH,2017-02-01,False
9,C,0,4,3
9,C,2,2,2
9,C,2,2,1
9,C,0,2,2
9,TF,3,2.75,8
""".strip().split('\n')
    expected: list[tuple[tuple[int, int], TypedLine]] = [
        (
            (9, 441),
            StudentHeader(
                StudentId=10,
                StudentName='John10'
            )
        ),
        (
            (9, 442),
            TrimesterHeader(
                Date='2017-02-01',
                WasAbroad=False
            )
        ),
        (
            (9, 443),
            ClassLine(
                Dept=0,
                Credits=4,
                Grade=3
            )
        ),
        (
            (9, 444),
            ClassLine(
                Dept=2,
                Credits=2,
                Grade=2
            )
        ),
        (
            (9, 445),
            ClassLine(
                Dept=2,
                Credits=2,
                Grade=1
            )
        ),
        (
            (9, 446),
            ClassLine(
                Dept=0,
                Credits=2,
                Grade=2
            )
        ),
        (
            (9, 447),
            TrimesterFooter(
                Major=3,
                GPA=2.75,
                Credits=8
            )
        )
    ]

    actual = list(map(lambda x: parse_line_to_types_with_line_no('filename', 441+x[0], x[1]), enumerate(prepped_data)))
    print_object_as_python_literal(actual)
    assert expected == actual
