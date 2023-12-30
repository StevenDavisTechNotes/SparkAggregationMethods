from typing import List

from challenges.deduplication.dedupe_test_data_types import PysparkTestMethod
from challenges.deduplication.using_pyspark.dedupe_pyspark_df_nested_pandas import \
    dedupe_pyspark_df_nested_pandas
from challenges.deduplication.using_pyspark.dedupe_pyspark_df_nested_python import \
    dedupe_pyspark_df_nested_python
from challenges.deduplication.using_pyspark.dedupe_pyspark_df_nested_w_col import \
    dedupe_pyspark_df_nested_w_col
from challenges.deduplication.using_pyspark.dedupe_pyspark_df_window import \
    dedupe_pyspark_df_window
from challenges.deduplication.using_pyspark.dedupe_pyspark_rdd_grp import \
    dedupe_pyspark_rdd_grp
from challenges.deduplication.using_pyspark.dedupe_pyspark_rdd_map_part import \
    dedupe_pyspark_rdd_map_part
from challenges.deduplication.using_pyspark.dedupe_pyspark_rdd_reduce import \
    dedupe_pyspark_rdd_reduce
from utils.inspection import nameof_function

pyspark_implementation_list: List[PysparkTestMethod] = [
    PysparkTestMethod(
        strategy_name=nameof_function(dedupe_pyspark_df_nested_pandas),
        language='python',
        interface='pandas',
        delegate=dedupe_pyspark_df_nested_pandas,
    ),
    PysparkTestMethod(
        strategy_name=nameof_function(dedupe_pyspark_df_nested_python),
        language='python',
        interface='fluent',
        delegate=dedupe_pyspark_df_nested_python,
    ),
    PysparkTestMethod(
        strategy_name=nameof_function(dedupe_pyspark_df_nested_w_col),
        language='python',
        interface='fluent',
        delegate=dedupe_pyspark_df_nested_w_col,
    ),
    PysparkTestMethod(
        strategy_name=nameof_function(dedupe_pyspark_df_window),
        language='python',
        interface='fluent',
        delegate=dedupe_pyspark_df_window,
    ),
    PysparkTestMethod(
        strategy_name=nameof_function(dedupe_pyspark_rdd_grp),
        language='python',
        interface='rdd',
        delegate=dedupe_pyspark_rdd_grp,
    ),
    PysparkTestMethod(
        strategy_name=nameof_function(dedupe_pyspark_rdd_map_part),
        language='python',
        interface='rdd',
        delegate=dedupe_pyspark_rdd_map_part,
    ),
    PysparkTestMethod(
        strategy_name=nameof_function(dedupe_pyspark_rdd_reduce),
        language='python',
        interface='rdd',
        delegate=dedupe_pyspark_rdd_reduce,
    ),
]

strategy_name_list = [x.strategy_name for x in pyspark_implementation_list]
