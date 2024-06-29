from challenges.deduplication.dedupe_test_data_types import \
    ChallengeMethodPythonPysparkRegistration
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
from utils.inspection import name_of_function

pyspark_implementation_list: list[ChallengeMethodPythonPysparkRegistration] = [
    ChallengeMethodPythonPysparkRegistration(
        original_strategy_name='dedupe_pandas',
        strategy_name=name_of_function(dedupe_pyspark_df_nested_pandas),
        language='python',
        interface='pandas',
        delegate=dedupe_pyspark_df_nested_pandas,
    ),
    ChallengeMethodPythonPysparkRegistration(
        original_strategy_name='dedupe_fluent_nested_python',
        strategy_name=name_of_function(dedupe_pyspark_df_nested_python),
        language='python',
        interface='fluent',
        delegate=dedupe_pyspark_df_nested_python,
    ),
    ChallengeMethodPythonPysparkRegistration(
        original_strategy_name='dedupe_fluent_nested_withCol',
        strategy_name=name_of_function(dedupe_pyspark_df_nested_w_col),
        language='python',
        interface='fluent',
        delegate=dedupe_pyspark_df_nested_w_col,
    ),
    ChallengeMethodPythonPysparkRegistration(
        original_strategy_name='dedupe_fluent_windows',
        strategy_name=name_of_function(dedupe_pyspark_df_window),
        language='python',
        interface='fluent',
        delegate=dedupe_pyspark_df_window,
    ),
    ChallengeMethodPythonPysparkRegistration(
        original_strategy_name='dedupe_rdd_groupby',
        strategy_name=name_of_function(dedupe_pyspark_rdd_grp),
        language='python',
        interface='rdd',
        delegate=dedupe_pyspark_rdd_grp,
    ),
    ChallengeMethodPythonPysparkRegistration(
        original_strategy_name='dedupe_rdd_mappart',
        strategy_name=name_of_function(dedupe_pyspark_rdd_map_part),
        language='python',
        interface='rdd',
        delegate=dedupe_pyspark_rdd_map_part,
    ),
    ChallengeMethodPythonPysparkRegistration(
        original_strategy_name='dedupe_rdd_reduce',
        strategy_name=name_of_function(dedupe_pyspark_rdd_reduce),
        language='python',
        interface='rdd',
        delegate=dedupe_pyspark_rdd_reduce,
    ),
]

STRATEGY_NAME_LIST = [x.strategy_name for x in pyspark_implementation_list]
