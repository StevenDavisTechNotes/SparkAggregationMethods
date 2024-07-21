from src.challenges.conditional.strategies.using_pyspark.cond_pyspark_df_grp_pandas import \
    cond_pyspark_df_grp_pandas
from src.challenges.conditional.strategies.using_pyspark.cond_pyspark_df_grp_pandas_numba import \
    cond_pyspark_df_grp_pandas_numba
from src.challenges.conditional.strategies.using_pyspark.cond_pyspark_df_join import \
    cond_pyspark_df_join
from src.challenges.conditional.strategies.using_pyspark.cond_pyspark_df_nested import \
    cond_pyspark_df_nested
from src.challenges.conditional.strategies.using_pyspark.cond_pyspark_df_null import \
    cond_pyspark_df_null
from src.challenges.conditional.strategies.using_pyspark.cond_pyspark_df_window import \
    cond_pyspark_df_window
from src.challenges.conditional.strategies.using_pyspark.cond_pyspark_df_zero import \
    cond_pyspark_df_zero
from src.challenges.conditional.strategies.using_pyspark.cond_pyspark_rdd_grp_map import \
    cond_pyspark_rdd_grp_map
from src.challenges.conditional.strategies.using_pyspark.cond_pyspark_rdd_map_part import \
    cond_pyspark_rdd_map_part
from src.challenges.conditional.strategies.using_pyspark.cond_pyspark_rdd_reduce import \
    cond_pyspark_rdd_reduce
from src.challenges.conditional.strategies.using_pyspark.cond_pyspark_sql_join import \
    cond_pyspark_sql_join
from src.challenges.conditional.strategies.using_pyspark.cond_pyspark_sql_nested import \
    cond_pyspark_sql_nested
from src.challenges.conditional.strategies.using_pyspark.cond_pyspark_sql_null import \
    cond_pyspark_sql_null
from src.six_field_test_data.six_generate_test_data import \
    ChallengeMethodPythonPysparkRegistration
from src.six_field_test_data.six_generate_test_data.six_test_data_for_python_only import \
    NumericalToleranceExpectations
from src.utils.inspection import name_of_function

# solutions_using_dask: list[ChallengeMethodPythonDaskRegistration] = [
# ]
# solutions_using_python_only: list[ChallengeMethodPythonOnlyRegistration] = [
# ]

solutions_using_pyspark: list[ChallengeMethodPythonPysparkRegistration] = [
    ChallengeMethodPythonPysparkRegistration(
        strategy_name_2018='cond_sql_join',
        strategy_name=name_of_function(cond_pyspark_sql_join),
        language='python',
        interface='sql',
        requires_gpu=False,
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        delegate=cond_pyspark_sql_join
    ),
    ChallengeMethodPythonPysparkRegistration(
        strategy_name_2018='cond_fluent_join',
        strategy_name=name_of_function(cond_pyspark_df_join),
        language='python',
        interface='fluent',
        requires_gpu=False,
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        delegate=cond_pyspark_df_join
    ),
    ChallengeMethodPythonPysparkRegistration(
        strategy_name_2018='cond_sql_null',
        strategy_name=name_of_function(cond_pyspark_sql_null),
        language='python',
        interface='sql',
        requires_gpu=False,
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        delegate=cond_pyspark_sql_null
    ),
    ChallengeMethodPythonPysparkRegistration(
        strategy_name_2018='cond_fluent_null',
        strategy_name=name_of_function(cond_pyspark_df_null),
        language='python',
        interface='fluent',
        requires_gpu=False,
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        delegate=cond_pyspark_df_null
    ),
    ChallengeMethodPythonPysparkRegistration(
        strategy_name_2018='cond_fluent_zero',
        strategy_name=name_of_function(cond_pyspark_df_zero),
        language='python',
        interface='fluent',
        requires_gpu=False,
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        delegate=cond_pyspark_df_zero
    ),
    ChallengeMethodPythonPysparkRegistration(
        strategy_name_2018='cond_pandas',
        strategy_name=name_of_function(cond_pyspark_df_grp_pandas),
        language='python',
        interface='pandas',
        requires_gpu=False,
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        delegate=cond_pyspark_df_grp_pandas
    ),
    ChallengeMethodPythonPysparkRegistration(
        strategy_name_2018='cond_pandas_numba',
        strategy_name=name_of_function(cond_pyspark_df_grp_pandas_numba),
        language='python',
        interface='pandas',
        requires_gpu=False,
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        delegate=cond_pyspark_df_grp_pandas_numba
    ),
    ChallengeMethodPythonPysparkRegistration(
        strategy_name_2018='cond_sql_nested',
        strategy_name=name_of_function(cond_pyspark_sql_nested),
        language='python',
        interface='sql',
        requires_gpu=False,
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        delegate=cond_pyspark_sql_nested
    ),
    ChallengeMethodPythonPysparkRegistration(
        strategy_name_2018='cond_fluent_nested',
        strategy_name=name_of_function(cond_pyspark_df_nested),
        language='python',
        interface='fluent',
        requires_gpu=False,
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        delegate=cond_pyspark_df_nested
    ),
    ChallengeMethodPythonPysparkRegistration(
        strategy_name_2018='cond_fluent_window',
        strategy_name=name_of_function(cond_pyspark_df_window),
        language='python',
        interface='fluent',
        requires_gpu=False,
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        delegate=cond_pyspark_df_window
    ),
    ChallengeMethodPythonPysparkRegistration(
        strategy_name_2018='cond_rdd_grpmap',
        strategy_name=name_of_function(cond_pyspark_rdd_grp_map),
        language='python',
        interface='rdd',
        requires_gpu=False,
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        delegate=cond_pyspark_rdd_grp_map
    ),
    ChallengeMethodPythonPysparkRegistration(
        strategy_name_2018='cond_rdd_reduce',
        strategy_name=name_of_function(cond_pyspark_rdd_reduce),
        language='python',
        interface='rdd',
        requires_gpu=False,
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        delegate=cond_pyspark_rdd_reduce
    ),
    ChallengeMethodPythonPysparkRegistration(
        strategy_name_2018='cond_rdd_mappart',
        strategy_name=name_of_function(cond_pyspark_rdd_map_part),
        language='python',
        interface='rdd',
        requires_gpu=False,
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        delegate=cond_pyspark_rdd_map_part
    ),
]


STRATEGY_NAME_LIST = [x.strategy_name for x in solutions_using_pyspark]
