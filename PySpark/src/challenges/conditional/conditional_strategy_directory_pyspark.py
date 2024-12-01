from spark_agg_methods_common_python.perf_test_common import (
    CalcEngine, NumericalToleranceExpectations, SolutionInterfacePySpark, SolutionLanguage,
)
from spark_agg_methods_common_python.utils.inspection import name_of_function

from src.challenges.conditional.strategies.using_pyspark.cond_pyspark_df_grp_pandas import cond_pyspark_df_grp_pandas
from src.challenges.conditional.strategies.using_pyspark.cond_pyspark_df_grp_pandas_numba import (
    cond_pyspark_df_grp_pandas_numba,
)
from src.challenges.conditional.strategies.using_pyspark.cond_pyspark_df_join import cond_pyspark_df_join
from src.challenges.conditional.strategies.using_pyspark.cond_pyspark_df_nested import cond_pyspark_df_nested
from src.challenges.conditional.strategies.using_pyspark.cond_pyspark_df_null import cond_pyspark_df_null
from src.challenges.conditional.strategies.using_pyspark.cond_pyspark_df_window import cond_pyspark_df_window
from src.challenges.conditional.strategies.using_pyspark.cond_pyspark_df_zero import cond_pyspark_df_zero
from src.challenges.conditional.strategies.using_pyspark.cond_pyspark_rdd_grp_map import cond_pyspark_rdd_grp_map
from src.challenges.conditional.strategies.using_pyspark.cond_pyspark_rdd_map_part import cond_pyspark_rdd_map_part
from src.challenges.conditional.strategies.using_pyspark.cond_pyspark_rdd_reduce import cond_pyspark_rdd_reduce
from src.challenges.conditional.strategies.using_pyspark.cond_pyspark_sql_join import cond_pyspark_sql_join
from src.challenges.conditional.strategies.using_pyspark.cond_pyspark_sql_nested import cond_pyspark_sql_nested
from src.challenges.conditional.strategies.using_pyspark.cond_pyspark_sql_null import cond_pyspark_sql_null
from src.challenges.six_field_test_data.six_test_data_for_pyspark import (
    SixFieldChallengeMethodPythonPysparkRegistration,
)

CONDITIONAL_STRATEGIES_USING_PYSPARK_REGISTRY: list[SixFieldChallengeMethodPythonPysparkRegistration] = [
    SixFieldChallengeMethodPythonPysparkRegistration(
        strategy_name_2018='cond_sql_join',
        strategy_name=name_of_function(cond_pyspark_sql_join),
        language=SolutionLanguage.PYTHON,
        engine=CalcEngine.PYSPARK,
        interface=SolutionInterfacePySpark.PYSPARK_DATAFRAME_SQL,
        requires_gpu=False,
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        delegate=cond_pyspark_sql_join
    ),
    SixFieldChallengeMethodPythonPysparkRegistration(
        strategy_name_2018='cond_fluent_join',
        strategy_name=name_of_function(cond_pyspark_df_join),
        language=SolutionLanguage.PYTHON,
        engine=CalcEngine.PYSPARK,
        interface=SolutionInterfacePySpark.PYSPARK_DATAFRAME_FLUENT,
        requires_gpu=False,
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        delegate=cond_pyspark_df_join
    ),
    SixFieldChallengeMethodPythonPysparkRegistration(
        strategy_name_2018='cond_sql_null',
        strategy_name=name_of_function(cond_pyspark_sql_null),
        language=SolutionLanguage.PYTHON,
        engine=CalcEngine.PYSPARK,
        interface=SolutionInterfacePySpark.PYSPARK_DATAFRAME_SQL,
        requires_gpu=False,
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        delegate=cond_pyspark_sql_null
    ),
    SixFieldChallengeMethodPythonPysparkRegistration(
        strategy_name_2018='cond_fluent_null',
        strategy_name=name_of_function(cond_pyspark_df_null),
        language=SolutionLanguage.PYTHON,
        engine=CalcEngine.PYSPARK,
        interface=SolutionInterfacePySpark.PYSPARK_DATAFRAME_FLUENT,
        requires_gpu=False,
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        delegate=cond_pyspark_df_null
    ),
    SixFieldChallengeMethodPythonPysparkRegistration(
        strategy_name_2018='cond_fluent_zero',
        strategy_name=name_of_function(cond_pyspark_df_zero),
        language=SolutionLanguage.PYTHON,
        engine=CalcEngine.PYSPARK,
        interface=SolutionInterfacePySpark.PYSPARK_DATAFRAME_FLUENT,
        requires_gpu=False,
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        delegate=cond_pyspark_df_zero
    ),
    SixFieldChallengeMethodPythonPysparkRegistration(
        strategy_name_2018='cond_pandas',
        strategy_name=name_of_function(cond_pyspark_df_grp_pandas),
        language=SolutionLanguage.PYTHON,
        engine=CalcEngine.PYSPARK,
        interface=SolutionInterfacePySpark.PYSPARK_DATAFRAME_PANDAS,
        requires_gpu=False,
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        delegate=cond_pyspark_df_grp_pandas
    ),
    SixFieldChallengeMethodPythonPysparkRegistration(
        strategy_name_2018='cond_pandas_numba',
        strategy_name=name_of_function(cond_pyspark_df_grp_pandas_numba),
        language=SolutionLanguage.PYTHON,
        engine=CalcEngine.PYSPARK,
        interface=SolutionInterfacePySpark.PYSPARK_DATAFRAME_PANDAS,
        requires_gpu=False,
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        delegate=cond_pyspark_df_grp_pandas_numba
    ),
    SixFieldChallengeMethodPythonPysparkRegistration(
        strategy_name_2018='cond_sql_nested',
        strategy_name=name_of_function(cond_pyspark_sql_nested),
        language=SolutionLanguage.PYTHON,
        engine=CalcEngine.PYSPARK,
        interface=SolutionInterfacePySpark.PYSPARK_DATAFRAME_SQL,
        requires_gpu=False,
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        delegate=cond_pyspark_sql_nested
    ),
    SixFieldChallengeMethodPythonPysparkRegistration(
        strategy_name_2018='cond_fluent_nested',
        strategy_name=name_of_function(cond_pyspark_df_nested),
        language=SolutionLanguage.PYTHON,
        engine=CalcEngine.PYSPARK,
        interface=SolutionInterfacePySpark.PYSPARK_DATAFRAME_FLUENT,
        requires_gpu=False,
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        delegate=cond_pyspark_df_nested
    ),
    SixFieldChallengeMethodPythonPysparkRegistration(
        strategy_name_2018='cond_fluent_window',
        strategy_name=name_of_function(cond_pyspark_df_window),
        language=SolutionLanguage.PYTHON,
        engine=CalcEngine.PYSPARK,
        interface=SolutionInterfacePySpark.PYSPARK_DATAFRAME_FLUENT,
        requires_gpu=False,
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        delegate=cond_pyspark_df_window
    ),
    SixFieldChallengeMethodPythonPysparkRegistration(
        strategy_name_2018='cond_rdd_grpmap',
        strategy_name=name_of_function(cond_pyspark_rdd_grp_map),
        language=SolutionLanguage.PYTHON,
        engine=CalcEngine.PYSPARK,
        interface=SolutionInterfacePySpark.PYSPARK_RDD,
        requires_gpu=False,
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        delegate=cond_pyspark_rdd_grp_map
    ),
    SixFieldChallengeMethodPythonPysparkRegistration(
        strategy_name_2018='cond_rdd_reduce',
        strategy_name=name_of_function(cond_pyspark_rdd_reduce),
        language=SolutionLanguage.PYTHON,
        engine=CalcEngine.PYSPARK,
        interface=SolutionInterfacePySpark.PYSPARK_RDD,
        requires_gpu=False,
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        delegate=cond_pyspark_rdd_reduce
    ),
    SixFieldChallengeMethodPythonPysparkRegistration(
        strategy_name_2018='cond_rdd_mappart',
        strategy_name=name_of_function(cond_pyspark_rdd_map_part),
        language=SolutionLanguage.PYTHON,
        engine=CalcEngine.PYSPARK,
        interface=SolutionInterfacePySpark.PYSPARK_RDD,
        requires_gpu=False,
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        delegate=cond_pyspark_rdd_map_part
    ),
]
