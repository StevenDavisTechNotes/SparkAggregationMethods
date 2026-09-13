from spark_agg_methods_common_python.perf_test_common import (
    CalcEngine, NumericalToleranceExpectations, SolutionInterfacePySpark,
    SolutionLanguage,
)
from spark_agg_methods_common_python.utils.inspection import name_of_function

from src.challenges.six_field_test_data.six_test_data_for_pyspark import (
    SixFieldChallengeMethodPythonPysparkRegistration,
)
from src.challenges.vanilla.strategies.using_pyspark.vanilla_pyspark_df_grp_builtin import (
    vanilla_pyspark_df_grp_builtin,
)
from src.challenges.vanilla.strategies.using_pyspark.vanilla_pyspark_df_grp_numba import (
    vanilla_pyspark_df_grp_numba,
)
from src.challenges.vanilla.strategies.using_pyspark.vanilla_pyspark_df_grp_numpy import (
    vanilla_pyspark_df_grp_numpy,
)
from src.challenges.vanilla.strategies.using_pyspark.vanilla_pyspark_df_grp_pandas import (
    vanilla_pyspark_df_grp_pandas,
)
from src.challenges.vanilla.strategies.using_pyspark.vanilla_pyspark_rdd_grp_map import (
    vanilla_pyspark_rdd_grp_map,
)
from src.challenges.vanilla.strategies.using_pyspark.vanilla_pyspark_rdd_mappart import (
    vanilla_pyspark_rdd_mappart,
)
from src.challenges.vanilla.strategies.using_pyspark.vanilla_pyspark_rdd_reduce import (
    vanilla_pyspark_rdd_reduce,
)
from src.challenges.vanilla.strategies.using_pyspark.vanilla_pyspark_sql import (
    vanilla_pyspark_sql,
)

VANILLA_STRATEGY_REGISTRY_PYSPARK: list[SixFieldChallengeMethodPythonPysparkRegistration] = [
    SixFieldChallengeMethodPythonPysparkRegistration(
        strategy_name_2018='vanilla_sql',
        strategy_name=name_of_function(vanilla_pyspark_sql),
        language=SolutionLanguage.PYTHON,
        engine=CalcEngine.PYSPARK,
        interface=SolutionInterfacePySpark.PYSPARK_DATAFRAME_SQL,
        requires_gpu=False,
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        delegate=vanilla_pyspark_sql
    ),
    SixFieldChallengeMethodPythonPysparkRegistration(
        strategy_name_2018='vanilla_fluent',
        strategy_name=name_of_function(vanilla_pyspark_df_grp_builtin),
        language=SolutionLanguage.PYTHON,
        engine=CalcEngine.PYSPARK,
        interface=SolutionInterfacePySpark.PYSPARK_DATAFRAME_SQL,
        requires_gpu=False,
        numerical_tolerance=NumericalToleranceExpectations.SIMPLE_SUM,
        delegate=vanilla_pyspark_df_grp_builtin,
    ),
    SixFieldChallengeMethodPythonPysparkRegistration(
        strategy_name_2018='vanilla_pandas',
        strategy_name=name_of_function(vanilla_pyspark_df_grp_pandas),
        language=SolutionLanguage.PYTHON,
        engine=CalcEngine.PYSPARK,
        interface=SolutionInterfacePySpark.PYSPARK_DATAFRAME_PANDAS,
        requires_gpu=False,
        numerical_tolerance=NumericalToleranceExpectations.SIMPLE_SUM,
        delegate=vanilla_pyspark_df_grp_pandas,
    ),
    SixFieldChallengeMethodPythonPysparkRegistration(
        strategy_name_2018='vanilla_pandas_numpy',
        strategy_name=name_of_function(vanilla_pyspark_df_grp_numpy),
        language=SolutionLanguage.PYTHON,
        engine=CalcEngine.PYSPARK,
        interface=SolutionInterfacePySpark.PYSPARK_DATAFRAME_PANDAS,
        requires_gpu=False,
        numerical_tolerance=NumericalToleranceExpectations.SIMPLE_SUM,
        delegate=vanilla_pyspark_df_grp_numpy,
    ),
    SixFieldChallengeMethodPythonPysparkRegistration(
        strategy_name_2018='vanilla_pandas_numba',
        strategy_name=name_of_function(vanilla_pyspark_df_grp_numba),
        language=SolutionLanguage.PYTHON,
        engine=CalcEngine.PYSPARK,
        interface=SolutionInterfacePySpark.PYSPARK_DATAFRAME_PANDAS,
        requires_gpu=True,
        numerical_tolerance=NumericalToleranceExpectations.NUMBA,
        delegate=vanilla_pyspark_df_grp_numba,
    ),
    SixFieldChallengeMethodPythonPysparkRegistration(
        strategy_name_2018='vanilla_rdd_grpmap',
        strategy_name=name_of_function(vanilla_pyspark_rdd_grp_map),
        language=SolutionLanguage.PYTHON,
        engine=CalcEngine.PYSPARK,
        interface=SolutionInterfacePySpark.PYSPARK_RDD,
        requires_gpu=False,
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        delegate=vanilla_pyspark_rdd_grp_map,
    ),
    SixFieldChallengeMethodPythonPysparkRegistration(
        strategy_name_2018='vanilla_rdd_reduce',
        strategy_name=name_of_function(vanilla_pyspark_rdd_reduce),
        language=SolutionLanguage.PYTHON,
        engine=CalcEngine.PYSPARK,
        interface=SolutionInterfacePySpark.PYSPARK_RDD,
        requires_gpu=False,
        numerical_tolerance=NumericalToleranceExpectations.SIMPLE_SUM,
        delegate=vanilla_pyspark_rdd_reduce,
    ),
    SixFieldChallengeMethodPythonPysparkRegistration(
        strategy_name_2018='vanilla_rdd_mappart',
        strategy_name=name_of_function(vanilla_pyspark_rdd_mappart),
        language=SolutionLanguage.PYTHON,
        engine=CalcEngine.PYSPARK,
        interface=SolutionInterfacePySpark.PYSPARK_RDD,
        requires_gpu=False,
        numerical_tolerance=NumericalToleranceExpectations.SIMPLE_SUM,
        delegate=vanilla_pyspark_rdd_mappart,
    ),
]
