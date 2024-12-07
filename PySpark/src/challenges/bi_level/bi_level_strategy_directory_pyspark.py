from spark_agg_methods_common_python.perf_test_common import (
    CalcEngine, NumericalToleranceExpectations, SolutionInterfacePySpark,
    SolutionLanguage,
)
from spark_agg_methods_common_python.utils.inspection import name_of_function

from src.challenges.bi_level.strategies.using_pyspark.bi_level_pyspark_df_grp_pandas import (
    bi_level_pyspark_df_grp_pandas,
)
from src.challenges.bi_level.strategies.using_pyspark.bi_level_pyspark_df_grp_pandas_numba import (
    bi_level_pyspark_df_grp_pandas_numba,
)
from src.challenges.bi_level.strategies.using_pyspark.bi_level_pyspark_df_join import (
    bi_level_pyspark_df_join,
)
from src.challenges.bi_level.strategies.using_pyspark.bi_level_pyspark_df_nested import (
    bi_level_pyspark_df_nested,
)
from src.challenges.bi_level.strategies.using_pyspark.bi_level_pyspark_df_window import (
    bi_level_pyspark_df_window,
)
from src.challenges.bi_level.strategies.using_pyspark.bi_level_pyspark_rdd_grp_map import (
    bi_level_pyspark_rdd_grp_map,
)
from src.challenges.bi_level.strategies.using_pyspark.bi_level_pyspark_rdd_map_part import (
    bi_level_pyspark_rdd_map_part,
)
from src.challenges.bi_level.strategies.using_pyspark.bi_level_pyspark_rdd_reduce_1 import (
    bi_level_pyspark_rdd_reduce_1,
)
from src.challenges.bi_level.strategies.using_pyspark.bi_level_pyspark_rdd_reduce_2 import (
    bi_level_pyspark_rdd_reduce_2,
)
from src.challenges.bi_level.strategies.using_pyspark.bi_level_pyspark_sql_join import (
    bi_level_pyspark_sql_join,
)
from src.challenges.bi_level.strategies.using_pyspark.bi_level_pyspark_sql_nested import (
    bi_level_pyspark_sql_nested,
)
from src.challenges.six_field_test_data.six_test_data_for_pyspark import (
    SixFieldChallengeMethodPythonPysparkRegistration,
)

BI_LEVEL_STRATEGY_REGISTRY_PYSPARK: list[SixFieldChallengeMethodPythonPysparkRegistration] = [
    SixFieldChallengeMethodPythonPysparkRegistration(
        strategy_name_2018='bi_sql_join',
        strategy_name=name_of_function(bi_level_pyspark_sql_join),
        language=SolutionLanguage.PYTHON,
        engine=CalcEngine.PYSPARK,
        interface=SolutionInterfacePySpark.PYSPARK_DATAFRAME_SQL,
        requires_gpu=False,
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        delegate=bi_level_pyspark_sql_join
    ),
    SixFieldChallengeMethodPythonPysparkRegistration(
        strategy_name_2018='bi_fluent_join',
        strategy_name=name_of_function(bi_level_pyspark_df_join),
        language=SolutionLanguage.PYTHON,
        engine=CalcEngine.PYSPARK,
        interface=SolutionInterfacePySpark.PYSPARK_DATAFRAME_FLUENT,
        requires_gpu=False,
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        delegate=bi_level_pyspark_df_join
    ),
    SixFieldChallengeMethodPythonPysparkRegistration(
        strategy_name_2018='bi_pandas',
        strategy_name=name_of_function(bi_level_pyspark_df_grp_pandas),
        language=SolutionLanguage.PYTHON,
        engine=CalcEngine.PYSPARK,
        interface=SolutionInterfacePySpark.PYSPARK_DATAFRAME_PANDAS,
        requires_gpu=False,
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        delegate=bi_level_pyspark_df_grp_pandas
    ),
    SixFieldChallengeMethodPythonPysparkRegistration(
        strategy_name_2018='bi_pandas_numba',
        strategy_name=name_of_function(bi_level_pyspark_df_grp_pandas_numba),
        language=SolutionLanguage.PYTHON,
        engine=CalcEngine.PYSPARK,
        interface=SolutionInterfacePySpark.PYSPARK_DATAFRAME_PANDAS,
        requires_gpu=True,
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        delegate=bi_level_pyspark_df_grp_pandas_numba
    ),
    SixFieldChallengeMethodPythonPysparkRegistration(
        strategy_name_2018='bi_sql_nested',
        strategy_name=name_of_function(bi_level_pyspark_sql_nested),
        language=SolutionLanguage.PYTHON,
        engine=CalcEngine.PYSPARK,
        interface=SolutionInterfacePySpark.PYSPARK_DATAFRAME_SQL,
        requires_gpu=False,
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        delegate=bi_level_pyspark_sql_nested
    ),
    SixFieldChallengeMethodPythonPysparkRegistration(
        strategy_name_2018='bi_fluent_nested',
        strategy_name=name_of_function(bi_level_pyspark_df_nested),
        language=SolutionLanguage.PYTHON,
        engine=CalcEngine.PYSPARK,
        interface=SolutionInterfacePySpark.PYSPARK_DATAFRAME_FLUENT,
        requires_gpu=False,
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        delegate=bi_level_pyspark_df_nested
    ),
    SixFieldChallengeMethodPythonPysparkRegistration(
        strategy_name_2018='bi_fluent_window',
        strategy_name=name_of_function(bi_level_pyspark_df_window),
        language=SolutionLanguage.PYTHON,
        engine=CalcEngine.PYSPARK,
        interface=SolutionInterfacePySpark.PYSPARK_DATAFRAME_FLUENT,
        requires_gpu=False,
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        delegate=bi_level_pyspark_df_window
    ),
    SixFieldChallengeMethodPythonPysparkRegistration(
        strategy_name_2018='bi_rdd_grpmap',
        strategy_name=name_of_function(bi_level_pyspark_rdd_grp_map),
        language=SolutionLanguage.PYTHON,
        engine=CalcEngine.PYSPARK,
        interface=SolutionInterfacePySpark.PYSPARK_RDD,
        requires_gpu=False,
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        delegate=bi_level_pyspark_rdd_grp_map
    ),
    SixFieldChallengeMethodPythonPysparkRegistration(
        strategy_name_2018='bi_rdd_reduce1',
        strategy_name=name_of_function(bi_level_pyspark_rdd_reduce_1),
        language=SolutionLanguage.PYTHON,
        engine=CalcEngine.PYSPARK,
        interface=SolutionInterfacePySpark.PYSPARK_RDD,
        requires_gpu=False,
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        delegate=bi_level_pyspark_rdd_reduce_1
    ),
    SixFieldChallengeMethodPythonPysparkRegistration(
        strategy_name_2018='bi_rdd_reduce2',
        strategy_name=name_of_function(bi_level_pyspark_rdd_reduce_2),
        language=SolutionLanguage.PYTHON,
        engine=CalcEngine.PYSPARK,
        interface=SolutionInterfacePySpark.PYSPARK_RDD,
        requires_gpu=False,
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        delegate=bi_level_pyspark_rdd_reduce_2
    ),
    SixFieldChallengeMethodPythonPysparkRegistration(
        strategy_name_2018='bi_rdd_mappart',
        strategy_name=name_of_function(bi_level_pyspark_rdd_map_part),
        language=SolutionLanguage.PYTHON,
        engine=CalcEngine.PYSPARK,
        interface=SolutionInterfacePySpark.PYSPARK_RDD,
        requires_gpu=False,
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        delegate=bi_level_pyspark_rdd_map_part
    ),
]
