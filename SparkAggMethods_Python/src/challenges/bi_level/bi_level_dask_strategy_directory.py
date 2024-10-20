from spark_agg_methods_common_python.perf_test_common import (
    CalcEngine, NumericalToleranceExpectations, SolutionInterfaceDask, SolutionLanguage,
)
from spark_agg_methods_common_python.utils.inspection import name_of_function

from src.challenges.bi_level.strategies.using_dask.bi_level_dask_sql_nested import bi_level_dask_sql_nested_no_gpu
from src.challenges.bi_level.strategies.using_dask.bi_level_dask_sql_single_join import (
    bi_level_dask_sql_single_join_no_gpu,
)
from src.challenges.bi_level.strategies.using_dask.bi_level_dask_sql_temp_join import bi_level_dask_sql_temp_join_no_gpu
from src.challenges.six_field_test_data.six_test_data_for_dask import ChallengeMethodPythonDaskRegistration

BI_LEVEL_STRATEGIES_USING_DASK_REGISTRY: list[ChallengeMethodPythonDaskRegistration] = [
    # ChallengeMethodPythonDaskRegistration(
    #     strategy_name_2018=None,
    #     strategy_name=name_of_function(bi_level_dask_bag_foldby_twice),
    #     language=SolutionLanguage.PYTHON,
    #     engine=CalcEngine.DASK,
    #     interface=SolutionInterfaceDask.DASK_BAG,
    #     numerical_tolerance=NumericalToleranceExpectations.NUMPY,
    #     requires_gpu=False,
    #     delegate=bi_level_dask_bag_foldby_twice,
    # ),
    ChallengeMethodPythonDaskRegistration(
        strategy_name_2018=None,
        strategy_name=name_of_function(bi_level_dask_sql_nested_no_gpu),
        language=SolutionLanguage.PYTHON,
        engine=CalcEngine.DASK,
        interface=SolutionInterfaceDask.DASK_DATAFRAME,
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        requires_gpu=False,
        delegate=bi_level_dask_sql_nested_no_gpu,
    ),
    ChallengeMethodPythonDaskRegistration(
        strategy_name_2018=None,
        strategy_name=name_of_function(bi_level_dask_sql_single_join_no_gpu),
        language=SolutionLanguage.PYTHON,
        engine=CalcEngine.DASK,
        interface=SolutionInterfaceDask.DASK_DATAFRAME,
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        requires_gpu=False,
        delegate=bi_level_dask_sql_single_join_no_gpu,
    ),
    ChallengeMethodPythonDaskRegistration(
        strategy_name_2018=None,
        strategy_name=name_of_function(bi_level_dask_sql_temp_join_no_gpu),
        language=SolutionLanguage.PYTHON,
        engine=CalcEngine.DASK,
        interface=SolutionInterfaceDask.DASK_DATAFRAME,
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        requires_gpu=False,
        delegate=bi_level_dask_sql_temp_join_no_gpu,
    ),
]
