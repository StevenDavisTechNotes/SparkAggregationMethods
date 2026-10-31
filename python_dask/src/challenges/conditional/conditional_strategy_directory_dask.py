from spark_agg_methods_common_python.perf_test_common import (
    CalcEngine, NumericalToleranceExpectations, SolutionInterfaceDask, SolutionLanguage,
)
from spark_agg_methods_common_python.utils.inspection import name_of_function
from src.challenges.conditional.strategies.using_dask.cond_dask_actors_accumulate import cond_dask_actors_accumulate
from src.challenges.conditional.strategies.using_dask.cond_dask_ddf_grp_apply import cond_dask_ddf_grp_apply
from src.challenges.conditional.strategies.using_dask.cond_dask_ddf_grp_udaf import cond_dask_ddf_grp_udaf
from src.challenges.conditional.strategies.using_dask.cond_dask_ddf_shuffle_local import cond_dask_ddf_shuffle_local
from src.challenges.conditional.strategies.using_dask.cond_dask_delayed_pl_chunks import cond_dask_delayed_pl_chunks
from src.challenges.conditional.strategies.using_dask.cond_dask_futures_as_completed import (
    cond_dask_futures_as_completed,
)
from src.challenges.six_field_test_data.six_test_data_for_dask import ChallengeMethodPythonDaskRegistration

CONDITIONAL_STRATEGY_REGISTRY_DASK: list[ChallengeMethodPythonDaskRegistration] = [
    ChallengeMethodPythonDaskRegistration(
        strategy_name_2018=None,
        strategy_name=name_of_function(cond_dask_ddf_grp_apply),
        language=SolutionLanguage.PYTHON,
        engine=CalcEngine.DASK,
        interface=SolutionInterfaceDask.DASK_DATAFRAME,
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        requires_gpu=False,
        delegate=cond_dask_ddf_grp_apply,
    ),
    ChallengeMethodPythonDaskRegistration(
        strategy_name_2018=None,
        strategy_name=name_of_function(cond_dask_ddf_grp_udaf),
        language=SolutionLanguage.PYTHON,
        engine=CalcEngine.DASK,
        interface=SolutionInterfaceDask.DASK_DATAFRAME,
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        requires_gpu=False,
        delegate=cond_dask_ddf_grp_udaf,
    ),
    ChallengeMethodPythonDaskRegistration(
        strategy_name_2018=None,
        strategy_name=name_of_function(cond_dask_delayed_pl_chunks),
        language=SolutionLanguage.PYTHON,
        engine=CalcEngine.DASK,
        interface=SolutionInterfaceDask.DASK_DELAYED,
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        requires_gpu=False,
        delegate=cond_dask_delayed_pl_chunks,
    ),
    ChallengeMethodPythonDaskRegistration(
        strategy_name_2018=None,
        strategy_name=name_of_function(cond_dask_ddf_shuffle_local),
        language=SolutionLanguage.PYTHON,
        engine=CalcEngine.DASK,
        interface=SolutionInterfaceDask.DASK_DATAFRAME,
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        requires_gpu=False,
        requires_cluster=True,
        delegate=cond_dask_ddf_shuffle_local,
    ),
    ChallengeMethodPythonDaskRegistration(
        strategy_name_2018=None,
        strategy_name=name_of_function(cond_dask_futures_as_completed),
        language=SolutionLanguage.PYTHON,
        engine=CalcEngine.DASK,
        interface=SolutionInterfaceDask.DASK_FUTURES,
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        requires_gpu=False,
        requires_cluster=True,
        delegate=cond_dask_futures_as_completed,
    ),
    ChallengeMethodPythonDaskRegistration(
        strategy_name_2018=None,
        strategy_name=name_of_function(cond_dask_actors_accumulate),
        language=SolutionLanguage.PYTHON,
        engine=CalcEngine.DASK,
        interface=SolutionInterfaceDask.DASK_ACTORS,
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        requires_gpu=False,
        requires_cluster=True,
        delegate=cond_dask_actors_accumulate,
    ),
]
