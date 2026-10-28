from spark_agg_methods_common_python.perf_test_common import (
    CalcEngine, NumericalToleranceExpectations, SolutionInterfacePythonST, SolutionLanguage,
)
from spark_agg_methods_common_python.utils.inspection import name_of_function
from src.challenges.conditional.strategies.single_threaded.cond_py_st_pd_grp_numpy import cond_py_st_pd_grp_numpy
from src.challenges.conditional.strategies.single_threaded.cond_py_st_pd_grp_pandas import cond_py_st_pd_grp_pandas
from src.challenges.conditional.strategies.single_threaded.cond_py_st_pd_grp_polars import cond_py_st_pd_grp_polars
from src.challenges.six_field_test_data.six_test_data_for_py_st import ChallengeMethodPythonSingleThreadedRegistration

CONDITIONAL_STRATEGY_REGISTRY_PYTHON_SINGLE_THREADED: list[ChallengeMethodPythonSingleThreadedRegistration] = [
    ChallengeMethodPythonSingleThreadedRegistration(
        strategy_name_2018=None,
        strategy_name=name_of_function(cond_py_st_pd_grp_numpy),
        language=SolutionLanguage.PYTHON,
        engine=CalcEngine.SINGLE_THREADED,
        interface=SolutionInterfacePythonST.PANDAS,
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        requires_gpu=False,
        delegate=cond_py_st_pd_grp_numpy,
    ),
    ChallengeMethodPythonSingleThreadedRegistration(
        strategy_name_2018=None,
        strategy_name=name_of_function(cond_py_st_pd_grp_pandas),
        language=SolutionLanguage.PYTHON,
        engine=CalcEngine.SINGLE_THREADED,
        interface=SolutionInterfacePythonST.PANDAS,
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        requires_gpu=False,
        delegate=cond_py_st_pd_grp_pandas,
    ),
    ChallengeMethodPythonSingleThreadedRegistration(
        strategy_name_2018=None,
        strategy_name=name_of_function(cond_py_st_pd_grp_polars),
        language=SolutionLanguage.PYTHON,
        engine=CalcEngine.SINGLE_THREADED,
        interface=SolutionInterfacePythonST.POLARS,
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        requires_gpu=False,
        delegate=cond_py_st_pd_grp_polars,
    ),
]
