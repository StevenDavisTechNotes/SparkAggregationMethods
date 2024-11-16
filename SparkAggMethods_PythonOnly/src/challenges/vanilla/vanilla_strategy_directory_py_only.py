from spark_agg_methods_common_python.perf_test_common import (
    CalcEngine, NumericalToleranceExpectations, SolutionInterfacePythonOnly, SolutionLanguage,
)
from spark_agg_methods_common_python.utils.inspection import name_of_function

from src.challenges.six_field_test_data.six_test_data_for_py_only import ChallengeMethodPythonOnlyRegistration
from src.challenges.vanilla.strategies.queue_mediated.vanilla_py_mt_queue_pd_prog_numpy import (
    vanilla_py_mt_queue_pd_prog_numpy,
)
from src.challenges.vanilla.strategies.single_threaded.vanilla_py_st_pd_grp_numba import vanilla_py_st_pd_grp_numba
from src.challenges.vanilla.strategies.single_threaded.vanilla_py_st_pd_grp_numpy import vanilla_py_st_pd_grp_numpy
from src.challenges.vanilla.strategies.single_threaded.vanilla_py_st_pd_prog_numpy import vanilla_py_st_pd_prog_numpy

VANILLA_STRATEGIES_USING_PYTHON_ONLY_REGISTRY: list[ChallengeMethodPythonOnlyRegistration] = [
    ChallengeMethodPythonOnlyRegistration(
        strategy_name_2018=None,
        strategy_name=name_of_function(vanilla_py_mt_queue_pd_prog_numpy),
        language=SolutionLanguage.PYTHON,
        engine=CalcEngine.PYTHON_ONLY,
        interface=SolutionInterfacePythonOnly.QUEUES,
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        requires_gpu=False,
        delegate=vanilla_py_mt_queue_pd_prog_numpy,
    ),
    ChallengeMethodPythonOnlyRegistration(
        strategy_name_2018=None,
        strategy_name=name_of_function(vanilla_py_st_pd_grp_numba),
        language=SolutionLanguage.PYTHON,
        engine=CalcEngine.PYTHON_ONLY,
        interface=SolutionInterfacePythonOnly.PANDAS,
        numerical_tolerance=NumericalToleranceExpectations.NUMBA,
        requires_gpu=True,
        delegate=vanilla_py_st_pd_grp_numba,
    ),
    ChallengeMethodPythonOnlyRegistration(
        strategy_name_2018=None,
        strategy_name=name_of_function(vanilla_py_st_pd_grp_numpy),
        language=SolutionLanguage.PYTHON,
        engine=CalcEngine.PYTHON_ONLY,
        interface=SolutionInterfacePythonOnly.PANDAS,
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        requires_gpu=False,
        delegate=vanilla_py_st_pd_grp_numpy,
    ),
    ChallengeMethodPythonOnlyRegistration(
        strategy_name_2018=None,
        strategy_name=name_of_function(vanilla_py_st_pd_prog_numpy),
        language=SolutionLanguage.PYTHON,
        engine=CalcEngine.PYTHON_ONLY,
        interface=SolutionInterfacePythonOnly.PROGRESSIVE,
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        requires_gpu=False,
        delegate=vanilla_py_st_pd_prog_numpy,
    ),
]
