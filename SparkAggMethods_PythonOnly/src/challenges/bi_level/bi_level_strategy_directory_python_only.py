from spark_agg_methods_common_python.perf_test_common import (
    CalcEngine, NumericalToleranceExpectations, SolutionInterfacePythonOnly, SolutionLanguage,
)
from spark_agg_methods_common_python.utils.inspection import name_of_function

from src.challenges.bi_level.strategies.using_python_only.bi_level_py_only_pd_grp_numpy import (
    bi_level_py_only_pd_grp_numpy,
)
from src.challenges.six_field_test_data.six_test_data_for_python_only import ChallengeMethodPythonOnlyRegistration

BI_LEVEL_STRATEGIES_USING_PYTHON_ONLY_REGISTRY: list[ChallengeMethodPythonOnlyRegistration] = [
    ChallengeMethodPythonOnlyRegistration(
        strategy_name_2018=None,
        strategy_name=name_of_function(bi_level_py_only_pd_grp_numpy),
        language=SolutionLanguage.PYTHON,
        engine=CalcEngine.PYTHON_ONLY,
        interface=SolutionInterfacePythonOnly.PANDAS,
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        requires_gpu=False,
        delegate=bi_level_py_only_pd_grp_numpy,
    ),
]
