from spark_agg_methods_common_python.challenges.sectional.section_test_data_types import SolutionScale
from spark_agg_methods_common_python.perf_test_common import CalcEngine, SolutionInterfacePythonOnly, SolutionLanguage
from spark_agg_methods_common_python.utils.inspection import name_of_function

from src.challenges.sectional.section_test_data_types_py_only import SectionChallengeMethodPythonOnlyRegistration
from src.challenges.sectional.strategies.section_py_only_single_threaded import section_py_only_single_threaded

SECTIONAL_STRATEGIES_USING_PYTHON_ONLY_REGISTRY: list[SectionChallengeMethodPythonOnlyRegistration] = [
    SectionChallengeMethodPythonOnlyRegistration(
        strategy_name_2018='section_nospark_single_threaded',
        strategy_name=name_of_function(section_py_only_single_threaded),
        language=SolutionLanguage.PYTHON,
        engine=CalcEngine.PYTHON_ONLY,
        interface=SolutionInterfacePythonOnly.PYTHON,
        scale=SolutionScale.SINGLE_LINE,
        requires_gpu=False,
        delegate=section_py_only_single_threaded
    ),
]
