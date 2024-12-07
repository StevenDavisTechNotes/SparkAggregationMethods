from spark_agg_methods_common_python.challenges.sectional.section_test_data_types import SolutionScale
from spark_agg_methods_common_python.perf_test_common import CalcEngine, SolutionInterfacePythonOnly, SolutionLanguage
from spark_agg_methods_common_python.utils.inspection import name_of_function

from src.challenges.sectional.section_test_data_types_py_only import SectionChallengeMethodPythonOnlyRegistration
from src.challenges.sectional.strategies.single_threaded.section_py_st_linear_file_read import (
    section_py_st_linear_file_read,
)

SECTIONAL_STRATEGIES_USING_PYTHON_ONLY_REGISTRY: list[SectionChallengeMethodPythonOnlyRegistration] = [
    SectionChallengeMethodPythonOnlyRegistration(
        strategy_name_2018='section_nospark_single_threaded',
        strategy_name=name_of_function(section_py_st_linear_file_read),
        language=SolutionLanguage.PYTHON,
        engine=CalcEngine.PYTHON_ONLY,
        interface=SolutionInterfacePythonOnly.SINGLE_THREADED,
        scale=SolutionScale.SINGLE_LINE,
        requires_gpu=False,
        delegate=section_py_st_linear_file_read
    ),
]
