from spark_agg_methods_common_python.challenges.sectional.sectional_test_data_types import SolutionScale
from spark_agg_methods_common_python.perf_test_common import CalcEngine, SolutionInterfacePythonST, SolutionLanguage
from spark_agg_methods_common_python.utils.inspection import name_of_function
from src.challenges.sectional.sectional_test_data_types_py_st import (
    SectionalChallengeMethodPythonSingleThreadedRegistration,
)
from src.challenges.sectional.strategies.single_threaded.sectional_py_st_linear_file_read import (
    sectional_py_st_linear_file_read,
)

SECTIONAL_STRATEGY_REGISTRY_PYTHON_SINGLE_THREADED: list[SectionalChallengeMethodPythonSingleThreadedRegistration] = [
    SectionalChallengeMethodPythonSingleThreadedRegistration(
        strategy_name_2018="section_nospark_single_threaded",
        strategy_name_2023="section_py_st_linear_file_read",
        strategy_name=name_of_function(sectional_py_st_linear_file_read),
        language=SolutionLanguage.PYTHON,
        engine=CalcEngine.SINGLE_THREADED,
        interface=SolutionInterfacePythonST.SINGLE_THREADED,
        scale=SolutionScale.SINGLE_LINE,
        requires_gpu=False,
        delegate=sectional_py_st_linear_file_read
    ),
]
