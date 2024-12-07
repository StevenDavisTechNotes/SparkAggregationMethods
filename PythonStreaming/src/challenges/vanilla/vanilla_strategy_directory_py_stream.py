from spark_agg_methods_common_python.perf_test_common import (
    CalcEngine, NumericalToleranceExpectations,
    SolutionInterfacePythonStreaming, SolutionLanguage,
)
from spark_agg_methods_common_python.utils.inspection import name_of_function

from src.challenges.six_field_test_data.six_test_data_for_py_stream import (
    ChallengeMethodPythonStreamingRegistration,
)
from src.challenges.vanilla.strategies.queue_mediated.vanilla_py_mt_queue_pd_prog_numpy import (
    vanilla_py_mt_queue_pd_prog_numpy,
)
from src.challenges.vanilla.strategies.queue_mediated.vanilla_py_mt_queue_pd_prog_numpy_1_reader import (
    vanilla_py_mt_queue_pd_prog_numpy_1_reader,
)

VANILLA_STRATEGY_REGISTRY_PYTHON_STREAMING: list[ChallengeMethodPythonStreamingRegistration] = [
    ChallengeMethodPythonStreamingRegistration(
        strategy_name_2018=None,
        strategy_name=name_of_function(vanilla_py_mt_queue_pd_prog_numpy),
        language=SolutionLanguage.PYTHON,
        engine=CalcEngine.STREAMING,
        interface=SolutionInterfacePythonStreaming.QUEUES,
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        requires_gpu=False,
        delegate=vanilla_py_mt_queue_pd_prog_numpy,
    ),
    ChallengeMethodPythonStreamingRegistration(
        strategy_name_2018=None,
        strategy_name=name_of_function(vanilla_py_mt_queue_pd_prog_numpy_1_reader),
        language=SolutionLanguage.PYTHON,
        engine=CalcEngine.STREAMING,
        interface=SolutionInterfacePythonStreaming.QUEUES,
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        requires_gpu=False,
        delegate=vanilla_py_mt_queue_pd_prog_numpy_1_reader,
    ),
]
