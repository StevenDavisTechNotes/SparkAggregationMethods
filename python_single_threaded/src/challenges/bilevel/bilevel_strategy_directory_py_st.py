from spark_agg_methods_common_python.perf_test_common import (
    CalcEngine, NumericalToleranceExpectations, SolutionInterfacePythonST, SolutionLanguage,
)
from spark_agg_methods_common_python.utils.inspection import name_of_function
from src.challenges.bilevel.strategies.single_threaded.bilevel_py_st_nb_grp_kernel import bilevel_py_st_nb_grp_kernel
from src.challenges.bilevel.strategies.single_threaded.bilevel_py_st_np_grp_reduceat import (
    bilevel_py_st_np_grp_reduceat,
)
from src.challenges.bilevel.strategies.single_threaded.bilevel_py_st_pa_grp_batches import bilevel_py_st_pa_grp_batches
from src.challenges.bilevel.strategies.single_threaded.bilevel_py_st_pd_grp_agg import bilevel_py_st_pd_grp_agg
from src.challenges.bilevel.strategies.single_threaded.bilevel_py_st_pd_grp_numba import bilevel_py_st_pd_grp_numba
from src.challenges.bilevel.strategies.single_threaded.bilevel_py_st_pd_grp_numpy import bilevel_py_st_pd_grp_numpy
from src.challenges.bilevel.strategies.single_threaded.bilevel_py_st_pd_prog_numpy import bilevel_py_st_pd_prog_numpy
from src.challenges.bilevel.strategies.single_threaded.bilevel_py_st_pl_grp_scan import bilevel_py_st_pl_grp_scan
from src.challenges.bilevel.strategies.single_threaded.bilevel_py_st_pl_grp_single import bilevel_py_st_pl_grp_single
from src.challenges.bilevel.strategies.single_threaded.bilevel_py_st_pl_grp_stripe import bilevel_py_st_pl_grp_stripe
from src.challenges.six_field_test_data.six_test_data_for_py_st import ChallengeMethodPythonSingleThreadedRegistration

BILEVEL_STRATEGY_REGISTRY_PYTHON_SINGLE_THREADED: list[ChallengeMethodPythonSingleThreadedRegistration] = [
    ChallengeMethodPythonSingleThreadedRegistration(
        strategy_name_2018=None,
        strategy_name_2023=None,
        strategy_name=name_of_function(bilevel_py_st_pd_grp_numba),
        language=SolutionLanguage.PYTHON,
        engine=CalcEngine.SINGLE_THREADED,
        interface=SolutionInterfacePythonST.PANDAS,
        numerical_tolerance=NumericalToleranceExpectations.NUMBA,
        requires_gpu=True,
        delegate=bilevel_py_st_pd_grp_numba,
    ),
    ChallengeMethodPythonSingleThreadedRegistration(
        strategy_name_2018=None,
        strategy_name_2023="bi_level_py_st_pd_grp_numpy",
        strategy_name=name_of_function(bilevel_py_st_pd_grp_numpy),
        language=SolutionLanguage.PYTHON,
        engine=CalcEngine.SINGLE_THREADED,
        interface=SolutionInterfacePythonST.PANDAS,
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        requires_gpu=False,
        delegate=bilevel_py_st_pd_grp_numpy,
    ),
    ChallengeMethodPythonSingleThreadedRegistration(
        strategy_name_2018=None,
        strategy_name_2023=None,
        strategy_name=name_of_function(bilevel_py_st_pl_grp_single),
        language=SolutionLanguage.PYTHON,
        engine=CalcEngine.SINGLE_THREADED,
        interface=SolutionInterfacePythonST.POLARS,
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        requires_gpu=False,
        delegate=bilevel_py_st_pl_grp_single,
    ),
    ChallengeMethodPythonSingleThreadedRegistration(
        strategy_name_2018=None,
        strategy_name_2023=None,
        strategy_name=name_of_function(bilevel_py_st_pl_grp_scan),
        language=SolutionLanguage.PYTHON,
        engine=CalcEngine.SINGLE_THREADED,
        interface=SolutionInterfacePythonST.POLARS,
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        requires_gpu=False,
        delegate=bilevel_py_st_pl_grp_scan,
    ),
    ChallengeMethodPythonSingleThreadedRegistration(
        strategy_name_2018=None,
        strategy_name_2023=None,
        strategy_name=name_of_function(bilevel_py_st_pl_grp_stripe),
        language=SolutionLanguage.PYTHON,
        engine=CalcEngine.SINGLE_THREADED,
        interface=SolutionInterfacePythonST.POLARS,
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        requires_gpu=False,
        delegate=bilevel_py_st_pl_grp_stripe,
    ),
    ChallengeMethodPythonSingleThreadedRegistration(
        strategy_name_2018=None,
        strategy_name_2023=None,
        strategy_name=name_of_function(bilevel_py_st_pd_grp_agg),
        language=SolutionLanguage.PYTHON,
        engine=CalcEngine.SINGLE_THREADED,
        interface=SolutionInterfacePythonST.PANDAS,
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        requires_gpu=False,
        delegate=bilevel_py_st_pd_grp_agg,
    ),
    ChallengeMethodPythonSingleThreadedRegistration(
        strategy_name_2018=None,
        strategy_name_2023=None,
        strategy_name=name_of_function(bilevel_py_st_np_grp_reduceat),
        language=SolutionLanguage.PYTHON,
        engine=CalcEngine.SINGLE_THREADED,
        interface=SolutionInterfacePythonST.NUMPY,
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        requires_gpu=False,
        delegate=bilevel_py_st_np_grp_reduceat,
    ),
    ChallengeMethodPythonSingleThreadedRegistration(
        strategy_name_2018=None,
        strategy_name_2023=None,
        strategy_name=name_of_function(bilevel_py_st_nb_grp_kernel),
        language=SolutionLanguage.PYTHON,
        engine=CalcEngine.SINGLE_THREADED,
        interface=SolutionInterfacePythonST.NUMBA,
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        requires_gpu=False,
        delegate=bilevel_py_st_nb_grp_kernel,
    ),
    ChallengeMethodPythonSingleThreadedRegistration(
        strategy_name_2018=None,
        strategy_name_2023=None,
        strategy_name=name_of_function(bilevel_py_st_pa_grp_batches),
        language=SolutionLanguage.PYTHON,
        engine=CalcEngine.SINGLE_THREADED,
        interface=SolutionInterfacePythonST.PYARROW,
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        requires_gpu=False,
        delegate=bilevel_py_st_pa_grp_batches,
    ),
    ChallengeMethodPythonSingleThreadedRegistration(
        strategy_name_2018=None,
        strategy_name_2023="bi_level_py_st_pd_prog_numpy",
        strategy_name=name_of_function(bilevel_py_st_pd_prog_numpy),
        language=SolutionLanguage.PYTHON,
        engine=CalcEngine.SINGLE_THREADED,
        interface=SolutionInterfacePythonST.PROGRESSIVE,
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        requires_gpu=False,
        delegate=bilevel_py_st_pd_prog_numpy,
    ),
]
