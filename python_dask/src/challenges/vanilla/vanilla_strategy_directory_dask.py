from spark_agg_methods_common_python.perf_test_common import (
    CalcEngine, NumericalToleranceExpectations, SolutionInterfaceDask, SolutionLanguage,
)
from spark_agg_methods_common_python.utils.inspection import name_of_function
from src.challenges.six_field_test_data.six_test_data_for_dask import ChallengeMethodPythonDaskRegistration
from src.challenges.vanilla.strategies.using_dask.vanilla_dask_actors_accumulate import vanilla_dask_actors_accumulate
from src.challenges.vanilla.strategies.using_dask.vanilla_dask_bag_accumulate import vanilla_dask_bag_accumulate
from src.challenges.vanilla.strategies.using_dask.vanilla_dask_bag_fold import vanilla_dask_bag_fold
from src.challenges.vanilla.strategies.using_dask.vanilla_dask_bag_foldby import vanilla_dask_bag_foldby
from src.challenges.vanilla.strategies.using_dask.vanilla_dask_bag_map_partitions import vanilla_dask_bag_map_partitions
from src.challenges.vanilla.strategies.using_dask.vanilla_dask_bag_reduction import vanilla_dask_bag_reduction
from src.challenges.vanilla.strategies.using_dask.vanilla_dask_ddf_grp_apply import vanilla_dask_ddf_grp_apply
from src.challenges.vanilla.strategies.using_dask.vanilla_dask_ddf_grp_udaf import vanilla_dask_ddf_grp_udaf
from src.challenges.vanilla.strategies.using_dask.vanilla_dask_ddf_shuffle_local import vanilla_dask_ddf_shuffle_local
from src.challenges.vanilla.strategies.using_dask.vanilla_dask_delayed_pl_chunks import vanilla_dask_delayed_pl_chunks
from src.challenges.vanilla.strategies.using_dask.vanilla_dask_futures_as_completed import (
    vanilla_dask_futures_as_completed,
)

VANILLA_STRATEGY_REGISTRY_DASK: list[ChallengeMethodPythonDaskRegistration] = [
    ChallengeMethodPythonDaskRegistration(
        strategy_name_2018=None,
        strategy_name_2023="vanilla_dask_bag_accumulate",
        strategy_name=name_of_function(vanilla_dask_bag_accumulate),
        language=SolutionLanguage.PYTHON,
        engine=CalcEngine.DASK,
        interface=SolutionInterfaceDask.DASK_BAG,
        numerical_tolerance=NumericalToleranceExpectations.SIMPLE_SUM,
        requires_gpu=False,
        delegate=vanilla_dask_bag_accumulate,
    ),
    ChallengeMethodPythonDaskRegistration(
        strategy_name_2018=None,
        strategy_name_2023="vanilla_dask_bag_fold",
        strategy_name=name_of_function(vanilla_dask_bag_fold),
        language=SolutionLanguage.PYTHON,
        engine=CalcEngine.DASK,
        interface=SolutionInterfaceDask.DASK_BAG,
        numerical_tolerance=NumericalToleranceExpectations.SIMPLE_SUM,
        requires_gpu=False,
        delegate=vanilla_dask_bag_fold,
    ),
    ChallengeMethodPythonDaskRegistration(
        strategy_name_2018=None,
        strategy_name_2023="vanilla_dask_bag_foldby",
        strategy_name=name_of_function(vanilla_dask_bag_foldby),
        language=SolutionLanguage.PYTHON,
        engine=CalcEngine.DASK,
        interface=SolutionInterfaceDask.DASK_BAG,
        numerical_tolerance=NumericalToleranceExpectations.SIMPLE_SUM,
        requires_gpu=False,
        delegate=vanilla_dask_bag_foldby,
    ),
    ChallengeMethodPythonDaskRegistration(
        strategy_name_2018=None,
        strategy_name_2023="vanilla_dask_bag_map_partitions",
        strategy_name=name_of_function(vanilla_dask_bag_map_partitions),
        language=SolutionLanguage.PYTHON,
        engine=CalcEngine.DASK,
        interface=SolutionInterfaceDask.DASK_BAG,
        numerical_tolerance=NumericalToleranceExpectations.SIMPLE_SUM,
        requires_gpu=False,
        delegate=vanilla_dask_bag_map_partitions,
    ),
    ChallengeMethodPythonDaskRegistration(
        strategy_name_2018=None,
        strategy_name_2023="vanilla_dask_bag_reduction",
        strategy_name=name_of_function(vanilla_dask_bag_reduction),
        language=SolutionLanguage.PYTHON,
        engine=CalcEngine.DASK,
        interface=SolutionInterfaceDask.DASK_BAG,
        numerical_tolerance=NumericalToleranceExpectations.SIMPLE_SUM,
        requires_gpu=False,
        delegate=vanilla_dask_bag_reduction,
    ),
    ChallengeMethodPythonDaskRegistration(
        strategy_name_2018=None,
        strategy_name_2023="vanilla_dask_ddf_grp_apply",
        strategy_name=name_of_function(vanilla_dask_ddf_grp_apply),
        language=SolutionLanguage.PYTHON,
        engine=CalcEngine.DASK,
        interface=SolutionInterfaceDask.DASK_DATAFRAME,
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        requires_gpu=False,
        delegate=vanilla_dask_ddf_grp_apply,
    ),
    ChallengeMethodPythonDaskRegistration(
        strategy_name_2018=None,
        strategy_name_2023="vanilla_dask_ddf_grp_udaf",
        strategy_name=name_of_function(vanilla_dask_ddf_grp_udaf),
        language=SolutionLanguage.PYTHON,
        engine=CalcEngine.DASK,
        interface=SolutionInterfaceDask.DASK_DATAFRAME,
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        requires_gpu=False,
        delegate=vanilla_dask_ddf_grp_udaf,
    ),
    ChallengeMethodPythonDaskRegistration(
        strategy_name_2018=None,
        strategy_name_2023=None,
        strategy_name=name_of_function(vanilla_dask_delayed_pl_chunks),
        language=SolutionLanguage.PYTHON,
        engine=CalcEngine.DASK,
        interface=SolutionInterfaceDask.DASK_DELAYED,
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        requires_gpu=False,
        delegate=vanilla_dask_delayed_pl_chunks,
    ),
    ChallengeMethodPythonDaskRegistration(
        strategy_name_2018=None,
        strategy_name_2023=None,
        strategy_name=name_of_function(vanilla_dask_ddf_shuffle_local),
        language=SolutionLanguage.PYTHON,
        engine=CalcEngine.DASK,
        interface=SolutionInterfaceDask.DASK_DATAFRAME,
        numerical_tolerance=NumericalToleranceExpectations.SIMPLE_SUM,
        requires_gpu=False,
        requires_cluster=True,
        delegate=vanilla_dask_ddf_shuffle_local,
    ),
    ChallengeMethodPythonDaskRegistration(
        strategy_name_2018=None,
        strategy_name_2023=None,
        strategy_name=name_of_function(vanilla_dask_futures_as_completed),
        language=SolutionLanguage.PYTHON,
        engine=CalcEngine.DASK,
        interface=SolutionInterfaceDask.DASK_FUTURES,
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        requires_gpu=False,
        requires_cluster=True,
        delegate=vanilla_dask_futures_as_completed,
    ),
    ChallengeMethodPythonDaskRegistration(
        strategy_name_2018=None,
        strategy_name_2023=None,
        strategy_name=name_of_function(vanilla_dask_actors_accumulate),
        language=SolutionLanguage.PYTHON,
        engine=CalcEngine.DASK,
        interface=SolutionInterfaceDask.DASK_ACTORS,
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        requires_gpu=False,
        requires_cluster=True,
        delegate=vanilla_dask_actors_accumulate,
    ),
]
