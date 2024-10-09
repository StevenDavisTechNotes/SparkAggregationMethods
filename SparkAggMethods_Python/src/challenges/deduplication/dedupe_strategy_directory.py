from src.challenges.deduplication.dedupe_test_data_types import ChallengeMethodPythonPysparkRegistration
from src.challenges.deduplication.using_pyspark.dedupe_pyspark_df_nested_pandas import dedupe_pyspark_df_nested_pandas
from src.challenges.deduplication.using_pyspark.dedupe_pyspark_df_nested_python import dedupe_pyspark_df_nested_python
from src.challenges.deduplication.using_pyspark.dedupe_pyspark_df_nested_w_col import dedupe_pyspark_df_nested_w_col
from src.challenges.deduplication.using_pyspark.dedupe_pyspark_df_window import dedupe_pyspark_df_window
from src.challenges.deduplication.using_pyspark.dedupe_pyspark_rdd_grp import dedupe_pyspark_rdd_grp
from src.challenges.deduplication.using_pyspark.dedupe_pyspark_rdd_map_part import dedupe_pyspark_rdd_map_part
from src.challenges.deduplication.using_pyspark.dedupe_pyspark_rdd_reduce import dedupe_pyspark_rdd_reduce
from src.perf_test_common import CalcEngine, SolutionInterfacePySpark, SolutionLanguage
from src.utils.inspection import name_of_function

# STRATEGIES_USING_DASK_REGISTRY: list[ChallengeMethodPythonDaskRegistration] = [
# ]

STRATEGIES_USING_PYSPARK_REGISTRY: list[ChallengeMethodPythonPysparkRegistration] = [
    ChallengeMethodPythonPysparkRegistration(
        strategy_name_2018='dedupe_pandas',
        strategy_name=name_of_function(dedupe_pyspark_df_nested_pandas),
        language=SolutionLanguage.PYTHON,
        engine=CalcEngine.PYSPARK,
        interface=SolutionInterfacePySpark.PYSPARK_DATAFRAME_PANDAS,
        requires_gpu=False,
        delegate=dedupe_pyspark_df_nested_pandas,
    ),
    ChallengeMethodPythonPysparkRegistration(
        strategy_name_2018='dedupe_fluent_nested_python',
        strategy_name=name_of_function(dedupe_pyspark_df_nested_python),
        language=SolutionLanguage.PYTHON,
        engine=CalcEngine.PYSPARK,
        interface=SolutionInterfacePySpark.PYSPARK_DATAFRAME_FLUENT,
        requires_gpu=False,
        delegate=dedupe_pyspark_df_nested_python,
    ),
    ChallengeMethodPythonPysparkRegistration(
        strategy_name_2018='dedupe_fluent_nested_withCol',
        strategy_name=name_of_function(dedupe_pyspark_df_nested_w_col),
        language=SolutionLanguage.PYTHON,
        engine=CalcEngine.PYSPARK,
        interface=SolutionInterfacePySpark.PYSPARK_DATAFRAME_FLUENT,
        requires_gpu=False,
        delegate=dedupe_pyspark_df_nested_w_col,
    ),
    ChallengeMethodPythonPysparkRegistration(
        strategy_name_2018='dedupe_fluent_windows',
        strategy_name=name_of_function(dedupe_pyspark_df_window),
        language=SolutionLanguage.PYTHON,
        engine=CalcEngine.PYSPARK,
        interface=SolutionInterfacePySpark.PYSPARK_DATAFRAME_FLUENT,
        requires_gpu=False,
        delegate=dedupe_pyspark_df_window,
    ),
    ChallengeMethodPythonPysparkRegistration(
        strategy_name_2018='dedupe_rdd_groupby',
        strategy_name=name_of_function(dedupe_pyspark_rdd_grp),
        language=SolutionLanguage.PYTHON,
        engine=CalcEngine.PYSPARK,
        interface=SolutionInterfacePySpark.PYSPARK_RDD,
        requires_gpu=False,
        delegate=dedupe_pyspark_rdd_grp,
    ),
    ChallengeMethodPythonPysparkRegistration(
        strategy_name_2018='dedupe_rdd_mappart',
        strategy_name=name_of_function(dedupe_pyspark_rdd_map_part),
        language=SolutionLanguage.PYTHON,
        engine=CalcEngine.PYSPARK,
        interface=SolutionInterfacePySpark.PYSPARK_RDD,
        requires_gpu=False,
        delegate=dedupe_pyspark_rdd_map_part,
    ),
    ChallengeMethodPythonPysparkRegistration(
        strategy_name_2018='dedupe_rdd_reduce',
        strategy_name=name_of_function(dedupe_pyspark_rdd_reduce),
        language=SolutionLanguage.PYTHON,
        engine=CalcEngine.PYSPARK,
        interface=SolutionInterfacePySpark.PYSPARK_RDD,
        requires_gpu=False,
        delegate=dedupe_pyspark_rdd_reduce,
    ),
]

# STRATEGIES_USING_PYTHON_ONLY_REGISTRY: list[ChallengeMethodPythonOnlyRegistration] = [
# ]
