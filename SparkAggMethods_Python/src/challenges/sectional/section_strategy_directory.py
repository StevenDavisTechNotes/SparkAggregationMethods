from src.challenges.sectional.section_test_data_types import (
    ChallengeMethodDaskRegistration, ChallengeMethodPysparkRegistration, ChallengeMethodPythonOnlyRegistration,
    SolutionScale,
)
from src.challenges.sectional.using_pyspark.section_pyspark_df_join_grp import section_join_groupby
from src.challenges.sectional.using_pyspark.section_pyspark_df_prep_grp_csv import section_pyspark_df_prep_grp_csv
from src.challenges.sectional.using_pyspark.section_pyspark_df_prep_txt import section_pyspark_df_prep_txt
from src.challenges.sectional.using_pyspark.section_pyspark_rdd_join_mappart import section_pyspark_rdd_join_mappart
from src.challenges.sectional.using_pyspark.section_pyspark_rdd_mappart_odd_even import (
    section_pyspark_rdd_mappart_odd_even,
)
from src.challenges.sectional.using_pyspark.section_pyspark_rdd_mappart_partials import (
    section_pyspark_rdd_mappart_partials,
)
from src.challenges.sectional.using_pyspark.section_pyspark_rdd_mappart_single_threaded import (
    section_pyspark_rdd_mappart_single_threaded,
)
from src.challenges.sectional.using_pyspark.section_pyspark_rdd_prep_mappart import section_pyspark_rdd_prep_mappart
from src.challenges.sectional.using_pyspark.section_pyspark_rdd_reduce_asymm_part import (
    section_pyspark_rdd_reduce_asymm_part,
)
from src.challenges.sectional.using_python_only.section_py_only_single_threaded import section_py_only_single_threaded
from src.perf_test_common import CalcEngine, SolutionInterfacePySpark, SolutionInterfacePythonOnly, SolutionLanguage
from src.utils.inspection import name_of_function

STRATEGIES_USING_DASK_REGISTRY: list[ChallengeMethodDaskRegistration] = [
]

STRATEGIES_USING_PYSPARK_REGISTRY: list[ChallengeMethodPysparkRegistration] = [
    ChallengeMethodPysparkRegistration(
        strategy_name_2018='section_mappart_single_threaded',
        strategy_name=name_of_function(section_pyspark_rdd_mappart_single_threaded),
        language=SolutionLanguage.PYTHON,
        engine=CalcEngine.PYSPARK,
        interface=SolutionInterfacePySpark.PYSPARK_RDD,
        scale=SolutionScale.WHOLE_FILE,
        requires_gpu=False,
        delegate=section_pyspark_rdd_mappart_single_threaded
    ),
    ChallengeMethodPysparkRegistration(
        strategy_name_2018='section_mappart_odd_even',
        strategy_name=name_of_function(section_pyspark_rdd_mappart_odd_even),
        language=SolutionLanguage.PYTHON,
        engine=CalcEngine.PYSPARK,
        interface=SolutionInterfacePySpark.PYSPARK_RDD,
        scale=SolutionScale.WHOLE_FILE,
        requires_gpu=False,
        delegate=section_pyspark_rdd_mappart_odd_even
    ),
    ChallengeMethodPysparkRegistration(
        strategy_name_2018='section_mappart_partials',
        strategy_name=name_of_function(section_pyspark_rdd_mappart_partials),
        language=SolutionLanguage.PYTHON,
        engine=CalcEngine.PYSPARK,
        interface=SolutionInterfacePySpark.PYSPARK_RDD,
        scale=SolutionScale.THREE_ROWS,
        requires_gpu=False,
        delegate=section_pyspark_rdd_mappart_partials
    ),
    ChallengeMethodPysparkRegistration(
        strategy_name_2018='section_asymreduce_partials',
        strategy_name=name_of_function(section_pyspark_rdd_reduce_asymm_part),
        language=SolutionLanguage.PYTHON,
        engine=CalcEngine.PYSPARK,
        interface=SolutionInterfacePySpark.PYSPARK_RDD,
        scale=SolutionScale.FINAL_SUMMARIES,
        requires_gpu=False,
        delegate=section_pyspark_rdd_reduce_asymm_part
    ),
    ChallengeMethodPysparkRegistration(
        strategy_name_2018='section_prep_mappart',
        strategy_name=name_of_function(section_pyspark_rdd_prep_mappart),
        language=SolutionLanguage.PYTHON,
        engine=CalcEngine.PYSPARK,
        interface=SolutionInterfacePySpark.PYSPARK_RDD,
        scale=SolutionScale.WHOLE_SECTION,
        requires_gpu=False,
        delegate=section_pyspark_rdd_prep_mappart
    ),
    ChallengeMethodPysparkRegistration(
        strategy_name_2018='section_prep_groupby',
        strategy_name=name_of_function(section_pyspark_df_prep_txt),
        language=SolutionLanguage.PYTHON,
        engine=CalcEngine.PYSPARK,
        interface=SolutionInterfacePySpark.PYSPARK_DATAFRAME_FLUENT,
        scale=SolutionScale.WHOLE_SECTION,
        requires_gpu=False,
        delegate=section_pyspark_df_prep_txt
    ),
    ChallengeMethodPysparkRegistration(
        strategy_name_2018='section_prepcsv_groupby',
        strategy_name=name_of_function(section_pyspark_df_prep_grp_csv),
        language=SolutionLanguage.PYTHON,
        engine=CalcEngine.PYSPARK,
        interface=SolutionInterfacePySpark.PYSPARK_DATAFRAME_FLUENT,
        scale=SolutionScale.WHOLE_SECTION,
        requires_gpu=False,
        delegate=section_pyspark_df_prep_grp_csv
    ),
    ChallengeMethodPysparkRegistration(
        strategy_name_2018='section_join_groupby',
        strategy_name=name_of_function(section_join_groupby),
        language=SolutionLanguage.PYTHON,
        engine=CalcEngine.PYSPARK,
        interface=SolutionInterfacePySpark.PYSPARK_DATAFRAME_FLUENT,
        scale=SolutionScale.WHOLE_SECTION,
        requires_gpu=False,
        delegate=section_join_groupby
    ),
    ChallengeMethodPysparkRegistration(
        strategy_name_2018='section_join_mappart',
        strategy_name=name_of_function(section_pyspark_rdd_join_mappart),
        language=SolutionLanguage.PYTHON,
        engine=CalcEngine.PYSPARK,
        interface=SolutionInterfacePySpark.PYSPARK_RDD,
        scale=SolutionScale.WHOLE_SECTION,
        requires_gpu=False,
        delegate=section_pyspark_rdd_join_mappart
    ),
]

STRATEGIES_USING_PYTHON_ONLY_REGISTRY: list[ChallengeMethodPythonOnlyRegistration] = [
    ChallengeMethodPythonOnlyRegistration(
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
