from spark_agg_methods_common_python.challenges.sectional.sectional_test_data_types import SolutionScale
from spark_agg_methods_common_python.perf_test_common import CalcEngine, SolutionInterfacePySpark, SolutionLanguage
from spark_agg_methods_common_python.utils.inspection import name_of_function
from src.challenges.sectional.sectional_test_data_types_pyspark import SectionalChallengeMethodPysparkRegistration
from src.challenges.sectional.strategies.using_pyspark.sectional_pyspark_df_join_grp import (
    sectional_pyspark_df_join_grp,
)
from src.challenges.sectional.strategies.using_pyspark.sectional_pyspark_df_prep_grp_csv import (
    sectional_pyspark_df_prep_grp_csv,
)
from src.challenges.sectional.strategies.using_pyspark.sectional_pyspark_df_prep_txt import (
    sectional_pyspark_df_prep_txt,
)
from src.challenges.sectional.strategies.using_pyspark.sectional_pyspark_rdd_join_mappart import (
    sectional_pyspark_rdd_join_mappart,
)
from src.challenges.sectional.strategies.using_pyspark.sectional_pyspark_rdd_mappart_odd_even import (
    sectional_pyspark_rdd_mappart_odd_even,
)
from src.challenges.sectional.strategies.using_pyspark.sectional_pyspark_rdd_mappart_partials import (
    sectional_pyspark_rdd_mappart_partials,
)
from src.challenges.sectional.strategies.using_pyspark.sectional_pyspark_rdd_mappart_single_threaded import (
    sectional_pyspark_rdd_mappart_single_threaded,
)
from src.challenges.sectional.strategies.using_pyspark.sectional_pyspark_rdd_prep_mappart import (
    sectional_pyspark_rdd_prep_mappart,
)
from src.challenges.sectional.strategies.using_pyspark.sectional_pyspark_rdd_reduce_asymm_part import (
    sectional_pyspark_rdd_reduce_asymm_part,
)

SECTIONAL_STRATEGY_REGISTRY_PYSPARK: list[SectionalChallengeMethodPysparkRegistration] = [
    SectionalChallengeMethodPysparkRegistration(
        strategy_name_2018="section_mappart_single_threaded",
        strategy_name_2023="section_pyspark_rdd_mappart_single_threaded",
        strategy_name=name_of_function(sectional_pyspark_rdd_mappart_single_threaded),
        language=SolutionLanguage.PYTHON,
        engine=CalcEngine.PYSPARK,
        interface=SolutionInterfacePySpark.PYSPARK_RDD,
        scale=SolutionScale.WHOLE_FILE,
        requires_gpu=False,
        delegate=sectional_pyspark_rdd_mappart_single_threaded
    ),
    SectionalChallengeMethodPysparkRegistration(
        strategy_name_2018="section_mappart_odd_even",
        strategy_name_2023="section_pyspark_rdd_mappart_odd_even",
        strategy_name=name_of_function(sectional_pyspark_rdd_mappart_odd_even),
        language=SolutionLanguage.PYTHON,
        engine=CalcEngine.PYSPARK,
        interface=SolutionInterfacePySpark.PYSPARK_RDD,
        scale=SolutionScale.WHOLE_FILE,
        requires_gpu=False,
        delegate=sectional_pyspark_rdd_mappart_odd_even
    ),
    SectionalChallengeMethodPysparkRegistration(
        strategy_name_2018="section_mappart_partials",
        strategy_name_2023="section_pyspark_rdd_mappart_partials",
        strategy_name=name_of_function(sectional_pyspark_rdd_mappart_partials),
        language=SolutionLanguage.PYTHON,
        engine=CalcEngine.PYSPARK,
        interface=SolutionInterfacePySpark.PYSPARK_RDD,
        scale=SolutionScale.THREE_ROWS,
        requires_gpu=False,
        delegate=sectional_pyspark_rdd_mappart_partials
    ),
    SectionalChallengeMethodPysparkRegistration(
        strategy_name_2018="section_asymreduce_partials",
        strategy_name_2023="section_pyspark_rdd_reduce_asymm_part",
        strategy_name=name_of_function(sectional_pyspark_rdd_reduce_asymm_part),
        language=SolutionLanguage.PYTHON,
        engine=CalcEngine.PYSPARK,
        interface=SolutionInterfacePySpark.PYSPARK_RDD,
        scale=SolutionScale.FINAL_SUMMARIES,
        requires_gpu=False,
        delegate=sectional_pyspark_rdd_reduce_asymm_part
    ),
    SectionalChallengeMethodPysparkRegistration(
        strategy_name_2018="section_prep_mappart",
        strategy_name_2023="section_pyspark_rdd_prep_mappart",
        strategy_name=name_of_function(sectional_pyspark_rdd_prep_mappart),
        language=SolutionLanguage.PYTHON,
        engine=CalcEngine.PYSPARK,
        interface=SolutionInterfacePySpark.PYSPARK_RDD,
        scale=SolutionScale.WHOLE_SECTION,
        requires_gpu=False,
        delegate=sectional_pyspark_rdd_prep_mappart
    ),
    SectionalChallengeMethodPysparkRegistration(
        strategy_name_2018="section_prep_groupby",
        strategy_name_2023="section_pyspark_df_prep_txt",
        strategy_name=name_of_function(sectional_pyspark_df_prep_txt),
        language=SolutionLanguage.PYTHON,
        engine=CalcEngine.PYSPARK,
        interface=SolutionInterfacePySpark.PYSPARK_DATAFRAME_FLUENT,
        scale=SolutionScale.WHOLE_SECTION,
        requires_gpu=False,
        delegate=sectional_pyspark_df_prep_txt
    ),
    SectionalChallengeMethodPysparkRegistration(
        strategy_name_2018="section_prepcsv_groupby",
        strategy_name_2023="section_pyspark_df_prep_grp_csv",
        strategy_name=name_of_function(sectional_pyspark_df_prep_grp_csv),
        language=SolutionLanguage.PYTHON,
        engine=CalcEngine.PYSPARK,
        interface=SolutionInterfacePySpark.PYSPARK_DATAFRAME_FLUENT,
        scale=SolutionScale.WHOLE_SECTION,
        requires_gpu=False,
        delegate=sectional_pyspark_df_prep_grp_csv
    ),
    SectionalChallengeMethodPysparkRegistration(
        strategy_name_2018="section_join_groupby",
        strategy_name_2023=None,
        strategy_name=name_of_function(sectional_pyspark_df_join_grp),
        language=SolutionLanguage.PYTHON,
        engine=CalcEngine.PYSPARK,
        interface=SolutionInterfacePySpark.PYSPARK_DATAFRAME_FLUENT,
        scale=SolutionScale.WHOLE_SECTION,
        requires_gpu=False,
        delegate=sectional_pyspark_df_join_grp
    ),
    SectionalChallengeMethodPysparkRegistration(
        strategy_name_2018="section_join_mappart",
        strategy_name_2023="section_pyspark_rdd_join_mappart",
        strategy_name=name_of_function(sectional_pyspark_rdd_join_mappart),
        language=SolutionLanguage.PYTHON,
        engine=CalcEngine.PYSPARK,
        interface=SolutionInterfacePySpark.PYSPARK_RDD,
        scale=SolutionScale.WHOLE_SECTION,
        requires_gpu=False,
        delegate=sectional_pyspark_rdd_join_mappart
    ),
]
