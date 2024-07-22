from src.challenges.sectional.section_test_data_types import (
    ChallengeMethodDaskRegistration, ChallengeMethodPysparkRegistration,
    ChallengeMethodPythonOnlyRegistration)
from src.challenges.sectional.using_pyspark.section_nospark_single_threaded import \
    section_nospark_single_threaded
from src.challenges.sectional.using_pyspark.section_pyspark_df_join_grp import \
    section_join_groupby
from src.challenges.sectional.using_pyspark.section_pyspark_df_prep_grp_csv import \
    section_pyspark_df_prep_grp_csv
from src.challenges.sectional.using_pyspark.section_pyspark_df_prep_txt import \
    section_pyspark_df_prep_txt
from src.challenges.sectional.using_pyspark.section_pyspark_rdd_join_mappart import \
    section_pyspark_rdd_join_mappart
from src.challenges.sectional.using_pyspark.section_pyspark_rdd_mappart_odd_even import \
    section_pyspark_rdd_mappart_odd_even
from src.challenges.sectional.using_pyspark.section_pyspark_rdd_mappart_partials import \
    section_pyspark_rdd_mappart_partials
from src.challenges.sectional.using_pyspark.section_pyspark_rdd_mappart_single_threaded import \
    section_pyspark_rdd_mappart_single_threaded
from src.challenges.sectional.using_pyspark.section_pyspark_rdd_prep_mappart import \
    section_pyspark_rdd_prep_mappart
from src.challenges.sectional.using_pyspark.section_pyspark_rdd_reduce_asymm_part import \
    section_pyspark_rdd_reduce_asymm_part
from src.challenges.sectional.using_python_only.section_py_only_single_threaded import \
    section_py_only_single_threaded
from src.utils.inspection import name_of_function

STRATEGIES_USING_DASK_REGISTRY: list[ChallengeMethodDaskRegistration] = [
]

STRATEGIES_USING_PYSPARK_REGISTRY: list[ChallengeMethodPysparkRegistration] = [
    ChallengeMethodPysparkRegistration(
        original_strategy_name='section_nospark_single_threaded',
        strategy_name=name_of_function(section_nospark_single_threaded),
        language='python',
        interface='python',
        scale='singleline',
        requires_gpu=False,
        delegate=section_nospark_single_threaded
    ),
    ChallengeMethodPysparkRegistration(
        original_strategy_name='section_mappart_single_threaded',
        strategy_name=name_of_function(section_pyspark_rdd_mappart_single_threaded),
        language='python',
        interface='rdd',
        scale='whole_file',
        requires_gpu=False,
        delegate=section_pyspark_rdd_mappart_single_threaded
    ),
    ChallengeMethodPysparkRegistration(
        original_strategy_name='section_mappart_odd_even',
        strategy_name=name_of_function(section_pyspark_rdd_mappart_odd_even),
        language='python',
        interface='rdd',
        scale='whole_section',

        requires_gpu=False,
        delegate=section_pyspark_rdd_mappart_odd_even
    ),
    ChallengeMethodPysparkRegistration(
        original_strategy_name='section_mappart_partials',
        strategy_name=name_of_function(section_pyspark_rdd_mappart_partials),
        language='python',
        interface='rdd',
        scale='three_rows',

        requires_gpu=False,
        delegate=section_pyspark_rdd_mappart_partials
    ),
    ChallengeMethodPysparkRegistration(
        original_strategy_name='section_asymreduce_partials',
        strategy_name=name_of_function(section_pyspark_rdd_reduce_asymm_part),
        language='python',
        interface='rdd',
        scale='final_summaries',

        requires_gpu=False,
        delegate=section_pyspark_rdd_reduce_asymm_part
    ),
    ChallengeMethodPysparkRegistration(
        original_strategy_name='section_prep_mappart',
        strategy_name=name_of_function(section_pyspark_rdd_prep_mappart),
        language='python',
        interface='rdd',
        scale='whole_section',

        requires_gpu=False,
        delegate=section_pyspark_rdd_prep_mappart
    ),
    ChallengeMethodPysparkRegistration(
        original_strategy_name='section_prep_groupby',
        strategy_name=name_of_function(section_pyspark_df_prep_txt),
        language='python',
        interface='df',
        scale='whole_section',

        requires_gpu=False,
        delegate=section_pyspark_df_prep_txt
    ),
    ChallengeMethodPysparkRegistration(
        original_strategy_name='section_prepcsv_groupby',
        strategy_name=name_of_function(section_pyspark_df_prep_grp_csv),
        language='python',
        interface='df',
        scale='whole_section',

        requires_gpu=False,
        delegate=section_pyspark_df_prep_grp_csv
    ),
    ChallengeMethodPysparkRegistration(
        original_strategy_name='section_join_groupby',
        strategy_name=name_of_function(section_join_groupby),
        language='python',
        interface='df',
        scale='whole_section',

        requires_gpu=False,
        delegate=section_join_groupby
    ),
    ChallengeMethodPysparkRegistration(
        original_strategy_name='section_join_mappart',
        strategy_name=name_of_function(section_pyspark_rdd_join_mappart),
        language='python',
        interface='rdd',
        scale='whole_section',
        requires_gpu=False,
        delegate=section_pyspark_rdd_join_mappart
    ),
]

STRATEGIES_USING_PYTHON_ONLY_REGISTRY: list[ChallengeMethodPythonOnlyRegistration] = [
    ChallengeMethodPythonOnlyRegistration(
        strategy_name=name_of_function(section_py_only_single_threaded),
        language='python',
        interface='python',
        scale='singleline',
        requires_gpu=False,
        delegate=section_py_only_single_threaded
    ),
]
