from typing import List

from challenges.sectional.section_test_data_types import (DaskTestMethod,
                                                          PysparkTestMethod)
from challenges.sectional.using_pyspark.section_nospark_single_threaded import \
    section_nospark_single_threaded
from challenges.sectional.using_pyspark.section_pyspark_df_join_grp import \
    section_join_groupby
from challenges.sectional.using_pyspark.section_pyspark_df_prep_grp_csv import \
    section_pyspark_df_prep_grp_csv
from challenges.sectional.using_pyspark.section_pyspark_df_prep_txt import \
    section_pyspark_df_prep_txt
from challenges.sectional.using_pyspark.section_pyspark_rdd_join_mappart import \
    section_pyspark_rdd_join_mappart
from challenges.sectional.using_pyspark.section_pyspark_rdd_mappart_odd_even import \
    section_pyspark_rdd_mappart_odd_even
from challenges.sectional.using_pyspark.section_pyspark_rdd_mappart_partials import \
    section_pyspark_rdd_mappart_partials
from challenges.sectional.using_pyspark.section_pyspark_rdd_mappart_single_threaded import \
    section_pyspark_rdd_mappart_single_threaded
from challenges.sectional.using_pyspark.section_pyspark_rdd_prep_mappart import \
    section_pyspark_rdd_prep_mappart
from challenges.sectional.using_pyspark.section_pyspark_rdd_reduce_asymm_part import \
    section_pyspark_rdd_reduce_asymm_part
from utils.inspection import nameof

dask_implementation_list: List[DaskTestMethod] = []
pyspark_implementation_list: List[PysparkTestMethod] = [
    PysparkTestMethod(
        strategy_name=nameof(section_nospark_single_threaded),
        language='python',
        interface='python',
        scale='singleline',
        delegate=section_nospark_single_threaded
    ),
    PysparkTestMethod(
        strategy_name=nameof(section_pyspark_rdd_mappart_single_threaded),
        language='python',
        interface='rdd',
        scale='wholefile',
        delegate=section_pyspark_rdd_mappart_single_threaded
    ),
    PysparkTestMethod(
        strategy_name=nameof(section_pyspark_rdd_mappart_odd_even),
        language='python',
        interface='rdd',
        scale='wholesection',
        delegate=section_pyspark_rdd_mappart_odd_even
    ),
    PysparkTestMethod(
        strategy_name=nameof(section_pyspark_rdd_mappart_partials),
        language='python',
        interface='rdd',
        scale='threerows',
        delegate=section_pyspark_rdd_mappart_partials
    ),
    PysparkTestMethod(
        strategy_name=nameof(section_pyspark_rdd_reduce_asymm_part),
        language='python',
        interface='rdd',
        scale='finalsummaries',
        delegate=section_pyspark_rdd_reduce_asymm_part
    ),
    PysparkTestMethod(
        strategy_name=nameof(section_pyspark_rdd_prep_mappart),
        language='python',
        interface='rdd',
        scale='wholesection',
        delegate=section_pyspark_rdd_prep_mappart
    ),
    PysparkTestMethod(
        strategy_name=nameof(section_pyspark_df_prep_txt),
        language='python',
        interface='df',
        scale='wholesection',
        delegate=section_pyspark_df_prep_txt
    ),
    PysparkTestMethod(
        strategy_name=nameof(section_pyspark_df_prep_grp_csv),
        language='python',
        interface='df',
        scale='wholesection',
        delegate=section_pyspark_df_prep_grp_csv
    ),
    PysparkTestMethod(
        strategy_name=nameof(section_join_groupby),
        language='python',
        interface='df',
        scale='wholesection',
        delegate=section_join_groupby
    ),
    PysparkTestMethod(
        strategy_name=nameof(section_pyspark_rdd_join_mappart),
        language='python',
        interface='rdd',
        scale='wholesection',
        delegate=section_pyspark_rdd_join_mappart
    ),
]


strategy_name_list = [x.strategy_name for x in pyspark_implementation_list]
