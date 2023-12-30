from typing import List

from challenges.sectional.PySpark_Strategy.SectionFluentPrepGroupBy import \
    section_prep_groupby
from challenges.sectional.PySpark_Strategy.SectionFluentPrepGroupByCSV import \
    section_prepcsv_groupby
from challenges.sectional.PySpark_Strategy.SectionJoinGroupBy import \
    section_join_groupby
from challenges.sectional.PySpark_Strategy.SectionJoinMapPart import \
    section_join_mappart
from challenges.sectional.PySpark_Strategy.SectionNoSparkST import \
    section_nospark_single_threaded
from challenges.sectional.PySpark_Strategy.SectionRddMapPartOddEven import \
    section_mappart_odd_even
from challenges.sectional.PySpark_Strategy.SectionRddMapPartPartials import \
    section_mappart_partials
from challenges.sectional.PySpark_Strategy.SectionRddMapPartST import \
    section_mappart_single_threaded
from challenges.sectional.PySpark_Strategy.SectionRddPrepMapPart import \
    section_prep_mappart
from challenges.sectional.PySpark_Strategy.SectionRddReduceAsymPart import \
    section_asymreduce_partials
from challenges.sectional.SectionTypeDefs import (DaskTestMethod,
                                                  PysparkTestMethod)

dask_implementation_list: List[DaskTestMethod] = []
pyspark_implementation_list: List[PysparkTestMethod] = [
    PysparkTestMethod(
        strategy_name='section_nospark_single_threaded',
        language='python',
        interface='python',
        scale='singleline',
        delegate=section_nospark_single_threaded
    ),
    PysparkTestMethod(
        strategy_name='section_mappart_single_threaded',
        language='python',
        interface='rdd',
        scale='wholefile',
        delegate=section_mappart_single_threaded
    ),
    PysparkTestMethod(
        strategy_name='section_mappart_odd_even',
        language='python',
        interface='rdd',
        scale='wholesection',
        delegate=section_mappart_odd_even
    ),
    PysparkTestMethod(
        strategy_name='section_mappart_partials',
        language='python',
        interface='rdd',
        scale='threerows',
        delegate=section_mappart_partials
    ),
    PysparkTestMethod(
        strategy_name='section_asymreduce_partials',
        language='python',
        interface='rdd',
        scale='finalsummaries',
        delegate=section_asymreduce_partials
    ),
    PysparkTestMethod(
        strategy_name='section_prep_mappart',
        language='python',
        interface='rdd',
        scale='wholesection',
        delegate=section_prep_mappart
    ),
    PysparkTestMethod(
        strategy_name='section_prep_groupby',
        language='python',
        interface='df',
        scale='wholesection',
        delegate=section_prep_groupby
    ),
    PysparkTestMethod(
        strategy_name='section_prepcsv_groupby',
        language='python',
        interface='df',
        scale='wholesection',
        delegate=section_prepcsv_groupby
    ),
    PysparkTestMethod(
        strategy_name='section_join_groupby',
        language='python',
        interface='df',
        scale='wholesection',
        delegate=section_join_groupby
    ),
    PysparkTestMethod(
        strategy_name='section_join_mappart',
        language='python',
        interface='rdd',
        scale='wholesection',
        delegate=section_join_mappart
    ),
]


strategy_name_list = [x.strategy_name for x in pyspark_implementation_list]
