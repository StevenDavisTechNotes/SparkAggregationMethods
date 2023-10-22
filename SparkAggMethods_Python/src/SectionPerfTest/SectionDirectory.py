from typing import List

from SectionPerfTest.PySparkStrategy.SectionFluentPrepGroupBy import \
    section_prep_groupby
from SectionPerfTest.PySparkStrategy.SectionFluentPrepGroupByCSV import \
    section_prepcsv_groupby
from SectionPerfTest.PySparkStrategy.SectionJoinGroupBy import \
    section_join_groupby
from SectionPerfTest.PySparkStrategy.SectionJoinMapPart import \
    section_join_mappart
from SectionPerfTest.PySparkStrategy.SectionNoSparkST import \
    section_nospark_single_threaded
from SectionPerfTest.PySparkStrategy.SectionRddMapPartOddEven import \
    section_mappart_odd_even
from SectionPerfTest.PySparkStrategy.SectionRddMapPartPartials import \
    section_mappart_partials
from SectionPerfTest.PySparkStrategy.SectionRddMapPartST import \
    section_mappart_single_threaded
from SectionPerfTest.PySparkStrategy.SectionRddPrepMapPart import \
    section_prep_mappart
from SectionPerfTest.PySparkStrategy.SectionRddReduceAsymPart import \
    section_asymreduce_partials
from SectionPerfTest.SectionTypeDefs import DaskTestMethod, PysparkTestMethod

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
