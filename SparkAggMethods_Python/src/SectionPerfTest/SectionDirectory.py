from typing import List

from SectionPerfTest.SectionTypeDefs import PythonTestMethod
from SectionPerfTest.Strategy.SectionFluentPrepGroupBy import section_prep_groupby
from SectionPerfTest.Strategy.SectionFluentPrepGroupByCSV import section_prepcsv_groupby
from SectionPerfTest.Strategy.SectionJoinGroupBy import section_join_groupby
from SectionPerfTest.Strategy.SectionJoinMapPart import section_join_mappart
from SectionPerfTest.Strategy.SectionNoSparkST import section_nospark_single_threaded
from SectionPerfTest.Strategy.SectionRddMapPartOddEven import section_mappart_odd_even
from SectionPerfTest.Strategy.SectionRddMapPartPartials import section_mappart_partials
from SectionPerfTest.Strategy.SectionRddMapPartST import section_mappart_single_threaded
from SectionPerfTest.Strategy.SectionRddPrepMapPart import section_prep_mappart
from SectionPerfTest.Strategy.SectionRddReduceAsymPart import section_asymreduce_partials


implementation_list: List[PythonTestMethod] = [
    PythonTestMethod(
        strategy_name='section_nospark_single_threaded',
        language='python',
        interface='python',
        scale='singleline',
        delegate=section_nospark_single_threaded
    ),
    PythonTestMethod(
        strategy_name='section_mappart_single_threaded',
        language='python',
        interface='rdd',
        scale='wholefile',
        delegate=section_mappart_single_threaded
    ),
    PythonTestMethod(
        strategy_name='section_mappart_odd_even',
        language='python',
        interface='rdd',
        scale='wholesection',
        delegate=section_mappart_odd_even
    ),
    PythonTestMethod(
        strategy_name='section_mappart_partials',
        language='python',
        interface='rdd',
        scale='threerows',
        delegate=section_mappart_partials
    ),
    PythonTestMethod(
        strategy_name='section_asymreduce_partials',
        language='python',
        interface='rdd',
        scale='finalsummaries',
        delegate=section_asymreduce_partials
    ),
    PythonTestMethod(
        strategy_name='section_prep_mappart',
        language='python',
        interface='rdd',
        scale='wholesection',
        delegate=section_prep_mappart
    ),
    PythonTestMethod(
        strategy_name='section_prep_groupby',
        language='python',
        interface='df',
        scale='wholesection',
        delegate=section_prep_groupby
    ),
    PythonTestMethod(
        strategy_name='section_prepcsv_groupby',
        language='python',
        interface='df',
        scale='wholesection',
        delegate=section_prepcsv_groupby
    ),
    PythonTestMethod(
        strategy_name='section_join_groupby',
        language='python',
        interface='df',
        scale='wholesection',
        delegate=section_join_groupby
    ),
    PythonTestMethod(
        strategy_name='section_join_mappart',
        language='python',
        interface='rdd',
        scale='wholesection',
        delegate=section_join_mappart
    ),
]


strategy_name_list = [x.strategy_name for x in implementation_list]
