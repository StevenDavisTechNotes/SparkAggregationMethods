from typing import List

from .SectionTypeDefs import PythonTestMethod
from .Strategy.SectionFluentPrepGroupBy import method_prep_groupby
from .Strategy.SectionFluentPrepGroupByCSV import method_prepcsv_groupby
from .Strategy.SectionJoinGroupBy import method_join_groupby
from .Strategy.SectionJoinMapPart import method_join_mappart
from .Strategy.SectionNoSparkST import method_nospark_single_threaded
from .Strategy.SectionRddMapPartOddEven import method_mappart_odd_even
from .Strategy.SectionRddMapPartPartials import method_mappart_partials
from .Strategy.SectionRddMapPartST import method_mappart_single_threaded
from .Strategy.SectionRddPrepMapPart import method_prep_mappart
from .Strategy.SectionRddReduceAsymPart import method_asymreduce_partials

implementation_list: List[PythonTestMethod] = [
    PythonTestMethod(
        strategy_name='method_nospark_single_threaded',
        language='python',
        interface='python',
        scale='singleline',
        delegate=method_nospark_single_threaded
    ),
    PythonTestMethod(
        strategy_name='method_mappart_single_threaded',
        language='python',
        interface='rdd',
        scale='wholefile',
        delegate=method_mappart_single_threaded
    ),
    PythonTestMethod(
        strategy_name='method_mappart_odd_even',
        language='python',
        interface='rdd',
        scale='wholesection',
        delegate=method_mappart_odd_even
    ),
    PythonTestMethod(
        strategy_name='method_mappart_partials',
        language='python',
        interface='rdd',
        scale='threerows',
        delegate=method_mappart_partials
    ),
    PythonTestMethod(
        strategy_name='method_asymreduce_partials',
        language='python',
        interface='rdd',
        scale='finalsummaries',
        delegate=method_asymreduce_partials
    ),
    PythonTestMethod(
        strategy_name='method_prep_mappart',
        language='python',
        interface='rdd',
        scale='wholesection',
        delegate=method_prep_mappart
    ),
    PythonTestMethod(
        strategy_name='method_prep_groupby',
        language='python',
        interface='df',
        scale='wholesection',
        delegate=method_prep_groupby
    ),
    PythonTestMethod(
        strategy_name='method_prepcsv_groupby',
        language='python',
        interface='df',
        scale='wholesection',
        delegate=method_prepcsv_groupby
    ),
    PythonTestMethod(
        strategy_name='method_join_groupby',
        language='python',
        interface='df',
        scale='wholesection',
        delegate=method_join_groupby
    ),
    PythonTestMethod(
        strategy_name='method_join_mappart',
        language='python',
        interface='rdd',
        scale='wholesection',
        delegate=method_join_mappart
    ),
]


strategy_name_list = [x.strategy_name for x in implementation_list]
