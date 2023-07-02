from typing import List

from .SectionTypeDefs import PythonTestMethod
from .Strategy.SectionFluentPrepGroupBy import method_prep_groupby
from .Strategy.SectionFluentPrepGroupByCSV import method_prepcsv_groupby
from .Strategy.SectionNoSparkST import method_nospark_single_threaded
from .Strategy.SectionRddMapParkST import method_mappart_single_threaded
from .Strategy.SectionRddMapPartOddEven import method_mappart_odd_even
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
]


strategy_name_list = [x.strategy_name for x in implementation_list]
