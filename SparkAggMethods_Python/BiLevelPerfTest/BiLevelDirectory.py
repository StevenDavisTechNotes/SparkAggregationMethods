from typing import List

from SixFieldTestData import PythonTestMethod

from .Strategy.BiLevelFluentJoin import bi_fluent_join
from .Strategy.BiLevelFluentNested import bi_fluent_nested
from .Strategy.BiLevelFluentWindow import bi_fluent_window
from .Strategy.BiLevelPandas import bi_pandas
from .Strategy.BiLevelPandasNumba import bi_pandas_numba
from .Strategy.BiLevelRddGrpMap import bi_rdd_grpmap
from .Strategy.BiLevelRddMapPart import bi_rdd_mappart
from .Strategy.BiLevelRddReduce1 import bi_rdd_reduce1
from .Strategy.BiLevelRddReduce2 import bi_rdd_reduce2
from .Strategy.BiLevelSqlJoin import bi_sql_join
from .Strategy.BiLevelSqlNested import bi_sql_nested

implementation_list: List[PythonTestMethod] = [
    PythonTestMethod(
        strategy_name='bi_sql_join',
        language='python',
        interface='sql',
        delegate=bi_sql_join
    ),
    PythonTestMethod(
        strategy_name='bi_fluent_join',
        language='python',
        interface='fluent',
        delegate=bi_fluent_join
    ),
    PythonTestMethod(
        strategy_name='bi_pandas',
        language='python',
        interface='pandas',
        delegate=bi_pandas
    ),
    PythonTestMethod(
        strategy_name='bi_pandas_numba',
        language='python',
        interface='pandas',
        delegate=bi_pandas_numba
    ),
    PythonTestMethod(
        strategy_name='bi_sql_nested',
        language='python',
        interface='sql',
        delegate=bi_sql_nested
    ),
    PythonTestMethod(
        strategy_name='bi_fluent_nested',
        language='python',
        interface='fluent',
        delegate=bi_fluent_nested
    ),
    PythonTestMethod(
        strategy_name='bi_fluent_window',
        language='python',
        interface='fluent',
        delegate=bi_fluent_window
    ),
    PythonTestMethod(
        strategy_name='bi_rdd_grpmap',
        language='python',
        interface='rdd',
        delegate=bi_rdd_grpmap
    ),
    PythonTestMethod(
        strategy_name='bi_rdd_reduce1',
        language='python',
        interface='rdd',
        delegate=bi_rdd_reduce1
    ),
    PythonTestMethod(
        strategy_name='bi_rdd_reduce2',
        language='python',
        interface='rdd',
        delegate=bi_rdd_reduce2
    ),
    PythonTestMethod(
        strategy_name='bi_rdd_mappart',
        language='python',
        interface='rdd',
        delegate=bi_rdd_mappart
    ),
]


strategy_name_list = [x.strategy_name for x in implementation_list]
