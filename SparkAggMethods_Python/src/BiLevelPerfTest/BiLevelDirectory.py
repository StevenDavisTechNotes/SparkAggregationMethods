from typing import List

from SixFieldCommon.SixFieldTestData import PythonTestMethod

from BiLevelPerfTest.Strategy.BiLevelFluentJoin import bi_fluent_join
from BiLevelPerfTest.Strategy.BiLevelFluentNested import bi_fluent_nested
from BiLevelPerfTest.Strategy.BiLevelFluentWindow import bi_fluent_window
from BiLevelPerfTest.Strategy.BiLevelPandas import bi_pandas
from BiLevelPerfTest.Strategy.BiLevelPandasNumba import bi_pandas_numba
from BiLevelPerfTest.Strategy.BiLevelRddGrpMap import bi_rdd_grpmap
from BiLevelPerfTest.Strategy.BiLevelRddMapPart import bi_rdd_mappart
from BiLevelPerfTest.Strategy.BiLevelRddReduce1 import bi_rdd_reduce1
from BiLevelPerfTest.Strategy.BiLevelRddReduce2 import bi_rdd_reduce2
from BiLevelPerfTest.Strategy.BiLevelSqlJoin import bi_sql_join
from BiLevelPerfTest.Strategy.BiLevelSqlNested import bi_sql_nested

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
