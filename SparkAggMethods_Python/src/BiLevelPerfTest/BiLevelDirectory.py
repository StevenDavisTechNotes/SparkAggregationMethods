from typing import List

from BiLevelPerfTest.PySparkStrategy.BiLevelFluentJoin import bi_fluent_join
from BiLevelPerfTest.PySparkStrategy.BiLevelFluentNested import \
    bi_fluent_nested
from BiLevelPerfTest.PySparkStrategy.BiLevelFluentWindow import \
    bi_fluent_window
from BiLevelPerfTest.PySparkStrategy.BiLevelPandas import bi_pandas
from BiLevelPerfTest.PySparkStrategy.BiLevelPandasNumba import bi_pandas_numba
from BiLevelPerfTest.PySparkStrategy.BiLevelRddGrpMap import bi_rdd_grpmap
from BiLevelPerfTest.PySparkStrategy.BiLevelRddMapPart import bi_rdd_mappart
from BiLevelPerfTest.PySparkStrategy.BiLevelRddReduce1 import bi_rdd_reduce1
from BiLevelPerfTest.PySparkStrategy.BiLevelRddReduce2 import bi_rdd_reduce2
from BiLevelPerfTest.PySparkStrategy.BiLevelSqlJoin import bi_sql_join
from BiLevelPerfTest.PySparkStrategy.BiLevelSqlNested import bi_sql_nested
from SixFieldCommon.PySpark_SixFieldTestData import PysparkPythonTestMethod

pyspark_implementation_list: List[PysparkPythonTestMethod] = [
    PysparkPythonTestMethod(
        strategy_name='bi_sql_join',
        language='python',
        interface='sql',
        delegate=bi_sql_join
    ),
    PysparkPythonTestMethod(
        strategy_name='bi_fluent_join',
        language='python',
        interface='fluent',
        delegate=bi_fluent_join
    ),
    PysparkPythonTestMethod(
        strategy_name='bi_pandas',
        language='python',
        interface='pandas',
        delegate=bi_pandas
    ),
    PysparkPythonTestMethod(
        strategy_name='bi_pandas_numba',
        language='python',
        interface='pandas',
        delegate=bi_pandas_numba
    ),
    PysparkPythonTestMethod(
        strategy_name='bi_sql_nested',
        language='python',
        interface='sql',
        delegate=bi_sql_nested
    ),
    PysparkPythonTestMethod(
        strategy_name='bi_fluent_nested',
        language='python',
        interface='fluent',
        delegate=bi_fluent_nested
    ),
    PysparkPythonTestMethod(
        strategy_name='bi_fluent_window',
        language='python',
        interface='fluent',
        delegate=bi_fluent_window
    ),
    PysparkPythonTestMethod(
        strategy_name='bi_rdd_grpmap',
        language='python',
        interface='rdd',
        delegate=bi_rdd_grpmap
    ),
    PysparkPythonTestMethod(
        strategy_name='bi_rdd_reduce1',
        language='python',
        interface='rdd',
        delegate=bi_rdd_reduce1
    ),
    PysparkPythonTestMethod(
        strategy_name='bi_rdd_reduce2',
        language='python',
        interface='rdd',
        delegate=bi_rdd_reduce2
    ),
    PysparkPythonTestMethod(
        strategy_name='bi_rdd_mappart',
        language='python',
        interface='rdd',
        delegate=bi_rdd_mappart
    ),
]


strategy_name_list = [x.strategy_name for x in pyspark_implementation_list]
