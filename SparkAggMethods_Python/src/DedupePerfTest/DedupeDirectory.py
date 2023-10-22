from typing import List

from DedupePerfTest.DedupeDataTypes import PysparkTestMethod
from DedupePerfTest.Strategy.DedupeFluentNestedPandas import dedupe_pandas
from DedupePerfTest.Strategy.DedupeFluentNestedPython import \
    dedupe_fluent_nested_python
from DedupePerfTest.Strategy.DedupeFluentNestedWCol import \
    dedupe_fluent_nested_withCol
from DedupePerfTest.Strategy.DedupeFluentWindow import dedupe_fluent_windows
from DedupePerfTest.Strategy.DedupeRddGroupBy import dedupe_rdd_groupby
from DedupePerfTest.Strategy.DedupeRddMapPart import dedupe_rdd_mappart
from DedupePerfTest.Strategy.DedupeRddReduce import dedupe_rdd_reduce

pyspark_implementation_list: List[PysparkTestMethod] = [
    PysparkTestMethod(
        strategy_name='dedupe_pandas',
        language='python',
        interface='pandas',
        delegate=dedupe_pandas,
    ),
    PysparkTestMethod(
        strategy_name='dedupe_fluent_nested_python',
        language='python',
        interface='fluent',
        delegate=dedupe_fluent_nested_python,
    ),
    PysparkTestMethod(
        strategy_name='dedupe_fluent_nested_withCol',
        language='python',
        interface='fluent',
        delegate=dedupe_fluent_nested_withCol,
    ),
    PysparkTestMethod(
        strategy_name='dedupe_fluent_windows',
        language='python',
        interface='fluent',
        delegate=dedupe_fluent_windows,
    ),
    PysparkTestMethod(
        strategy_name='dedupe_rdd_groupby',
        language='python',
        interface='rdd',
        delegate=dedupe_rdd_groupby,
    ),
    PysparkTestMethod(
        strategy_name='dedupe_rdd_mappart',
        language='python',
        interface='rdd',
        delegate=dedupe_rdd_mappart,
    ),
    PysparkTestMethod(
        strategy_name='dedupe_rdd_reduce',
        language='python',
        interface='rdd',
        delegate=dedupe_rdd_reduce,
    ),
]

strategy_name_list = [x.strategy_name for x in pyspark_implementation_list]
