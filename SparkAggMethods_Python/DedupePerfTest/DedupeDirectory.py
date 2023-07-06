from typing import List

from .DedupeDataTypes import PythonTestMethod
from .Strategy.DedupeFluentNestedPandas import method_pandas
from .Strategy.DedupeFluentNestedPython import method_fluent_nested_python
from .Strategy.DedupeFluentNestedWCol import method_fluent_nested_withCol
from .Strategy.DedupeFluentWindow import method_fluent_windows
from .Strategy.DedupeRddGroupBy import method_rdd_groupby
from .Strategy.DedupeRddMapPart import method_rdd_mappart
from .Strategy.DedupeRddReduce import method_rdd_reduce


implementation_list: List[PythonTestMethod] = [
    PythonTestMethod(
        strategy_name='method_pandas',
        language='python',
        interface='pandas',
        delegate=method_pandas,
    ),
    PythonTestMethod(
        strategy_name='method_fluent_nested_python',
        language='python',
        interface='fluent',
        delegate=method_fluent_nested_python,
    ),
    PythonTestMethod(
        strategy_name='method_fluent_nested_withCol',
        language='python',
        interface='fluent',
        delegate=method_fluent_nested_withCol,
    ),
    PythonTestMethod(
        strategy_name='method_fluent_windows',
        language='python',
        interface='fluent',
        delegate=method_fluent_windows,
    ),
    PythonTestMethod(
        strategy_name='method_rdd_groupby',
        language='python',
        interface='rdd',
        delegate=method_rdd_groupby,
    ),
    PythonTestMethod(
        strategy_name='method_rdd_mappart',
        language='python',
        interface='rdd',
        delegate=method_rdd_mappart,
    ),
    PythonTestMethod(
        strategy_name='method_rdd_reduce',
        language='python',
        interface='rdd',
        delegate=method_rdd_reduce,
    ),
]

strategy_name_list = [x.strategy_name for x in implementation_list]
