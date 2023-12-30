#!python3
# usage: cd src; python -m perf_test_runner_for_pyspark

from challenges.bi_level import BiLevelPySparkRunner  # noqa: F401
from challenges.conditional import CondPySparkRunner  # noqa: F401
from challenges.deduplication import DedupePySparkRunner  # noqa: F401
from challenges.sectional import SectionPySparkRunner  # noqa: F401
from challenges.vanilla import VanillaPySparkRunner  # noqa: F401

VanillaPySparkRunner.main()
BiLevelPySparkRunner.main()
CondPySparkRunner.main()
SectionPySparkRunner.main()
DedupePySparkRunner.main()
