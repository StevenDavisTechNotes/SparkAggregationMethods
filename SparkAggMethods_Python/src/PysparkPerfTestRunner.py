#!python3
# usage: cd src; python -m PerfTestRunner

from BiLevelPerfTest import BiLevelPySparkRunner  # noqa: F401
from ConditionalPerfTest import CondPySparkRunner  # noqa: F401
from DedupePerfTest import DedupePySparkRunner  # noqa: F401
from SectionPerfTest import SectionPySparkRunner  # noqa: F401
from VanillaPerfTest import VanillaPySparkRunner  # noqa: F401

VanillaPySparkRunner.main()
BiLevelPySparkRunner.main()
CondPySparkRunner.main()
SectionPySparkRunner.main()
DedupePySparkRunner.main()
