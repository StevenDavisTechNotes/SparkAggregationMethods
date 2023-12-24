#!python3
# usage: cd src; python PerfTestRunner.py

from BiLevelPerfTest import BiLevelPySparkRunner  # noqa: F401
from ConditionalPerfTest import CondPySparkRunner  # noqa: F401
from DedupePerfTest import DedupePySparkRunner  # noqa: F401
from SectionPerfTest import SectionPySparkRunner  # noqa: F401
from VanillaPerfTest import VanillaDaskRunner  # noqa: F401
from VanillaPerfTest import VanillaPySparkRunner  # noqa: F401

VanillaDaskRunner.main()
VanillaPySparkRunner.main()
BiLevelPySparkRunner.main()
CondPySparkRunner.main()
SectionPySparkRunner.main()
DedupePySparkRunner.main()
