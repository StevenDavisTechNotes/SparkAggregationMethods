#!python3
# usage: python PerfTestRunner.py

from BiLevelPerfTest.BiLevelRunner import main as BiLevelRunner  # noqa: F401
from ConditionalPerfTest.CondRunner import main as CondRunner  # noqa: F401
from VanillaPerfTest.VanillaRunner import main as VanillaRunner  # noqa: F401
from DedupePerfTest.DedupeRunner import main as DedupeRunner  # noqa: F401
from SectionPerfTest.SectionRunner import main as SectionRunner  # noqa: F401

# VanillaRunner()
# BiLevelRunner()
# CondRunner()
SectionRunner()
# DedupeRunner()
