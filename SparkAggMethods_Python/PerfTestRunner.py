#!python3
# usage: python PerfTestRunner.py

from BiLevelPerfTest.BiLevelRunner import main as BiLevelRunner
from ConditionalPerfTest.CondRunner import main as CondRunner
from VanillaPerfTest.VanillaRunner import main as VanillaRunner
from DedupePerfTest.DedupeRunner import main as DedupeRunner
from SectionPerfTest.SectionRunner import main as SectionRunner

# VanillaRunner()
# BiLevelRunner()
# CondRunner()
# SectionRunner()
DedupeRunner()
