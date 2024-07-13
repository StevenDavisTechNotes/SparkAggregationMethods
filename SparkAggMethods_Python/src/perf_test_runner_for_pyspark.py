#!python
# usage: .\venv\Scripts\activate.ps1; python -m src.perf_test_runner_for_pyspark

from src.challenges.bi_level import bi_level_pyspark_runner
from src.challenges.conditional import conditional_pyspark_runner
from src.challenges.deduplication import dedupe_pyspark_runner
from src.challenges.sectional import section_pyspark_runner
from src.challenges.vanilla import vanilla_pyspark_runner

vanilla_pyspark_runner.main()
bi_level_pyspark_runner.main()
conditional_pyspark_runner.main()
section_pyspark_runner.main()
dedupe_pyspark_runner.main()
