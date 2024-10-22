#!python
# usage: .\venv\Scripts\activate.ps1; python -m src.perf_test_runner_for_pyspark

from src.challenges.bi_level import bi_level_runner_pyspark
from src.challenges.conditional import conditional_runner_pyspark
from src.challenges.deduplication import dedupe_runner_pyspark
from src.challenges.sectional import section_runner_pyspark
from src.challenges.vanilla import vanilla_runner_pyspark

vanilla_runner_pyspark.main()
bi_level_runner_pyspark.main()
conditional_runner_pyspark.main()
section_runner_pyspark.main()
dedupe_runner_pyspark.main()
