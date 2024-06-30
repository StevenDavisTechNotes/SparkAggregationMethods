#!python
# usage: .\venv\Scripts\activate.ps1; cd src; python -m perf_test_runner_for_pyspark

from challenges.bi_level import bi_level_pyspark_runner
from challenges.conditional import conditional_pyspark_runner
from challenges.deduplication import dedupe_pyspark_runner
from challenges.sectional import section_pyspark_runner
from challenges.vanilla import vanilla_pyspark_runner

vanilla_pyspark_runner.main()
bi_level_pyspark_runner.main()
conditional_pyspark_runner.main()
section_pyspark_runner.main()
dedupe_pyspark_runner.main()
