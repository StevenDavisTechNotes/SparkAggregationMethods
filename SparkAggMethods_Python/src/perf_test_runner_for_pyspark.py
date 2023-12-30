#!python3
# usage: cd src; python -m perf_test_runner_for_pyspark

from challenges.bi_level import bi_level_pyspark_runner  # noqa: F401
from challenges.conditional import conditional_pyspark_runner  # noqa: F401
from challenges.deduplication import dedupe_pyspark_runner  # noqa: F401
from challenges.sectional import section_pyspark_runner  # noqa: F401
from challenges.vanilla import vanilla_pyspark_runner  # noqa: F401

vanilla_pyspark_runner.main()
bi_level_pyspark_runner.main()
conditional_pyspark_runner.main()
section_pyspark_runner.main()
dedupe_pyspark_runner.main()
