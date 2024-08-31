# usage: .\run_all_runners.ps1
python -m src.challenges.bi_level.bi_level_dask_runner --runs 5
python -m src.challenges.bi_level.bi_level_pyspark_runner --runs 5
python -m src.challenges.bi_level.bi_level_python_only_runner --runs 5
python -m src.challenges.conditional.conditional_pyspark_runner --runs 5
python -m src.challenges.deduplication.dedupe_pyspark_runner --runs 5
python -m src.challenges.sectional.section_pyspark_runner --runs 5
python -m src.challenges.vanilla.vanilla_pyspark_runner --runs 5
