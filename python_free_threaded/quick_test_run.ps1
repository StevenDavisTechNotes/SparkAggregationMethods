# usage: . .\run_all_runners.ps1

.\.venv\Scripts\activate.ps1
python -O -m src.challenges.bi_level.bi_level_runner_py_ft --runs 0
python -O -m src.challenges.conditional.conditional_runner_py_ft --runs 0
python -O -m src.challenges.deduplication.dedupe_runner_py_ft --runs 0
python -O -m src.challenges.sectional.section_runner_py_ft --runs 0
python -O -m src.challenges.vanilla.vanilla_runner_py_ft --runs 0


