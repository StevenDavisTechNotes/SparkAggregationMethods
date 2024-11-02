#!python
# usage: python -m src.perf_test_runner_for_python_only

from src.challenges.bi_level import bi_level_runner_py_only
# from src.challenges.conditional import conditional_python_only_runner
# from src.challenges.deduplication import dedupe_python_only_runner
from src.challenges.sectional import section_runner_py_only
from src.challenges.vanilla import vanilla_runner_py_only

if __name__ == "__main__":
    print(f"Running {__file__}")
    bi_level_runner_py_only.main()
    # conditional_python_only_runner.main()
    # dedupe_python_only_runner.main()
    section_runner_py_only.main()
    vanilla_runner_py_only.main()
    print("Done!")
