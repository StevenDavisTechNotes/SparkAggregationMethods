from src.perf_test_runner_for_py_all import find_errors

FLAKY_STEP_FAILURE = [
    "26/11/25 22:36:43 ERROR tidy_session_pyspark: Error in s for 30k: An error occurred in collectAndServe.",
    ": org.apache.spark.SparkException: Python worker exited unexpectedly (crashed).",
    "\tat org.apache.spark.api.python.BasePythonRunner.read(PythonRunner.scala:704)",
    "INFO:__main__:next",
]


def test_spark_task_retry_records_are_not_errors():
    lines = ["26/11/25 22:35:19 ERROR Executor: Exception in task 0.0", "org.apache.spark.SparkException: x"]
    assert find_errors(lines, is_pyspark=True) == []


def test_pyspark_worker_crash_is_flaky_only_for_pyspark():
    assert [x.flaky for x in find_errors(FLAKY_STEP_FAILURE, is_pyspark=True)] == [True]
    assert [x.flaky for x in find_errors(FLAKY_STEP_FAILURE, is_pyspark=False)] == [False]


def test_other_pyspark_error_is_not_flaky():
    lines = [
        "26/11/25 22:36:43 ERROR tidy_session_pyspark: Error in s for 30k: abs_diff=2e-12 >= numerical_tolerance=1e-12",
        "INFO:__main__:next",
    ]
    assert [x.flaky for x in find_errors(lines, is_pyspark=True)] == [False]


def test_child_failed_after_flaky_error_is_flaky_but_unexplained_child_failure_is_not():
    explained = [
        "INFO:__main__:Child 1/2 of c: --runs 1",
        *FLAKY_STEP_FAILURE,
        "ERROR:__main__:Child failed: c --runs 1",
    ]
    assert [x.flaky for x in find_errors(explained, is_pyspark=True)] == [True]
    unexplained = ["INFO:__main__:Child 2/2 of c: --runs 1", "ERROR:__main__:Child failed: c --runs 1"]
    assert [x.flaky for x in find_errors(unexplained, is_pyspark=True)] == [False]


def test_traceback_block_is_captured_with_its_exception_line():
    lines = [
        "Traceback (most recent call last):",
        '  File "x.py", line 1, in main',
        "ValueError: abs_diff=1 >= numerical_tolerance=0.1",
    ]
    errors = find_errors(lines, is_pyspark=False)
    assert len(errors) == 1
    assert errors[0].text.endswith("numerical_tolerance=0.1")
