import pytest
from src.perf_test_runner_for_pyspark import parse_request


def test_runs_and_target_num_samples_are_mutually_exclusive():
    with pytest.raises(SystemExit):
        parse_request(["--runs", "3", "--target-num-samples", "3"])


def test_runs_means_exactly_that_many_and_ignores_the_logs():
    request = parse_request(["--runs", "2", "--no-shuffle"])
    assert (request.num_runs, request.top_up) == (2, False)
    assert request.runner_argv == ["--no-shuffle", "--runs", "2"]


def test_target_num_samples_tops_up_and_is_passed_to_the_challenge_runners_as_runs():
    request = parse_request(["--target-num-samples", "3"])
    assert (request.num_runs, request.top_up) == (3, True)
    assert request.runner_argv == ["--runs", "3"]


def test_one_run_is_the_default():
    request = parse_request([])
    assert (request.num_runs, request.top_up) == (1, False)


def test_negative_counts_are_rejected():
    with pytest.raises(SystemExit):
        parse_request(["--target-num-samples", "-1"])
