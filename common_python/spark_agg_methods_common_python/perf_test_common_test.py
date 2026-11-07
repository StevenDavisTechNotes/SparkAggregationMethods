from spark_agg_methods_common_python.perf_test_common import run_log_has_current_header

HEADER = " elapsed_time,engine,strategy_name,"
OLD_HEADER = " elapsed_time,strategy_name,"
RESULT = "1.5,pyspark,some_strategy,"


def test_empty_log_needs_a_header():
    assert not run_log_has_current_header([], HEADER)


def test_log_ending_in_the_same_header_does_not_need_another():
    assert run_log_has_current_header([HEADER + "\n", RESULT + "\n", RESULT + "\n"], HEADER)


def test_changed_columns_need_a_new_header_even_if_the_old_one_appears_earlier():
    assert not run_log_has_current_header([HEADER + "\n", RESULT + "\n", OLD_HEADER + "\n", RESULT + "\n"], HEADER)


def test_result_lines_are_not_mistaken_for_headers():
    assert not run_log_has_current_header([RESULT + "\n"], HEADER)
