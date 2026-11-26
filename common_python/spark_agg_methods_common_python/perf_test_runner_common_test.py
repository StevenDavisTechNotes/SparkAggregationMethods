import sys
import types

from spark_agg_methods_common_python.challenges.vanilla.vanilla_test_data_types import (
    DATA_SIZE_LIST_VANILLA_MAXIMAL, DATA_SIZE_LIST_VANILLA_MINIMAL,
)
from spark_agg_methods_common_python.perf_test_common import Challenge
from spark_agg_methods_common_python.perf_test_runner_common import (
    QUICK_NUM_SIZES, ChallengeRunner, discover_challenge_runners, parse_request,
)


class FakeWriter:
    RUN_LOG_FILE_NAME = "fake_runs.csv"


def make_runner_module(name: str = "fake_vanilla_runner_x") -> types.ModuleType:
    module = types.ModuleType(name)
    module.CHALLENGE = Challenge.VANILLA  # ty: ignore[unresolved-attribute]
    module.DATA_SIZE_LIST_VANILLA_MAXIMAL = DATA_SIZE_LIST_VANILLA_MAXIMAL  # ty: ignore[unresolved-attribute]
    module.DATA_SIZE_LIST_VANILLA_MINIMAL = DATA_SIZE_LIST_VANILLA_MINIMAL  # ty: ignore[unresolved-attribute]
    FakeWriter.__module__ = name
    module.FakeWriter = FakeWriter  # ty: ignore[unresolved-attribute]
    return module


def test_conventions_locate_size_list_writer_and_quick_sizes():
    runner = ChallengeRunner(module=make_runner_module())
    assert runner.size_list_name == "DATA_SIZE_LIST_VANILLA_MAXIMAL"
    assert runner.writer_class is FakeWriter
    assert runner.quick_size_codes == [x.size_code for x in DATA_SIZE_LIST_VANILLA_MINIMAL][:QUICK_NUM_SIZES]


def test_quick_request_restricts_sizes_and_disables_shuffle():
    runner = ChallengeRunner(module=make_runner_module())
    request = parse_request(["--quick"])
    argv = request.argv_for(runner)
    assert request.quick
    assert "--no-shuffle" in argv
    assert argv[argv.index("--size") + 1:] == runner.quick_size_codes
    assert parse_request(["--runs", "0"]).runner_argv == ["--runs", "0"]


def test_discovery_finds_runner_files_of_the_engine_suffix_only(tmp_path, monkeypatch):
    package_root = tmp_path / "fake_challenges_pkg"
    for challenge, suffix in (("alpha", "x"), ("beta", "x"), ("beta", "y"), ("gamma", "y")):
        folder = package_root / challenge
        folder.mkdir(parents=True, exist_ok=True)
        (folder / f"{challenge}_runner_{suffix}.py").write_text("")
    (package_root / "alpha" / "six_runner_base_x.py").write_text("")
    monkeypatch.syspath_prepend(str(tmp_path))
    import fake_challenges_pkg  # ty: ignore[unresolved-import]
    runners = discover_challenge_runners(fake_challenges_pkg, "x")
    assert [x.name for x in runners] == ["alpha_runner_x", "beta_runner_x"]
    sys.modules.pop("fake_challenges_pkg", None)
