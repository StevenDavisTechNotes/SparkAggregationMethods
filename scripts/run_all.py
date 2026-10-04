"""Run autopep8, flake8 and/or pytest across every Python sub-project.

Select tools with --autopep8, --flake8, --pytest (default: all three when none is given).
Skip sub-projects with --exclude <project_root> (repeatable).
"""
import argparse
import os
import subprocess
import sys
from configparser import ConfigParser
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, NamedTuple

REPO_ROOT = Path(__file__).resolve().parent.parent

# pytest exit code 5 = no tests collected; several sub-projects have none yet.
PYTEST_NO_TESTS_COLLECTED = 5


class PythonProject(NamedTuple):
    project_root: str
    python_root: str


PYTHON_PROJECTS: tuple[PythonProject, ...] = (
    PythonProject("admin", "src"),
    PythonProject("common_python", "spark_agg_methods_common_python"),
    PythonProject("py_spark", "src"),
    PythonProject("python_dask", "src"),
    PythonProject("python_free_threaded", "src"),
    PythonProject("python_single_threaded", "src"),
    PythonProject("simple_queued_pipelines", "simple_queued_pipelines"),
)


def child_env() -> dict[str, str]:
    """Environment for per-project `uv run` children.

    The parent `uv run` exports the root VIRTUAL_ENV; children must resolve their own project venv.
    """
    return {key: value for key, value in os.environ.items() if key != "VIRTUAL_ENV"}


def read_setup_cfg_int(section: str, key: str) -> int:
    """Read an integer setting from the repo-root setup.cfg, the single source for flake8/pycodestyle limits."""
    parser = ConfigParser()
    parser.read_string((REPO_ROOT / "setup.cfg").read_text(encoding="utf-8"))
    return parser.getint(section, key)


@dataclass(frozen=True)
class Tool:
    name: str
    build_command: Callable[[PythonProject], list[str]]
    ok_return_codes: tuple[int, ...] = (0,)


def _autopep8_command(project: PythonProject) -> list[str]:
    max_line_length = read_setup_cfg_int("pycodestyle", "max_line_length")
    return [
        "uv", "run", "python", "-m", "autopep8",
        "--in-place",
        "--recursive",
        f"--max-line-length={max_line_length}",
        "--exclude=.venv",
        project.python_root,
    ]


def _flake8_command(project: PythonProject) -> list[str]:
    max_line_length = read_setup_cfg_int("flake8", "max-line-length")
    return [
        "uv", "run", "python", "-m", "flake8",
        f"--max-line-length={max_line_length}",
        "--exclude=.venv",
        project.python_root,
    ]


def _pytest_command(project: PythonProject) -> list[str]:
    return ["uv", "run", "python", "-m", "pytest", project.python_root]


AUTOPEP8 = Tool("autopep8", _autopep8_command)
FLAKE8 = Tool("flake8", _flake8_command)
PYTEST = Tool("pytest", _pytest_command, ok_return_codes=(0, PYTEST_NO_TESTS_COLLECTED))


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--autopep8", action="store_true", help="Run autopep8 --in-place")
    parser.add_argument("--flake8", action="store_true", help="Run flake8")
    parser.add_argument("--pytest", action="store_true", help="Run pytest")
    parser.add_argument(
        "--exclude",
        action="append",
        default=[],
        choices=[project.project_root for project in PYTHON_PROJECTS],
        metavar="PROJECT",
        help="Sub-project folder to skip for every selected tool (repeatable)",
    )
    args = parser.parse_args(argv)
    if not (args.autopep8 or args.flake8 or args.pytest):
        args.autopep8 = args.flake8 = args.pytest = True
    return args


def run_tool(tool: Tool, exclude: list[str], env: dict[str, str]) -> list[str]:
    """Run one tool over every non-excluded project; return the folders where it failed."""
    failed_folders: list[str] = []
    for project in PYTHON_PROJECTS:
        folder = project.project_root
        if folder in exclude:
            print(f"\n{'='*50}\nSkipping {tool.name} in {folder}\n{'='*50}", flush=True)
            continue

        print(f"\n{'='*50}\nRunning {tool.name} in {folder}\n{'='*50}", flush=True)
        result = subprocess.run(tool.build_command(project), cwd=REPO_ROOT / folder, env=env)
        if result.returncode not in tool.ok_return_codes:
            failed_folders.append(folder)
            print(f"{tool.name} failed in {folder}!", file=sys.stderr)
    return failed_folders


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    tools = [tool for tool, selected in ((AUTOPEP8, args.autopep8), (FLAKE8, args.flake8), (PYTEST, args.pytest))
             if selected]

    env = child_env()
    failures = {tool.name: run_tool(tool, args.exclude, env) for tool in tools}

    failed = {name: folders for name, folders in failures.items() if folders}
    if failed:
        for name, folders in failed.items():
            print(f"\n{name} failed in: {', '.join(folders)}", file=sys.stderr)
        return 1
    print(f"\n{', '.join(failures)} passed successfully in all projects!")
    return 0


if __name__ == "__main__":
    sys.exit(main())
