"""Run autopep8, flake8, ty, pytest, pyright and/or the quick benchmark runs across every Python sub-project.

Select tools with --autopep8, --flake8, --ty, --pytest, --pyright, --quick-run
(default: all but pyright and quick-run when none is given).
--quick-run executes each challenge-bearing sub-project's quick_test_run.ps1.
Skip sub-projects with --exclude <project_root> (repeatable).
"""
import argparse
import os
import shutil
import subprocess
import sys
import tomllib
from configparser import ConfigParser
from dataclasses import dataclass
from functools import cache
from pathlib import Path
from typing import Callable, NamedTuple

REPO_ROOT = Path(__file__).resolve().parent.parent

# pytest exit code 5 = no tests collected; several sub-projects have none yet.
PYTEST_NO_TESTS_COLLECTED = 5


class PythonProject(NamedTuple):
    project_root: str
    python_root: str
    has_challenges: bool


PYTHON_PROJECTS: tuple[PythonProject, ...] = (
    PythonProject(
        project_root="admin",
        python_root="src",
        has_challenges=False,
    ),
    PythonProject(
        project_root="common_python",
        python_root="spark_agg_methods_common_python",
        has_challenges=False,
    ),
    PythonProject(
        project_root="py_spark",
        python_root="src",
        has_challenges=True,
    ),
    PythonProject(
        project_root="python_dask",
        python_root="src",
        has_challenges=True,
    ),
    PythonProject(
        project_root="python_free_threaded",
        python_root="src",
        has_challenges=True,
    ),
    PythonProject(
        project_root="python_single_threaded",
        python_root="src",
        has_challenges=True,
    ),
    PythonProject(
        project_root="simple_queued_pipelines",
        python_root="simple_queued_pipelines",
        has_challenges=False,
    ),
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
    ok_return_codes: tuple[int, ...]
    challenges_only: bool = False


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


def _find_project_dir(start: Path) -> Path:
    """Nearest directory at or above `start` holding a pyproject.toml, the way `uv run` discovers its project."""
    for directory in (start, *start.parents):
        if (directory / "pyproject.toml").is_file():
            return directory
    raise FileNotFoundError(f"No pyproject.toml at or above {start}")


def _read_pyproject(project_dir: Path) -> dict:
    with open(project_dir / "pyproject.toml", "rb") as f:
        return tomllib.load(f)


def _packaged_source_roots(project_dir: Path) -> list[Path]:
    """Directories that `[tool.setuptools.packages.find] where` exposes as import roots; empty when unset."""
    find = _read_pyproject(project_dir).get("tool", {}).get("setuptools", {}).get("packages", {}).get("find", {})
    return [(project_dir / where).resolve() for where in find.get("where", [])]


def _extra_search_paths(project: PythonProject) -> list[Path]:
    """Import roots that ty cannot see through an editable install, derived from `[tool.uv.sources]`.

    Setuptools" editable finder is invisible to static analysis, so each path source contributes the import roots
    it packages (its `[tool.setuptools.packages.find] where`), or its own directory when it declares none. The
    project"s own packaging roots count too, so `common_python` resolves its own package under its own venv.
    """
    project_dir = _find_project_dir(REPO_ROOT / project.project_root)
    roots = _packaged_source_roots(project_dir)
    sources = _read_pyproject(project_dir).get("tool", {}).get("uv", {}).get("sources", {})
    for source in sources.values():
        if "path" in source:
            source_dir = (project_dir / source["path"]).resolve()
            roots.extend(_packaged_source_roots(source_dir) or [source_dir])
    return list(dict.fromkeys(roots))


def _ty_command(project: PythonProject) -> list[str]:
    extra_search_paths = [
        argument
        for path in _extra_search_paths(project)
        for argument in ("--extra-search-path", str(path))
    ]
    return [
        "uv", "run", "python", "-m", "ty", "check",
        *extra_search_paths,
        project.python_root,
    ]


@cache
def _pyright_executable() -> str:
    """Locate the globally installed (npm) pyright, even when npm"s global bin folder is not on PATH."""
    found = shutil.which("pyright")
    if found is None:
        npm = shutil.which("npm")
        if npm is not None:
            prefix = subprocess.run([npm, "prefix", "-g"], capture_output=True, text=True, check=True).stdout.strip()
            found = shutil.which("pyright", path=prefix if sys.platform == "win32" else str(Path(prefix) / "bin"))
    if found is None:
        raise FileNotFoundError("pyright not found; install it with `npm install -g pyright`")
    return found


def _venv_python(project: PythonProject) -> Path:
    """Interpreter of the project"s own uv venv, which pyright needs to resolve third-party imports."""
    venv = _find_project_dir(REPO_ROOT / project.project_root) / ".venv"
    return venv / "Scripts" / "python.exe" if sys.platform == "win32" else venv / "bin" / "python"


def _pyright_command(project: PythonProject) -> list[str]:
    return [
        _pyright_executable(),
        "--pythonpath", str(_venv_python(project)),
        project.python_root,
    ]


def _quick_run_command(project: PythonProject) -> list[str]:
    shell = "powershell" if sys.platform == "win32" else "pwsh"
    return [shell, "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", "quick_test_run.ps1"]


def _pytest_command(project: PythonProject) -> list[str]:
    return [
        "uv", "run", "python", "-m", "pytest", "-m", "not slow",
        project.python_root,
    ]


AUTOPEP8 = Tool(
    name="autopep8",
    build_command=_autopep8_command,
    ok_return_codes=(0,),
)
FLAKE8 = Tool(
    name="flake8",
    build_command=_flake8_command,
    ok_return_codes=(0,),
)
TY = Tool(
    name="ty",
    build_command=_ty_command,
    ok_return_codes=(0,),
)
PYTEST = Tool(
    name="pytest",
    build_command=_pytest_command,
    ok_return_codes=(0, PYTEST_NO_TESTS_COLLECTED),
)
PYRIGHT = Tool(
    name="pyright",
    build_command=_pyright_command,
    ok_return_codes=(0,),
)
QUICK_RUN = Tool(
    name="quick-run",
    build_command=_quick_run_command,
    ok_return_codes=(0,),
    challenges_only=True,
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--autopep8", action="store_true", help="Run autopep8 --in-place")
    parser.add_argument("--flake8", action="store_true", help="Run flake8")
    parser.add_argument("--ty", action="store_true", help="Run ty check")
    parser.add_argument("--pytest", action="store_true", help="Run pytest")
    parser.add_argument("--pyright", action="store_true", help="Run global npm pyright (opt-in, not a default)")
    parser.add_argument(
        "--quick-run",
        action="store_true",
        default=False,
        help="Run quick_test_run.ps1 in every sub-project that has challenges (opt-in, not a default)",
    )
    parser.add_argument(
        "--exclude",
        action="append",
        default=[],
        choices=[project.project_root for project in PYTHON_PROJECTS],
        metavar="PROJECT",
        help="Sub-project folder to skip for every selected tool (repeatable)",
    )
    args = parser.parse_args(argv)
    # pyright is opt-in: it depends on a global npm install, unlike the uv-managed tools.
    if not (args.autopep8 or args.flake8 or args.ty or args.pytest or args.pyright or args.quick_run):
        args.autopep8 = args.flake8 = args.ty = args.pytest = True
    return args


def run_tool(tool: Tool, exclude: list[str], env: dict[str, str]) -> list[str]:
    """Run one tool over every non-excluded project; return the folders where it failed."""
    failed_folders: list[str] = []
    for project in PYTHON_PROJECTS:
        if tool.challenges_only and not project.has_challenges:
            continue
        folder = project.project_root
        if folder in exclude:
            print(f'\n{"="*50}\nSkipping {tool.name} in {folder}\n{"="*50}', flush=True)
            continue

        print(f'\n{"="*50}\nRunning {tool.name} in {folder}\n{"="*50}', flush=True)
        result = subprocess.run(tool.build_command(project), cwd=REPO_ROOT / folder, env=env)
        if result.returncode not in tool.ok_return_codes:
            failed_folders.append(folder)
            print(f"{tool.name} failed in {folder}!", file=sys.stderr)
    return failed_folders


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    tools = [
        tool for tool, selected in (
            (AUTOPEP8, args.autopep8),
            (FLAKE8, args.flake8),
            (TY, args.ty),
            (PYTEST, args.pytest),
            (PYRIGHT, args.pyright),
            (QUICK_RUN, args.quick_run),
        )
        if selected]

    env = child_env()
    failures = {tool.name: run_tool(tool, args.exclude, env) for tool in tools}

    failed = {name: folders for name, folders in failures.items() if folders}
    if failed:
        for name, folders in failed.items():
            print(f'\n{name} failed in: {", ".join(folders)}', file=sys.stderr)
        return 1
    print(f'\n{", ".join(failures)} passed successfully in all projects!')
    return 0


if __name__ == "__main__":
    sys.exit(main())
