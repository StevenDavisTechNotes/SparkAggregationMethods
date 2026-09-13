import subprocess
import sys


def run_flake8():
    targets = [
        "admin",
        "common_python",
        "py_spark",
        "python_dask",
        "python_free_threaded",
        "python_single_threaded",
        "simple_queued_pipelines",
    ]
    failed = False
    for folder in targets:
        print(f"\n{'='*50}\nRunning flake8 in {folder}\n{'='*50}", flush=True)
        result = subprocess.run(["uv", "run", "python", "-m", "flake8", "--max-line-length=120", "--exclude=.venv", "."], cwd=folder)
        if result.returncode != 0:
            failed = True
            print(f"Flake8 failed in {folder}!", file=sys.stderr)

    if failed:
        sys.exit(1)
    else:
        print("\nFlake8 passed successfully in all projects!")
        sys.exit(0)

if __name__ == "__main__":
    run_flake8()

