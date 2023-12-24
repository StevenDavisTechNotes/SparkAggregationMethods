import multiprocessing
import os
import random
from typing import Any

import numpy


def always_true(
        _: Any
) -> bool:
    return True


def detectCPUs():
    return multiprocessing.cpu_count()


def int_divide_round_up(
        x: int,
        y: int,
) -> int:
    return (x + y - 1) // y


def set_random_seed(
        seed: int
) -> None:
    random.seed(seed)
    numpy.random.seed(seed + 1)


def root_folder_abs_path() -> str:
    abs_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    return abs_path
