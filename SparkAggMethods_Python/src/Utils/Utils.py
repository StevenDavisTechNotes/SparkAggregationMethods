import multiprocessing
import random
import numpy
from typing import Any


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
