from typing import Iterable
from dataclasses import dataclass


@dataclass(frozen=True)
class ExternalTestMethod:
    strategy_name: str
    language: str
    interface: str


@dataclass(frozen=True)
class TestMethodDescription:
    data_name: str
    raw_method_name: str
    language: str
    interface: str


def count_iter(
        iterator: Iterable
):
    count = 0
    for _ in iterator:
        count += 1
    return count
