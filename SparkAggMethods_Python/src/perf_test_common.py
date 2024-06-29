from dataclasses import dataclass
from enum import Enum
from typing import Iterable


@dataclass(frozen=True)
class ChallengeMethodExternalRegistration:
    strategy_name: str
    language: str
    interface: str


@dataclass(frozen=True)
class ChallengeMethodDescription:
    data_name: str
    raw_method_name: str
    language: str
    interface: str


class CalcEngine(Enum):
    PYSPARK = 'pyspark'
    DASK = 'dask'
    SCALA_SPARK = 'spark'


def count_iter(
        iterator: Iterable
):
    count = 0
    for _ in iterator:
        count += 1
    return count
