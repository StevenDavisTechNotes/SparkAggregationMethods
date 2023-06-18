from dataclasses import dataclass

@dataclass(frozen=True)
class RunResult:
    dataSize: int
    elapsedTime: float
    recordCount: int
