from typing import TextIO
from dataclasses import dataclass
import datetime

from VanillaPerfTest.VanillaDirectory import PythonTestMethod

@dataclass(frozen=True)
class RunResult:
    dataSize: int
    elapsedTime: float
    recordCount: int

def write_run_result(cond_method: PythonTestMethod, result: RunResult, file: TextIO):
    print(f'{cond_method.name},{cond_method.interface},{result.dataSize},{result.elapsedTime},{result.recordCount},{datetime.datetime.now().isoformat()},', file=file)
    file.flush()
