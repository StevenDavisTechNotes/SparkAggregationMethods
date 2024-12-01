from types import FunctionType
from typing import Callable


# from https://stackoverflow.com/questions/2553354/simpler-way-to-create-dictionary-of-separate-variables
def name_of(obj, callingLocals=locals()) -> str:
    """
    quick function to print the first variable's name that has the given value
    If not for the default-Valued callingLocals, the function would always
    get the name as "obj", which is not what I want.
    """
    allLocals = list(callingLocals.items())
    for k, v in allLocals:
        if v is obj:
            return k
    raise Exception("That variable isn't defined")


def name_of_function(func: Callable) -> str:
    """
    quick function to print the first variable's name that has the given value
    If not for the default-Valued callingLocals, the function would always
    get the name as "obj", which is not what I want.
    """
    assert isinstance(func, FunctionType)
    return func.__name__
