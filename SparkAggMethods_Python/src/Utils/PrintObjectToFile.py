import io
import os
import re
from typing import Any, TextIO, Optional, List, cast
import prettyprinter


def install_extras(
        include: Optional[List[str]] = None,
        *,
        exclude: List[str] = [],
        raise_on_error: bool = True,
        warn_on_error: bool = False) -> None:
    prettyprinter.install_extras(
        include=frozenset(include)
        if include is not None
        else prettyprinter.ALL_EXTRAS,
        exclude=frozenset(exclude),
        raise_on_error=raise_on_error,
        warn_on_error=warn_on_error)


install_extras(
    include=['requests', 'dataclasses'])


def pprint(
        object: Any,
        stream: TextIO,
        indent: int = 4,
        width: int = 79,
        depth: Optional[int] = None,
        compact: bool = False,
        ribbon_width: int = 71,
        max_seq_len: int = 1000,
        sort_dict_keys: bool = False,
        end: str = "\n") -> None:
    cast(Any, prettyprinter).pprint(
        object,
        stream,
        indent=indent,
        width=width,
        depth=depth,
        compact=compact,
        ribbon_width=ribbon_width,
        max_seq_len=max_seq_len,
        sort_dict_keys=sort_dict_keys,
        end=end)


def pick_file_path(suffix: str) -> str:
    return os.path.join(os.environ['TMP'], f"LastUnitTestData{suffix}.py")

re_match_dt_1: re.Pattern[str] | None = None

def pprints(rows: Any, fh: TextIO) -> None:
    global re_match_dt_1, re_match_dt_2, re_match_2, re_match_3
    with io.StringIO(newline='\n') as intermediateStream:
        pprint(
            rows,
            stream=intermediateStream)
        text = intermediateStream.getvalue()

        if re_match_dt_1 is None:
            re_match_dt_1 = re.compile(r"(\w+\.)+(?P<type>\w+)\(", re.MULTILINE)
        text = re_match_dt_1.sub(r"\g<type>(", text)

        fh.write(text)

def PrintObjectAsPythonLiteral(rows: Any, suffix: str = "") -> None:
    with open(pick_file_path(suffix), "w", encoding="utf-8") as fh:
        pprints(rows, fh)


def PrintText(src: str, suffix: str = "") -> None:
    with open(pick_file_path(suffix), "w", encoding='utf8') as fh:
        print(src, file=fh)
