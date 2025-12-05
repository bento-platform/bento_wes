import itertools
from typing import Any

__all__ = ["denest_list"]


def denest_list(x: Any) -> list:
    if isinstance(x, list):
        return list(itertools.chain.from_iterable(map(denest_list, x)))
    return [x]
