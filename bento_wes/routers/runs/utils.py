import itertools
from typing import Any


def _denest_list(x: Any) -> list:
    if isinstance(x, list):
        return list(itertools.chain.from_iterable(map(_denest_list, x)))
    return [x]
