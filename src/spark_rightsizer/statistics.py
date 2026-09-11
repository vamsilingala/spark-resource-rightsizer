"""Small statistical helpers with no third-party runtime dependency."""

from __future__ import annotations

import math
from typing import Iterable, List, Optional, Tuple


def numeric(values: Iterable[Optional[float]]) -> List[float]:
    return [float(value) for value in values if value is not None and math.isfinite(float(value))]


def quantile(values: Iterable[Optional[float]], fraction: float) -> Optional[float]:
    ordered = sorted(numeric(values))
    if not ordered:
        return None
    position = min(1.0, max(0.0, fraction)) * (len(ordered) - 1)
    low = int(math.floor(position))
    high = int(math.ceil(position))
    if low == high:
        return ordered[low]
    return ordered[low] + (ordered[high] - ordered[low]) * (position - low)


def weighted_quantile(weighted_values: Iterable[Tuple[float, float]], fraction: float) -> Optional[float]:
    ordered = sorted((value, weight) for value, weight in weighted_values if weight > 0)
    if not ordered:
        return None
    target = sum(weight for _, weight in ordered) * min(1.0, max(0.0, fraction))
    cumulative = 0.0
    for value, weight in ordered:
        cumulative += weight
        if cumulative >= target:
            return value
    return ordered[-1][0]


def percentage_change(before: Optional[float], after: Optional[float]) -> Optional[float]:
    if before is None or after is None or before <= 0:
        return None
    return round((before - after) * 100.0 / before, 2)


def percent_value(value: Optional[float]) -> Optional[float]:
    if value is None:
        return None
    return value * 100.0 if 0.0 <= value <= 1.0 else value
