from collections import defaultdict, deque
from datetime import timedelta
from typing import ClassVar


class Clock:
    _timings: ClassVar[dict[str, deque[float]]] = defaultdict(
        lambda: deque(maxlen=1000)
    )

    def add_timing(self, name: str, timing: float) -> None:
        self._timings[name].append(timing)

    def get_avg(self, name: str, window_size: int = 5) -> float:
        values = list(self._timings[name])[-window_size:]
        return sum(values) / len(values)

    def eta(self, name: str, count_remaining: int, window_size: int = 5) -> timedelta:
        avg_seconds = self.get_avg(name, window_size)
        seconds_remaining = int(avg_seconds * count_remaining)
        return timedelta(seconds=seconds_remaining)
