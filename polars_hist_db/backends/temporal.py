from typing import Optional

from ..core import TimeHint


def system_time_hint_clause(time_hint: Optional[TimeHint]) -> Optional[str]:
    if time_hint is None:
        return None
    return time_hint.build()
