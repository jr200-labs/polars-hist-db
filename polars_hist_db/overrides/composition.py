from __future__ import annotations

from typing import Protocol

from .operations import CompositionRevision
from .pagination import Page, paginate


class LayerCompositionStore(Protocol):
    def append(self, revision: CompositionRevision, actor_id: str) -> None: ...

    def revisions(
        self,
        layer_id: str | None = None,
        *,
        cursor: str | None = None,
        limit: int = 100,
    ) -> Page[CompositionRevision]: ...


class InMemoryLayerCompositionStore:
    def __init__(self) -> None:
        self._revisions: list[CompositionRevision] = []

    def append(self, revision: CompositionRevision, actor_id: str) -> None:
        del actor_id
        if any(item.revision_id == revision.revision_id for item in self._revisions):
            raise ValueError("composition revision already exists")
        self._revisions.append(revision)

    def revisions(
        self,
        layer_id: str | None = None,
        *,
        cursor: str | None = None,
        limit: int = 100,
    ) -> Page[CompositionRevision]:
        return paginate(
            (
                item
                for item in self._revisions
                if layer_id is None or item.layer_id == layer_id
            ),
            lambda item: (item.recorded_at, item.revision_id),
            cursor=cursor,
            limit=limit,
        )
