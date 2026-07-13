from __future__ import annotations

from typing import Protocol

from .operations import CompositionRevision


class LayerCompositionStore(Protocol):
    def append(self, revision: CompositionRevision, actor_id: str) -> None: ...

    def revisions(
        self, layer_id: str | None = None
    ) -> tuple[CompositionRevision, ...]: ...


class InMemoryLayerCompositionStore:
    def __init__(self) -> None:
        self._revisions: list[CompositionRevision] = []

    def append(self, revision: CompositionRevision, actor_id: str) -> None:
        del actor_id
        if any(item.revision_id == revision.revision_id for item in self._revisions):
            raise ValueError("composition revision already exists")
        self._revisions.append(revision)

    def revisions(self, layer_id: str | None = None) -> tuple[CompositionRevision, ...]:
        return tuple(
            item
            for item in sorted(
                self._revisions,
                key=lambda value: (value.recorded_at, value.revision_id),
            )
            if layer_id is None or item.layer_id == layer_id
        )
