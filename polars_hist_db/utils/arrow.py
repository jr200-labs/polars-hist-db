from __future__ import annotations

import pyarrow as pa


class ArrowSchemaContractError(ValueError):
    pass


def require_unique_arrow_field_names(
    schema: pa.Schema, *, context: str = "Arrow schema"
) -> None:
    duplicates = sorted(
        name
        for name in set(schema.names)
        if len(schema.get_all_field_indices(name)) > 1
    )
    if duplicates:
        raise ArrowSchemaContractError(
            f"{context} contains duplicate field names: {', '.join(duplicates)}"
        )
