"""Schema definitions for field values."""

from typing import Any

from easyminer.schemas import BaseSchema


class Value(BaseSchema):
    """A value for a field with its frequency."""

    id: int
    value: Any = None
    frequency: int
