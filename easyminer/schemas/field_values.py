"""Schema definitions for field values."""

from typing import Any

from pydantic import BaseModel


class Value(BaseModel):
    """A value for a field with its frequency."""

    id: int
    value: Any
    frequency: int
