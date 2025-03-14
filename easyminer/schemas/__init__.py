from humps import camelize
from pydantic import BaseModel, ConfigDict


class BaseSchema(BaseModel):
    """
    Base schema model that other models will inherit from.
    Uses snake_case for Python and camelCase for JSON serialization/deserialization.
    """

    model_config = ConfigDict(
        populate_by_name=True,  # Allow population by Python attribute names
        alias_generator=camelize,  # Convert field names to camelCase for JSON
        json_schema_extra={"by_alias": True},  # Use camelCase in JSON schema (API docs)
    )
