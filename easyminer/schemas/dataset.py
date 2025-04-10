from pydantic import Field

from easyminer.schemas import BaseSchema
from easyminer.schemas.data import DbType


class DatasetSchema(BaseSchema):
    id: int = Field(..., description="Dataset ID")
    name: str = Field(..., description="Dataset name")
    size: int = Field(..., description="Dataset size")
    type: DbType = Field(..., description="Database type")
    data_source_id: int = Field(..., description="Data source ID")
