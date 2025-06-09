from datetime import datetime
from uuid import UUID

from easyminer.schemas import BaseSchema


class Miner(BaseSchema):
    state: str
    task_id: UUID
    started: datetime = datetime(2025, 1, 1)
    result_url: str


class MineStartResponse(BaseSchema):
    code: str
    miner: Miner
