from fastapi import APIRouter

from easyminer.config import API_V1_PREFIX

router = APIRouter(
    prefix=API_V1_PREFIX,
    tags=["Preprocessing API"],
)
