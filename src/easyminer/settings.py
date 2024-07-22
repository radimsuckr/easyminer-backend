from fastapi import FastAPI
from tortoise.contrib.fastapi import register_tortoise

TORTOISE_ORM = {
    "connections": {"default": "mysql://easyminer:easyminer@127.0.0.1:3306/easyminer"},
    "apps": {
        "models": {
            "models": ["easyminer.model", "aerich.models"],
            "default_connection": "default",
        },
    },
}


def init_db(app: FastAPI) -> None:
    register_tortoise(app, config=TORTOISE_ORM)
