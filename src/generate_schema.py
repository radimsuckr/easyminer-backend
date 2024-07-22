from tortoise import Tortoise, run_async

from .easyminer.settings import TORTOISE_ORM


async def connect_to_database():
    await Tortoise.init(TORTOISE_ORM)


async def main():
    await connect_to_database()
    await Tortoise.generate_schemas()


if __name__ == "__main__":
    run_async(main())
