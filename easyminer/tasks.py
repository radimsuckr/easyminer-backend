from easyminer.worker import app


@app.task
def add(x: int, y: int) -> int:
    return x + y
