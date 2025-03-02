import abc
from pathlib import Path


class Storage(abc.ABC):
    @abc.abstractmethod
    def save(self, path: Path, content: bytes) -> int: ...


class DiskStorage(Storage):
    def __init__(self, root: Path) -> None:
        super().__init__()
        self._root = root.resolve()

    def save(self, path: Path, content: bytes):
        path = self._root / path
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("wb") as f:
            return f.write(content)
