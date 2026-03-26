import re
from pathlib import Path
from typing import override

from easyminer.config import ROOT_DIR

from .storage import Storage


class DiskStorage(Storage):
    def __init__(self, root: Path | None = None) -> None:
        super().__init__()

        self._root: Path = root or (ROOT_DIR / "var" / "storage")
        self._root.mkdir(parents=True, exist_ok=True)

    @override
    def save(self, key: str, content: bytes) -> str:
        path = self._root / key
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("wb") as f:
            f.write(content)
        return key

    @override
    def read(self, key: str) -> bytes:
        full_path = self._root / key
        if not full_path.exists():
            raise FileNotFoundError(f"File not found: {key}")
        return full_path.read_bytes()

    @override
    def exists(self, key: str) -> bool:
        return (self._root / key).exists()

    @override
    def list_files(self, prefix: str, pattern: re.Pattern[str] | None = None) -> list[str]:
        dir_path = self._root / prefix
        if not dir_path.exists():
            raise FileNotFoundError(f"Directory not found: {prefix}")

        files = [str(p.relative_to(self._root)) for p in dir_path.iterdir() if p.is_file()]
        if pattern is not None:
            files = [f for f in files if pattern.search(f)]
        return files
