import abc
from pathlib import Path


class Storage(abc.ABC):
    @abc.abstractmethod
    def save(self, path: Path, content: bytes) -> tuple[int, Path]: ...

    @abc.abstractmethod
    def read(self, path: Path) -> bytes: ...

    @abc.abstractmethod
    def exists(self, path: Path) -> bool: ...

    @abc.abstractmethod
    def list_files(self, path: Path, pattern: str = "*") -> list[Path]: ...


class DiskStorage(Storage):
    def __init__(self, root: Path) -> None:
        super().__init__()
        self._root = root.resolve()
        self._root.mkdir(parents=True, exist_ok=True)

    def save(self, path: Path, content: bytes):
        path = self._root / path
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("wb") as f:
            written = f.write(content)
        return written, path

    def read(self, path: Path) -> bytes:
        """Read content from a file.

        Args:
            path: Path to the file, relative to the storage root

        Returns:
            The file contents as bytes

        Raises:
            FileNotFoundError: If the file doesn't exist
        """
        full_path = self._root / path
        if not full_path.exists():
            raise FileNotFoundError(f"File not found: {path}")
        return full_path.read_bytes()

    def exists(self, path: Path) -> bool:
        """Check if a file or directory exists.

        Args:
            path: Path to check, relative to the storage root

        Returns:
            True if the file or directory exists, False otherwise
        """
        return (self._root / path).exists()

    def list_files(self, path: Path, pattern: str = "*") -> list[Path]:
        """List files in a directory.

        Args:
            path: Directory path, relative to the storage root
            pattern: Glob pattern to filter files

        Returns:
            List of relative paths to matching files

        Raises:
            FileNotFoundError: If the directory doesn't exist
        """
        dir_path = self._root / path
        if not dir_path.exists():
            raise FileNotFoundError(f"Directory not found: {path}")

        # Return file paths relative to storage root
        return [p.relative_to(self._root) for p in dir_path.glob(pattern)]
