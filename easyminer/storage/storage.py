import abc
import re


class Storage(abc.ABC):
    @abc.abstractmethod
    def save(self, key: str, content: bytes) -> str: ...

    @abc.abstractmethod
    def read(self, key: str) -> bytes: ...

    @abc.abstractmethod
    def exists(self, key: str) -> bool: ...

    @abc.abstractmethod
    def list_files(self, prefix: str, pattern: re.Pattern[str] | None = None) -> list[str]: ...
