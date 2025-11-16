import bz2
import gzip
import io
import zipfile

MAX_DECOMPRESSED_SIZE = 10 * 1024 * 1024  # 10 MB


class CompressionError(Exception):
    pass


def decompress_zip(data: bytes) -> str:
    try:
        with zipfile.ZipFile(io.BytesIO(data), "r") as zf:
            filenames = zf.namelist()
            if not filenames:
                raise CompressionError("ZIP archive is empty")

            first_file = filenames[0]
            info = zf.getinfo(first_file)

            if info.file_size > MAX_DECOMPRESSED_SIZE:
                raise CompressionError(
                    f"Decompressed size {info.file_size} bytes exceeds {MAX_DECOMPRESSED_SIZE} byte limit"
                )

            content = zf.read(first_file)
            return content.decode("utf-8", errors="replace")

    except zipfile.BadZipFile as e:
        raise CompressionError(f"Corrupted ZIP archive: {e}")
    except UnicodeDecodeError as e:
        raise CompressionError(f"Failed to decode ZIP content as UTF-8: {e}")
    except Exception as e:
        raise CompressionError(f"ZIP extraction failed: {e}")


def decompress_gzip(data: bytes) -> str:
    try:
        decompressed = gzip.decompress(data)

        if len(decompressed) > MAX_DECOMPRESSED_SIZE:
            raise CompressionError(
                f"Decompressed size {len(decompressed)} bytes exceeds {MAX_DECOMPRESSED_SIZE} byte limit"
            )

        return decompressed.decode("utf-8", errors="replace")

    except gzip.BadGzipFile as e:
        raise CompressionError(f"Corrupted GZIP archive: {e}")
    except UnicodeDecodeError as e:
        raise CompressionError(f"Failed to decode GZIP content as UTF-8: {e}")
    except Exception as e:
        raise CompressionError(f"GZIP decompression failed: {e}")


def decompress_bzip2(data: bytes) -> str:
    try:
        decompressed = bz2.decompress(data)

        if len(decompressed) > MAX_DECOMPRESSED_SIZE:
            raise CompressionError(
                f"Decompressed size {len(decompressed)} bytes exceeds {MAX_DECOMPRESSED_SIZE} byte limit"
            )

        return decompressed.decode("utf-8", errors="replace")

    except OSError as e:
        raise CompressionError(f"Corrupted BZIP2 archive: {e}")
    except UnicodeDecodeError as e:
        raise CompressionError(f"Failed to decode BZIP2 content as UTF-8: {e}")
    except Exception as e:
        raise CompressionError(f"BZIP2 decompression failed: {e}")


def extract_first_n_lines(text: str, n: int) -> str:
    lines = text.split("\n", n)
    return "\n".join(lines[:n])
