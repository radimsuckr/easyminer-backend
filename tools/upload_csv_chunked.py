#!/usr/bin/env env python
import argparse
import sys
import time
from collections.abc import Iterator
from pathlib import Path

import httpx

# Default configuration
DEFAULT_BASE_URL = "http://localhost:8000/api/v1"
DEFAULT_MAX_CHUNK_SIZE = 500_000  # 1MB (note: server limit is 500KB + 5% overhead)
SERVER_MAX_CHUNK_SIZE = 500_000  # 500KB + 5% overhead as per server spec


class CSVChunker:
    """Handles CSV file chunking by rows to stay under size limit."""

    def __init__(self, file_path: Path, max_chunk_size: int):
        self.file_path = file_path
        self.max_chunk_size = max_chunk_size
        self.encoding = "utf-8"

    def estimate_row_size(self, sample_rows: int = 100) -> int:
        """
        Estimate average row size in bytes by sampling the first N rows.

        Args:
            sample_rows: Number of rows to sample

        Returns:
            Average row size in bytes
        """
        total_bytes = 0
        row_count = 0

        with open(self.file_path, encoding=self.encoding, newline="") as f:
            # Read header
            header_line = f.readline()
            total_bytes += len(header_line.encode(self.encoding))

            # Sample rows
            for i, line in enumerate(f):
                if i >= sample_rows:
                    break
                total_bytes += len(line.encode(self.encoding))
                row_count += 1

        if row_count == 0:
            return len(header_line.encode(self.encoding))

        avg_row_size = total_bytes // (row_count + 1)  # +1 for header
        return avg_row_size

    def chunk_by_rows(self) -> Iterator[tuple[str, int, int]]:
        """
        Yield chunks of CSV data, ensuring each chunk is under max_chunk_size.

        Yields:
            Tuple of (chunk_data, row_count, chunk_size_bytes)
        """
        with open(self.file_path, encoding=self.encoding, newline="") as f:
            # Read and store header
            header_line = f.readline()
            header_bytes = len(header_line.encode(self.encoding))

            # Estimate row size to determine optimal chunk size
            f.seek(0)  # Reset to beginning
            avg_row_size = self.estimate_row_size()

            # Calculate how many rows we can fit in a chunk
            available_space = self.max_chunk_size - header_bytes
            if available_space <= 0:
                raise ValueError(
                    f"Header size ({header_bytes} bytes) exceeds max chunk size ({self.max_chunk_size} bytes)"
                )

            estimated_rows_per_chunk = max(1, int(available_space / avg_row_size * 0.9))  # 90% safety margin
            print(f"[INFO] Estimated average row size: {avg_row_size} bytes")
            print(f"[INFO] Estimated rows per chunk: {estimated_rows_per_chunk}")

            # Reset and skip header for chunking
            f.seek(0)
            next(f)  # Skip header

            chunk_lines = [header_line]
            chunk_size = header_bytes
            row_count = 0
            total_rows = 0
            chunk_num = 0
            first_chunk = True

            for line in f:
                line_bytes = len(line.encode(self.encoding))

                # Check if adding this line would exceed the limit
                # For first chunk, include header in size calculation
                if first_chunk:
                    would_exceed = (chunk_size + line_bytes) > self.max_chunk_size
                else:
                    # For subsequent chunks, only count lines (no header)
                    would_exceed = (
                        sum(len(chunk_line.encode(self.encoding)) for chunk_line in chunk_lines[1:]) + line_bytes
                    ) > self.max_chunk_size

                if would_exceed and row_count > 0:
                    # Yield current chunk
                    chunk_data = "".join(chunk_lines)
                    actual_size = len(chunk_data.encode(self.encoding))
                    chunk_num += 1
                    print(f"[CHUNK {chunk_num}] {row_count} rows, {actual_size:,} bytes")
                    yield chunk_data, row_count, actual_size

                    total_rows += row_count

                    # Start new chunk with header
                    chunk_lines = [header_line]
                    chunk_size = header_bytes
                    row_count = 0
                    first_chunk = False

                # Add line to current chunk
                chunk_lines.append(line)
                chunk_size += line_bytes
                row_count += 1

            # Yield final chunk if there's data
            if row_count > 0:
                chunk_data = "".join(chunk_lines)
                actual_size = len(chunk_data.encode(self.encoding))
                chunk_num += 1
                print(f"[CHUNK {chunk_num}] {row_count} rows, {actual_size:,} bytes")
                yield chunk_data, row_count, actual_size
                total_rows += row_count

            print(f"[INFO] Total rows processed: {total_rows}")


class CSVUploader:
    """Handles uploading CSV chunks to the EasyMiner API."""

    def __init__(self, base_url: str, upload_id: str, api_key: str):
        self.base_url = base_url.rstrip("/")
        self.upload_id = upload_id
        self.api_key = api_key
        self.upload_url = f"{self.base_url}/upload/{self.upload_id}"

        # Create httpx client with headers
        self.session = httpx.Client(
            headers={
                "Authorization": f"ApiKey {self.api_key}",
                "Content-Type": "text/plain",
            }
        )

    def upload_chunk(self, chunk_data: str, chunk_num: int) -> bool:
        """
        Upload a single chunk to the API.

        Args:
            chunk_data: The CSV chunk data
            chunk_num: Chunk number for logging

        Returns:
            True if upload was successful and should continue, False if complete

        Raises:
            Exception if upload fails
        """
        retry_count = 0
        max_retries = 300

        while retry_count < max_retries:
            try:
                response = self.session.post(
                    self.upload_url,
                    content=chunk_data.encode("utf-8"),
                    timeout=30,
                )

                if response.status_code == 202:
                    # Chunk accepted, continue with next
                    print(f"[UPLOAD] Chunk {chunk_num} accepted (202 Accepted)")
                    return True

                elif response.status_code == 200:
                    # Upload complete
                    print(f"[UPLOAD] Chunk {chunk_num} completed upload (200 OK)")
                    try:
                        result = response.json()
                        print(f"[SUCCESS] Data source created: {result}")
                        return False  # Upload is complete
                    except Exception:
                        # If response is not JSON, just treat as success
                        return False

                elif response.status_code == 429:
                    # Too many requests, wait and retry
                    print("[UPLOAD] Rate limited (429), waiting 0.5s before retry...")
                    time.sleep(0.5)
                    retry_count += 1
                    continue

                elif response.status_code == 413:
                    # Chunk too large
                    error_msg = f"Chunk {chunk_num} is too large (413 Payload Too Large)"
                    try:
                        error_details = response.json()
                        error_msg += f": {error_details}"
                    except Exception:
                        pass
                    raise Exception(error_msg)

                elif response.status_code == 403:
                    # Upload already closed
                    raise Exception("Upload already closed (403 Forbidden)")

                elif response.status_code == 404:
                    # Upload not found
                    try:
                        error_details = response.json()
                        raise Exception(f"Upload not found (404): {error_details}")
                    except Exception as e:
                        if "Upload not found" in str(e):
                            raise
                        raise Exception("Upload not found (404 Not Found)")

                else:
                    # Other error
                    error_msg = f"Upload failed with status {response.status_code}"
                    try:
                        error_details = response.json()
                        error_msg += f": {error_details}"
                    except Exception:
                        error_msg += f": {response.text}"
                    raise Exception(error_msg)

            except (httpx.HTTPError, httpx.TimeoutException, Exception) as e:
                if isinstance(e, (httpx.HTTPError, httpx.TimeoutException)):
                    print(f"[ERROR] Network error: {e}")
                    retry_count += 1
                    if retry_count < max_retries:
                        print(f"[RETRY] Retrying ({retry_count}/{max_retries})...")
                        time.sleep(1)
                    else:
                        raise Exception(f"Failed after {max_retries} retries: {e}")
                else:
                    # Re-raise if not a network error
                    raise

        raise Exception(f"Failed to upload chunk {chunk_num} after {max_retries} retries")

    def finalize_upload(self) -> dict:
        """
        Send empty POST to signal upload completion.

        Returns:
            API response with data source details
        """
        print("[FINALIZE] Sending completion signal (empty POST)...")

        try:
            response = self.session.post(
                self.upload_url,
                content=b"",
                timeout=30,
            )

            if response.status_code == 200:
                result = response.json()
                print("[SUCCESS] Upload finalized successfully!")
                print(f"[RESULT] Data source ID: {result.get('id')}")
                print(f"[RESULT] Name: {result.get('name')}")
                print(f"[RESULT] Type: {result.get('type')}")
                print(f"[RESULT] Size: {result.get('size')} rows")
                return result

            else:
                error_msg = f"Finalization failed with status {response.status_code}"
                try:
                    error_details = response.json()
                    error_msg += f": {error_details}"
                except Exception:
                    error_msg += f": {response.text}"
                raise Exception(error_msg)

        except (httpx.HTTPError, httpx.TimeoutException) as e:
            raise Exception(f"Network error during finalization: {e}")


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(
        description="Upload CSV file in chunks to EasyMiner API",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Upload with default settings
  python upload_csv_chunked.py data.csv abc-123-def --api-key "mykey123"

  # Upload with custom URL and chunk size
  python upload_csv_chunked.py data.csv abc-123-def --api-key "mykey123" \\
      --url "http://api.example.com/api/v1" --max-chunk-size 500000

Notes:
  - The upload_id must be obtained first by calling POST /upload/start
  - Max chunk size is limited by the server (500KB + 5% overhead = ~525KB)
  - Using 1MB chunks may result in 413 errors; use --max-chunk-size 500000 to be safe
        """,
    )

    parser.add_argument(
        "csv_file",
        type=Path,
        help="Path to the CSV file to upload",
    )

    parser.add_argument(
        "upload_id",
        type=str,
        help="Upload ID obtained from POST /upload/start",
    )

    parser.add_argument(
        "--api-key",
        type=str,
        required=True,
        help="API key for authentication (format: 'ApiKey encoded_key')",
    )

    parser.add_argument(
        "--url",
        type=str,
        default=DEFAULT_BASE_URL,
        help=f"Base API URL (default: {DEFAULT_BASE_URL})",
    )

    parser.add_argument(
        "--max-chunk-size",
        type=int,
        default=DEFAULT_MAX_CHUNK_SIZE,
        help=f"Maximum chunk size in bytes (default: {DEFAULT_MAX_CHUNK_SIZE}, server limit: {SERVER_MAX_CHUNK_SIZE})",
    )

    args = parser.parse_args()

    # Validate inputs
    if not args.csv_file.exists():
        print(f"[ERROR] File not found: {args.csv_file}", file=sys.stderr)
        sys.exit(1)

    if not args.csv_file.is_file():
        print(f"[ERROR] Not a file: {args.csv_file}", file=sys.stderr)
        sys.exit(1)

    # Warn if chunk size exceeds server limit
    if args.max_chunk_size > SERVER_MAX_CHUNK_SIZE:
        print(f"[WARNING] Max chunk size ({args.max_chunk_size}) exceeds server limit ({SERVER_MAX_CHUNK_SIZE})")
        print(f"[WARNING] This may result in 413 errors. Consider using --max-chunk-size {SERVER_MAX_CHUNK_SIZE}")
        print()

    # Print configuration
    print("=" * 70)
    print("CSV Chunked Upload for EasyMiner")
    print("=" * 70)
    print(f"File:           {args.csv_file}")
    print(f"Upload ID:      {args.upload_id}")
    print(f"API URL:        {args.url}")
    print(f"Max chunk size: {args.max_chunk_size:,} bytes")
    print("=" * 70)
    print()

    try:
        # Initialize chunker and uploader
        chunker = CSVChunker(args.csv_file, args.max_chunk_size)
        uploader = CSVUploader(args.url, args.upload_id, args.api_key)

        # Process and upload chunks
        print("[START] Processing CSV file...")
        start_time = time.time()
        chunk_num = 0
        upload_complete = False

        for chunk_data, row_count, chunk_size in chunker.chunk_by_rows():
            chunk_num += 1

            # Upload chunk
            should_continue = uploader.upload_chunk(chunk_data, chunk_num)

            if not should_continue:
                # Server returned 200, upload is complete
                upload_complete = True
                break

            # Small delay between chunks to avoid overwhelming the server
            time.sleep(0.1)

        # If we haven't received a 200 yet, send the finalization signal
        if not upload_complete:
            _ = uploader.finalize_upload()

        end_time = time.time()
        elapsed_time = end_time - start_time

        print()
        print("=" * 70)
        print("[DONE] Upload completed successfully!")
        print(f"[TIME] Total upload time: {elapsed_time:.2f} seconds ({elapsed_time / 60:.2f} minutes)")
        print("=" * 70)

    except KeyboardInterrupt:
        print("\n[ABORTED] Upload cancelled by user", file=sys.stderr)
        sys.exit(130)

    except Exception as e:
        print(f"\n[FAILED] Upload failed: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
