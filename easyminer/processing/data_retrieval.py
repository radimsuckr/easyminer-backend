import csv
import io
import json
import logging
import math
from pathlib import Path
from typing import Any

import numpy as np
from sqlalchemy.ext.asyncio import AsyncSession

from easyminer.models.data import DataSource, Field
from easyminer.storage import DiskStorage

logger = logging.getLogger(__name__)


class DataRetrieval:
    """Class for retrieving data from stored chunks."""

    def __init__(
        self,
        storage: DiskStorage,
        data_source_id: int,
        encoding: str = "utf-8",
        separator: str = ",",
        quote_char: str = '"',
    ):
        """Initialize data retrieval.

        Args:
            storage: Storage instance for accessing files
            data_source_id: Data source ID
            encoding: Text encoding for CSV files
            separator: CSV separator character
            quote_char: CSV quote character
        """
        self.storage = storage
        self.data_source_id = data_source_id
        self.encoding = encoding
        self.separator = separator
        self.quote_char = quote_char
        self.logger = logging.getLogger(__name__)
        self.chunks_dir = Path(f"{data_source_id}/chunks")
        self.results_dir = Path(f"{data_source_id}/results")

    async def get_preview_data(
        self, limit: int = 10, field_ids: list[int] | None = None
    ) -> tuple[list[str], list[dict[str, Any]]]:
        """Get preview data from a data source.

        Args:
            limit: Maximum number of rows to return
            field_ids: List of field IDs to include (or None for all fields)

        Returns:
            Tuple of (field_names, rows)
            field_names is a list of field names
            rows is a list of dictionaries mapping field names to values

        Raises:
            FileNotFoundError: If no data chunks are found
        """
        try:
            # Get list of chunk files
            chunk_files = self.storage.list_files(self.chunks_dir, "*.chunk")
            if not chunk_files:
                raise FileNotFoundError(
                    f"No chunks found for data source {self.data_source_id}"
                )

            # Read the first chunk (which should contain the header and initial rows)
            chunk_data = self.storage.read(chunk_files[0])

            # Parse the CSV
            try:
                text = chunk_data.decode(self.encoding)
            except UnicodeDecodeError:
                self.logger.warning(
                    f"Error decoding with {self.encoding}, falling back to utf-8"
                )
                text = chunk_data.decode("utf-8", errors="replace")

            # Parse CSV
            reader = csv.reader(
                io.StringIO(text), delimiter=self.separator, quotechar=self.quote_char
            )

            # Get header and rows
            rows = list(reader)
            if not rows:
                return [], []

            header = rows[0]
            data_rows = rows[1 : min(limit + 1, len(rows))]

            # If we need more rows and have more chunks, read additional chunks
            remaining_rows = limit - len(data_rows)
            chunk_index = 1

            while remaining_rows > 0 and chunk_index < len(chunk_files):
                try:
                    # Read the next chunk
                    next_chunk_data = self.storage.read(chunk_files[chunk_index])
                    next_text = next_chunk_data.decode(self.encoding, errors="replace")
                    next_reader = csv.reader(
                        io.StringIO(next_text),
                        delimiter=self.separator,
                        quotechar=self.quote_char,
                    )

                    # Skip header if this isn't the first chunk
                    next_rows = list(next_reader)
                    additional_rows = next_rows[
                        1 : min(remaining_rows + 1, len(next_rows))
                    ]

                    # Add to our data
                    data_rows.extend(additional_rows)
                    remaining_rows -= len(additional_rows)

                except Exception as e:
                    self.logger.error(
                        f"Error reading chunk {chunk_files[chunk_index]}: {e}"
                    )
                    break

                chunk_index += 1

            # Convert rows to dictionaries
            result_rows = []
            for row in data_rows:
                if len(row) != len(header):
                    # Skip rows with wrong number of columns
                    continue

                row_dict = {header[i]: row[i] for i in range(len(header))}
                result_rows.append(row_dict)

            return header, result_rows

        except Exception as e:
            self.logger.error(f"Error retrieving preview data: {e}")
            raise

    async def generate_histogram(
        self,
        field: Field,
        bins: int = 10,
        min_value: float | None = None,
        max_value: float | None = None,
        min_inclusive: bool = True,
        max_inclusive: bool = True,
    ) -> tuple[list[dict[str, Any]], str]:
        """Generate histogram data for a numeric field.

        Args:
            field: The field to generate histogram for
            bins: Number of bins to use
            min_value: Minimum value to include (or None to use field min)
            max_value: Maximum value to include (or None to use field max)
            min_inclusive: Whether min_value is inclusive
            max_inclusive: Whether max_value is inclusive

        Returns:
            Tuple of (histogram_data, json_filename)
            histogram_data is a list of dictionaries with keys:
                - interval_start: Start of interval
                - interval_end: End of interval
                - count: Number of values in interval
            json_filename is the path to the saved JSON file

        Raises:
            ValueError: If field is not numeric or other validation error
        """
        if field.data_type not in ["integer", "float", "numeric"]:
            raise ValueError(f"Field {field.name} is not numeric")

        # Determine range for histogram
        actual_min = float(field.min_value) if field.min_value is not None else 0
        actual_max = float(field.max_value) if field.max_value is not None else 0

        if min_value is not None:
            range_min = min_value
        else:
            range_min = actual_min

        if max_value is not None:
            range_max = max_value
        else:
            range_max = actual_max

        # Apply inclusivity
        if not min_inclusive and range_min == actual_min:
            range_min += 1e-10  # Small epsilon for float comparison
        if not max_inclusive and range_max == actual_max:
            range_max -= 1e-10  # Small epsilon for float comparison

        # Check the range is valid
        if range_min >= range_max:
            raise ValueError(f"Invalid range: min={range_min}, max={range_max}")

        # Get all values for the field
        field_values = []

        try:
            # Get list of chunk files
            chunk_files = self.storage.list_files(self.chunks_dir, "*.chunk")
            if not chunk_files:
                raise FileNotFoundError(
                    f"No chunks found for data source {self.data_source_id}"
                )

            # Process each chunk file
            for chunk_file in chunk_files:
                try:
                    chunk_data = self.storage.read(chunk_file)
                    text = chunk_data.decode(self.encoding, errors="replace")

                    reader = csv.reader(
                        io.StringIO(text),
                        delimiter=self.separator,
                        quotechar=self.quote_char,
                    )

                    rows = list(reader)
                    if not rows:
                        continue

                    header = rows[0]
                    data_rows = rows[1:]

                    # Find the field's index in the header
                    try:
                        field_index = header.index(field.name)
                    except ValueError:
                        self.logger.warning(
                            f"Field {field.name} not found in header: {header}"
                        )
                        continue

                    # Collect numeric values for this field
                    for row in data_rows:
                        if field_index < len(row) and row[field_index]:
                            try:
                                value = float(row[field_index])

                                # Only include values in our specified range
                                if (
                                    min_value is not None
                                    and min_inclusive
                                    and value < min_value
                                ):
                                    continue
                                if (
                                    min_value is not None
                                    and not min_inclusive
                                    and value <= min_value
                                ):
                                    continue
                                if (
                                    max_value is not None
                                    and max_inclusive
                                    and value > max_value
                                ):
                                    continue
                                if (
                                    max_value is not None
                                    and not max_inclusive
                                    and value >= max_value
                                ):
                                    continue

                                field_values.append(value)
                            except ValueError:
                                # Skip non-numeric values
                                continue
                except Exception as e:
                    self.logger.error(f"Error processing chunk {chunk_file}: {e}")
                    continue

            # Now create the histogram
            if not field_values:
                # Create empty histogram with regular intervals
                step = (range_max - range_min) / bins
                histogram_data = []

                for i in range(bins):
                    interval_start = range_min + i * step
                    interval_end = (
                        range_min + (i + 1) * step if i < bins - 1 else range_max
                    )
                    histogram_data.append(
                        {
                            "interval_start": interval_start,
                            "interval_end": interval_end,
                            "count": 0,
                        }
                    )
            else:
                # Create histogram from actual data
                if field.data_type == "integer":
                    # For integers, use integer bins
                    range_min = math.floor(range_min)
                    range_max = math.ceil(range_max)
                    bin_edges = np.linspace(range_min, range_max, bins + 1).astype(int)
                else:
                    # For floats, use float bins
                    bin_edges = np.linspace(range_min, range_max, bins + 1)

                hist, bin_edges = np.histogram(field_values, bins=bin_edges)

                # Convert to expected format
                histogram_data = []
                for i in range(len(hist)):
                    interval_start = float(bin_edges[i])
                    interval_end = float(bin_edges[i + 1])
                    histogram_data.append(
                        {
                            "interval_start": interval_start,
                            "interval_end": interval_end,
                            "count": int(hist[i]),
                        }
                    )

            # Save results to a JSON file
            result_file = f"histogram_{field.id}_{bins}.json"
            result_path = self.results_dir / result_file

            # Create directory if it doesn't exist
            if not self.storage.exists(self.results_dir):
                self.storage.save(self.results_dir / ".keep", b"")  # Create directory

            # Save to JSON
            json_content = json.dumps(
                {
                    "field_id": field.id,
                    "field_name": field.name,
                    "bins": bins,
                    "min_value": range_min,
                    "max_value": range_max,
                    "min_inclusive": min_inclusive,
                    "max_inclusive": max_inclusive,
                    "histogram": histogram_data,
                },
                indent=2,
            )

            self.storage.save(result_path, json_content.encode("utf-8"))

            return histogram_data, str(result_path)

        except Exception as e:
            self.logger.error(f"Error generating histogram: {e}")
            raise

    async def read_file_result(self, result_path: str) -> dict[str, Any]:
        """Read a result file.

        Args:
            result_path: Path to the result file

        Returns:
            The file contents parsed as JSON

        Raises:
            FileNotFoundError: If the file doesn't exist
            ValueError: If the file is not valid JSON
        """
        try:
            content = self.storage.read(Path(result_path))
            return json.loads(content.decode("utf-8"))
        except FileNotFoundError:
            raise
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in result file: {e}")
        except Exception as e:
            self.logger.error(f"Error reading result file: {e}")
            raise


async def get_data_preview(
    db: AsyncSession,
    data_source: DataSource,
    limit: int = 10,
    field_ids: list[int] | None = None,
) -> tuple[list[str], list[dict[str, Any]]]:
    """Get a preview of data from a data source.

    Args:
        db: Database session
        data_source: Data source object
        limit: Maximum number of rows to return
        field_ids: List of field IDs to include (or None for all fields)

    Returns:
        Tuple of (field_names, rows)
    """
    # Get data source settings - use defaults
    encoding = "utf-8"
    separator = ","
    quote_char = '"'

    # Create data retrieval instance
    storage = DiskStorage(Path("../../var/data"))
    retrieval = DataRetrieval(
        storage=storage,
        data_source_id=data_source.id,
        encoding=encoding,
        separator=separator,
        quote_char=quote_char,
    )

    try:
        # Get preview data
        header, rows = await retrieval.get_preview_data(limit=limit)

        # If field_ids is provided, filter the results
        if field_ids:
            # Get field names to include
            included_fields = []
            for field_id in field_ids:
                from easyminer.crud.aio.field import get_field_by_id

                field = await get_field_by_id(db, field_id, data_source.id)
                if field:
                    included_fields.append(field.name)

            # Filter header and rows
            filtered_header = [name for name in header if name in included_fields]
            filtered_rows = []
            for row in rows:
                filtered_row = {
                    name: value
                    for name, value in row.items()
                    if name in included_fields
                }
                filtered_rows.append(filtered_row)

            return filtered_header, filtered_rows

        return header, rows

    except Exception as e:
        logger.error(f"Error retrieving preview data: {e}")
        return [], []


async def generate_histogram_for_field(
    db: AsyncSession,
    field: Field,
    data_source: DataSource,
    bins: int = 10,
    min_value: float | None = None,
    max_value: float | None = None,
    min_inclusive: bool = True,
    max_inclusive: bool = True,
) -> tuple[list[dict[str, Any]], str]:
    """Generate a histogram for a numeric field.

    Args:
        db: Database session
        field: Field to generate histogram for
        data_source: Data source
        bins: Number of bins to use
        min_value: Minimum value to include
        max_value: Maximum value to include
        min_inclusive: Whether min_value is inclusive
        max_inclusive: Whether max_value is inclusive

    Returns:
        Tuple of (histogram_data, result_path)
    """
    # Get data source settings - use defaults
    encoding = "utf-8"
    separator = ","
    quote_char = '"'

    # Create data retrieval instance
    storage = DiskStorage(Path("../../var/data"))
    retrieval = DataRetrieval(
        storage=storage,
        data_source_id=data_source.id,
        encoding=encoding,
        separator=separator,
        quote_char=quote_char,
    )

    # Generate histogram
    histogram_data, result_path = await retrieval.generate_histogram(
        field=field,
        bins=bins,
        min_value=min_value,
        max_value=max_value,
        min_inclusive=min_inclusive,
        max_inclusive=max_inclusive,
    )

    return histogram_data, result_path


async def read_task_result(result_path: str) -> dict[str, Any]:
    """Read a task result file.

    Args:
        result_path: Path to the result file

    Returns:
        The file contents parsed as JSON
    """
    storage = DiskStorage(Path("../../var/data"))

    try:
        content = storage.read(Path(result_path))
        return json.loads(content.decode("utf-8"))
    except FileNotFoundError:
        logger.error(f"Result file {result_path} not found")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Error decoding JSON in result file {result_path}: {e}")
        raise ValueError(f"Invalid JSON format in result file: {e}")
    except Exception as e:
        logger.error(f"Error reading result file {result_path}: {e}")
        raise
