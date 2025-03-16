import csv
import io
import logging
from pathlib import Path
from typing import Any

from sqlalchemy import insert
from sqlalchemy.ext.asyncio import AsyncSession

from easyminer.models.data import DataSource, Field, FieldType
from easyminer.schemas import BaseSchema
from easyminer.storage import DiskStorage


class FieldStats(BaseSchema):
    missing_count: int
    unique_count: int
    min_value: str | int | float | None
    max_value: str | int | float | None
    avg_value: float | None
    has_nulls: bool


class CsvProcessor:
    """Process CSV files from uploaded chunks."""

    def __init__(
        self,
        data_source: DataSource,
        db: AsyncSession,
        data_source_id: int,
        encoding: str = "utf-8",
        separator: str = ",",
        quote_char: str = '"',
    ):
        """Initialize the CSV processor.

        Args:
            data_source: The data source containing the uploaded CSV chunks
            db: Database session for updates
            data_source_id: The ID of the data source
            encoding: The encoding to use for CSV parsing
            separator: The separator character for CSV parsing
            quote_char: The quote character for CSV parsing
        """
        self.data_source = data_source
        self.data_source_id = data_source_id
        self.db = db
        self.logger = logging.getLogger(__name__)
        self.encoding = encoding
        self.separator = separator
        self.quote_char = quote_char
        self.storage = DiskStorage(Path("../../var/data"))

    async def process_chunks(self, storage_dir: Path) -> None:
        """Process all chunks for a data source.

        Args:
            storage_dir: Directory where chunks are stored
        """
        self.logger.info(f"Processing chunks for data source {self.data_source_id}")

        # Find all chunk files storage_dir should be a relative path to the DiskStorage root
        chunks = sorted(storage_dir.glob("*.chunk"))
        if not chunks:
            self.logger.warning(f"No chunks found in {storage_dir}")
            return

        # Combine chunks and parse
        combined_data = b""
        for chunk_file in chunks:
            try:
                # Use Path's read_bytes() instead of open()
                combined_data += chunk_file.read_bytes()
            except Exception as e:
                self.logger.error(f"Error reading chunk {chunk_file}: {str(e)}")
                continue

        # Parse the combined data
        row_count, fields = self._parse_csv(combined_data)

        # Update the data source with row count
        self.data_source.row_count = row_count

        # Create fields for the data source
        await self.db.execute(
            insert(Field),
            [
                {
                    "name": name,
                    "data_type": field_type,
                    "data_source_id": self.data_source_id,
                    "index": idx,
                    **stats,
                }
                for idx, (name, field_type, stats) in enumerate(fields)
            ],
        )

        # Commit changes
        await self.db.commit()
        self.logger.info(
            f"Processed {row_count} rows for data source {self.data_source_id}"
        )

    def _parse_csv(
        self, data: bytes
    ) -> tuple[int, list[tuple[str, str, dict[str, Any]]]]:
        """Parse CSV data and extract fields.

        Args:
            data: CSV data as bytes

        Returns:
            Tuple containing (row_count, list of field tuples)
            Each field tuple contains (name, data_type, stats_dict)
        """
        try:
            text = data.decode(self.encoding)
        except UnicodeDecodeError:
            self.logger.error(f"Error decoding CSV with encoding {self.encoding}")
            text = data.decode("utf-8", errors="replace")  # Fallback

        # Parse CSV
        reader = csv.reader(
            io.StringIO(text), delimiter=self.separator, quotechar=self.quote_char
        )

        # Extract header and rows
        header = next(reader, [])
        rows = list(reader)
        row_count = len(rows)

        # Process fields and determine data types
        fields = []
        for i, col_name in enumerate(header):
            # Extract column values
            col_values = [row[i] for row in rows if i < len(row)]

            # Determine field type and stats
            field_type, stats = self._analyze_field(col_values)

            fields.append((col_name, field_type, stats))

        return row_count, fields

    def _analyze_field(self, values: list[str]) -> tuple[FieldType, FieldStats]:
        """Analyze field values to determine type and statistics.

        Args:
            name: Field name
            values: List of field values as strings

        Returns:
            Tuple containing (field_type, statistics_dict)
        """
        # Count missing values
        missing_values = values.count("")

        # Remove empty values for analysis
        non_empty = [v for v in values if v]
        if not non_empty:
            return FieldType.nominal, FieldStats(
                missing_count=missing_values,
                unique_count=0,
                min_value=None,
                max_value=None,
                avg_value=None,
                has_nulls=missing_values > 0,
            )

        # Determine if values can be parsed as numbers
        numeric_values = []
        is_numeric = True

        for val in non_empty:
            try:
                numeric_values.append(float(val))
            except ValueError:
                is_numeric = False
                numeric_values.clear()  # The data is not numerical, we can clear the list
                break

        # Calculate statistics
        unique_count = len(set(values))
        has_nulls = missing_values > 0

        if is_numeric and len(numeric_values) > 0:
            # Field is numeric, calculate numeric stats
            min_value = str(min(numeric_values)) if numeric_values else None
            max_value = str(max(numeric_values)) if numeric_values else None
            avg_value = (
                sum(numeric_values) / len(numeric_values) if numeric_values else None
            )

            return FieldType.numeric, FieldStats(
                missing_count=missing_values,
                unique_count=unique_count,
                min_value=min_value,
                max_value=max_value,
                avg_value=avg_value,
                has_nulls=has_nulls,
            )
        else:
            # Field is string, calculate string stats
            return FieldType.nominal, FieldStats(
                missing_count=missing_values,
                unique_count=unique_count,
                min_value=min(non_empty, key=len),
                max_value=max(non_empty, key=len),
                avg_value=None,
                has_nulls=has_nulls,
            )
