import csv
import io
import logging
from pathlib import Path
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from easyminer.models.data import DataSource, Field


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

    async def process_chunks(self, storage_dir: Path) -> None:
        """Process all chunks for a data source.

        Args:
            storage_dir: Directory where chunks are stored
        """
        self.logger.info(f"Processing chunks for data source {self.data_source_id}")

        # Find all chunk files
        chunks = sorted(storage_dir.glob("*.chunk"))
        if not chunks:
            self.logger.warning(f"No chunks found in {storage_dir}")
            return

        # Combine chunks and parse
        combined_data = b""
        for chunk_file in chunks:
            with open(chunk_file, "rb") as f:
                combined_data += f.read()

        # Parse the combined data
        row_count, fields = self._parse_csv(combined_data)

        # Update the data source with row count
        self.data_source.row_count = row_count

        # Create fields for the data source
        for idx, (name, field_type, stats) in enumerate(fields):
            field = Field(
                name=name,
                data_type=field_type,
                data_source_id=self.data_source_id,
                index=idx,
                **stats,
            )
            self.db.add(field)

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
            field_type, stats = self._analyze_field(col_name, col_values)

            fields.append((col_name, field_type, stats))

        return row_count, fields

    def _analyze_field(
        self, name: str, values: list[str]
    ) -> tuple[str, dict[str, Any]]:
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
            return "string", {
                "missing_values_count": missing_values,
                "unique_values_count": 0,
            }

        # Determine if values can be parsed as numbers
        numeric_values = []
        is_numeric = True

        for val in non_empty:
            try:
                numeric_values.append(float(val))
            except ValueError:
                is_numeric = False
                break

        # Calculate statistics
        unique_values_count = len(set(values))

        if is_numeric:
            # Field is numeric, calculate numeric stats
            field_type = "float" if any("." in val for val in non_empty) else "integer"
            min_value = str(min(numeric_values)) if numeric_values else None
            max_value = str(max(numeric_values)) if numeric_values else None
            avg_value = (
                sum(numeric_values) / len(numeric_values) if numeric_values else None
            )

            # Calculate standard deviation
            std_value = None
            if len(numeric_values) > 1 and avg_value is not None:
                mean = avg_value
                variance = sum((x - mean) ** 2 for x in numeric_values) / len(
                    numeric_values
                )
                std_value = variance**0.5

            return field_type, {
                "missing_values_count": missing_values,
                "unique_values_count": unique_values_count,
                "min_value": min_value,
                "max_value": max_value,
                "avg_value": avg_value,
                "std_value": std_value,
            }
        else:
            # Field is string, calculate string stats
            return "string", {
                "missing_values_count": missing_values,
                "unique_values_count": unique_values_count,
                "min_value": min(non_empty, key=len) if non_empty else None,
                "max_value": max(non_empty, key=len) if non_empty else None,
            }
