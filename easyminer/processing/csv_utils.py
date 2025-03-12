"""Utilities for processing CSV data."""

from typing import Any

from easyminer.schemas.field_values import Value


def extract_field_values_from_csv(
    csv_text: str, field, encoding="utf-8", separator=",", quote_char='"'
) -> list[Value]:
    """Extract unique field values and their frequencies from CSV content.

    Args:
        csv_text: The CSV content as a string
        field: Field object with index and data_type properties
        encoding: The encoding of the CSV text
        separator: The column separator character
        quote_char: The quote character for string values

    Returns:
        List of Value objects with unique values and their frequencies,
        sorted by frequency in descending order
    """
    value_counts: dict[Any, int] = {}

    # Process the CSV data
    lines = csv_text.splitlines()
    if not lines:
        return []

    # Skip header
    lines = lines[1:]

    # Extract values for the field
    for line in lines:
        if not line.strip():
            continue

        parts = line.split(separator)
        if field.index < len(parts):
            # Get the field value, handle quotes
            val = parts[field.index].strip()
            if val.startswith(quote_char) and val.endswith(quote_char):
                val = val[1:-1]

            # Process the value
            field_val: Any = None
            if not val:
                field_val = None
            elif field.data_type in ["integer", "float"] and val:
                try:
                    field_val = float(val) if field.data_type == "float" else int(val)
                except (ValueError, TypeError):
                    field_val = val
            else:
                field_val = val

            # Update counts
            if field_val in value_counts:
                value_counts[field_val] += 1
            else:
                value_counts[field_val] = 1

    # Create result objects with frequencies
    result = []
    for i, (value, frequency) in enumerate(
        sorted(value_counts.items(), key=lambda x: x[1], reverse=True)
    ):
        result.append(Value(id=i, value=value, frequency=frequency))

    return result
