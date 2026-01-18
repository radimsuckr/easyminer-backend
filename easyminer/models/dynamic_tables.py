"""Dynamic table factories for per-datasource and per-dataset tables.

This module provides factory functions to create and manage dynamic tables that are
created at runtime for each data source and dataset. These tables are not managed
by Alembic migrations - they are created/dropped programmatically.

The Scala implementation uses:
- data_source_{ID} - stores instance data (field values per row)
- value_{ID} - stores unique values and frequencies for data sources
- dataset_{ID} - stores preprocessed instance data
- pp_value_{ID} - stores unique values for preprocessed datasets
"""

import logging

from sqlalchemy import Column, Double, Index, Integer, MetaData, String, Table
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

# Separate metadata for dynamic tables - not managed by Alembic
dynamic_metadata = MetaData()


def get_data_source_table_name(data_source_id: int) -> str:
    """Get the table name for a data source's instance table."""
    return f"data_source_{data_source_id}"


def get_data_source_value_table_name(data_source_id: int) -> str:
    """Get the table name for a data source's value table."""
    return f"value_{data_source_id}"


def get_dataset_table_name(dataset_id: int) -> str:
    """Get the table name for a dataset's instance table."""
    return f"dataset_{dataset_id}"


def get_dataset_value_table_name(dataset_id: int) -> str:
    """Get the table name for a dataset's value table."""
    return f"pp_value_{dataset_id}"


def get_data_source_table(data_source_id: int) -> Table:
    """Factory for data_source_{ID} table.

    This table stores instance data (cell values) for a data source.
    Each row represents a single cell value in the original data.

    Columns:
    - pid: Primary key (auto-increment)
    - id: Row ID in the original data (was row_id)
    - field: Field ID (was field_id)
    - value_nominal: String value
    - value_numeric: Numeric value (Double for MySQL compatibility)
    """
    table_name = get_data_source_table_name(data_source_id)
    return Table(
        table_name,
        dynamic_metadata,
        Column("pid", Integer, primary_key=True, autoincrement=True),
        Column("id", Integer, nullable=False),  # row_id
        Column("field", Integer, nullable=False),  # field_id
        Column("value_nominal", String(255), nullable=True),
        Column("value_numeric", Double, nullable=True),
        Index(f"ix_{table_name}_field", "field"),
        Index(f"ix_{table_name}_id_field", "id", "field"),
        mysql_engine='MyISAM',
        mysql_charset='utf8',
        mysql_collate='utf8_bin',
        extend_existing=True,
    )


def get_data_source_value_table(data_source_id: int) -> Table:
    """Factory for value_{ID} table.

    This table stores unique values and their frequencies for a data source.

    Columns:
    - id: Primary key (auto-increment)
    - field: Field ID
    - value_nominal: String value
    - value_numeric: Numeric value
    - frequency: Count of occurrences
    """
    table_name = get_data_source_value_table_name(data_source_id)
    return Table(
        table_name,
        dynamic_metadata,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("field", Integer, nullable=False),
        Column("value_nominal", String(255), nullable=True),
        Column("value_numeric", Double, nullable=True),
        Column("frequency", Integer, nullable=False),
        Index(f"ix_{table_name}_field", "field"),
        mysql_engine='MyISAM',
        mysql_charset='utf8',
        mysql_collate='utf8_bin',
        extend_existing=True,
    )


def get_dataset_table(dataset_id: int) -> Table:
    """Factory for dataset_{ID} table.

    This table stores preprocessed instance data for a dataset.

    Columns:
    - id: Primary key (auto-increment)
    - tid: Transaction ID (row number)
    - attribute: Attribute ID
    - value: Value ID (references pp_value_{ID})
    """
    table_name = get_dataset_table_name(dataset_id)
    return Table(
        table_name,
        dynamic_metadata,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("tid", Integer, nullable=False),  # transaction/row ID
        Column("attribute", Integer, nullable=False),
        Column("value", Integer, nullable=False),  # references pp_value_{ID}.id
        Index(f"ix_{table_name}_tid", "tid"),
        Index(f"ix_{table_name}_attribute", "attribute"),
        mysql_engine='MyISAM',
        mysql_charset='utf8',
        mysql_collate='utf8_bin',
        extend_existing=True,
    )


def get_dataset_value_table(dataset_id: int) -> Table:
    """Factory for pp_value_{ID} table.

    This table stores unique preprocessed values for a dataset attribute.

    Columns:
    - id: Primary key (auto-increment)
    - attribute: Attribute ID
    - value: String representation of the value
    - frequency: Count of occurrences
    """
    table_name = get_dataset_value_table_name(dataset_id)
    return Table(
        table_name,
        dynamic_metadata,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("attribute", Integer, nullable=False),
        Column("value", String(255), nullable=False),
        Column("frequency", Integer, nullable=False),
        Index(f"ix_{table_name}_attribute", "attribute"),
        mysql_engine='MyISAM',
        mysql_charset='utf8',
        mysql_collate='utf8_bin',
        extend_existing=True,
    )


def create_data_source_tables(engine: Engine, data_source_id: int) -> None:
    """Create dynamic tables for a data source.

    Creates:
    - data_source_{ID} table for instance data
    - value_{ID} table for unique values

    Uses MyISAM engine for MySQL/MariaDB (specified in table definition).
    """
    instance_table = get_data_source_table(data_source_id)
    value_table = get_data_source_value_table(data_source_id)

    with engine.begin() as conn:
        instance_table.create(conn, checkfirst=True)
        value_table.create(conn, checkfirst=True)

    logger.info(f"Created tables for data source {data_source_id}: {instance_table.name}, {value_table.name}")


def drop_data_source_tables(engine: Engine, data_source_id: int) -> None:
    """Drop dynamic tables for a data source."""
    instance_table = get_data_source_table(data_source_id)
    value_table = get_data_source_value_table(data_source_id)

    with engine.begin() as conn:
        value_table.drop(conn, checkfirst=True)
        instance_table.drop(conn, checkfirst=True)

    # Remove from metadata cache
    if instance_table.name in dynamic_metadata.tables:
        dynamic_metadata.remove(instance_table)
    if value_table.name in dynamic_metadata.tables:
        dynamic_metadata.remove(value_table)

    logger.info(f"Dropped tables for data source {data_source_id}")


def create_dataset_tables(engine: Engine, dataset_id: int) -> None:
    """Create dynamic tables for a dataset.

    Creates:
    - dataset_{ID} table for preprocessed instance data
    - pp_value_{ID} table for unique preprocessed values

    Uses MyISAM engine for MySQL/MariaDB (specified in table definition).
    """
    instance_table = get_dataset_table(dataset_id)
    value_table = get_dataset_value_table(dataset_id)

    with engine.begin() as conn:
        value_table.create(conn, checkfirst=True)
        instance_table.create(conn, checkfirst=True)

    logger.info(f"Created tables for dataset {dataset_id}: {instance_table.name}, {value_table.name}")


def drop_dataset_tables(engine: Engine, dataset_id: int) -> None:
    """Drop dynamic tables for a dataset."""
    instance_table = get_dataset_table(dataset_id)
    value_table = get_dataset_value_table(dataset_id)

    with engine.begin() as conn:
        instance_table.drop(conn, checkfirst=True)
        value_table.drop(conn, checkfirst=True)

    # Remove from metadata cache
    if instance_table.name in dynamic_metadata.tables:
        dynamic_metadata.remove(instance_table)
    if value_table.name in dynamic_metadata.tables:
        dynamic_metadata.remove(value_table)

    logger.info(f"Dropped tables for dataset {dataset_id}")
