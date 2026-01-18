"""Initial schema - Scala-compatible dynamic tables.

Revision ID: 001_init
Revises:
Create Date: 2025-01-18

This migration creates the metadata tables for the Scala-compatible schema.
Dynamic tables (data_source_{ID}, value_{ID}, dataset_{ID}, pp_value_{ID})
are created at runtime via easyminer.models.dynamic_tables module.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "001_init"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # Upload workflow tables
    op.create_table(
        "preview_upload",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("uuid", sa.UUID(), nullable=False),
        sa.Column("max_lines", sa.Integer(), nullable=False),
        sa.Column("compression", sa.Enum("zip", "gzip", "bzip2", "none", name="compressiontype"), nullable=False),
        sa.Column("media_type", sa.Enum("csv", name="mediatype"), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("uuid"),
    )

    op.create_table(
        "task_result",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("value", sa.JSON(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )

    op.create_table(
        "upload",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("uuid", sa.UUID(), nullable=False),
        sa.Column("name", sa.String(length=100), nullable=False),
        sa.Column("media_type", sa.Enum("csv", name="mediatype"), nullable=False),
        sa.Column("db_type", sa.Enum("limited", name="dbtype"), nullable=False),
        sa.Column("separator", sa.String(length=1), nullable=False),
        sa.Column("encoding", sa.String(length=40), nullable=False),
        sa.Column("quotes_char", sa.String(length=1), nullable=False),
        sa.Column("escape_char", sa.String(length=1), nullable=False),
        sa.Column("locale", sa.Enum("en", name="locale"), nullable=False),
        sa.Column("state", sa.Enum("initialized", "locked", "ready", "finished", name="uploadstate"), nullable=False),
        sa.Column("last_change_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )

    op.create_table(
        "chunk",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("uploaded_at", sa.DateTime(), nullable=False),
        sa.Column("path", sa.String(length=255), nullable=False),
        sa.Column("upload_id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(["upload_id"], ["upload.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_chunk_id"), "chunk", ["id"], unique=False)

    op.create_table(
        "null_value",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("value", sa.String(length=255), nullable=False),
        sa.Column("upload_id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(["upload_id"], ["upload.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )

    op.create_table(
        "data_type",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("index", sa.Integer(), nullable=False),
        sa.Column("value", sa.Enum("nominal", "numeric", name="fieldtype"), nullable=True),
        sa.Column("upload_id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(["upload_id"], ["upload.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )

    # Data source table (no created_at/updated_at, has active boolean)
    op.create_table(
        "data_source",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("type", sa.Enum("limited", name="dbtype"), nullable=False),
        sa.Column("size", sa.Integer(), nullable=False),
        sa.Column("active", sa.Boolean(), nullable=False),
        sa.Column("upload_id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(["upload_id"], ["upload.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_data_source_id"), "data_source", ["id"], unique=False)

    # Field table with composite primary key (id, data_source)
    # Note: no autoincrement because SQLite doesn't support it with composite keys
    op.create_table(
        "field",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("data_type", sa.Enum("nominal", "numeric", name="fieldtype"), nullable=False),
        sa.Column("index", sa.Integer(), nullable=False),
        sa.Column("unique_values_size_nominal", sa.Integer(), nullable=False),
        sa.Column("unique_values_size_numeric", sa.Integer(), nullable=False),
        sa.Column("support_nominal", sa.Integer(), nullable=False),
        sa.Column("support_numeric", sa.Integer(), nullable=False),
        sa.Column("data_source", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(["data_source"], ["data_source.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id", "data_source"),
    )

    op.create_table(
        "field_numeric_detail",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("min_value", sa.DECIMAL(), nullable=False),
        sa.Column("max_value", sa.DECIMAL(), nullable=False),
        sa.Column("avg_value", sa.DECIMAL(), nullable=False),
        sa.ForeignKeyConstraint(["id"], ["field.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_field_numeric_detail_id"), "field_numeric_detail", ["id"], unique=False)

    # Dataset table (renamed is_active to active, data_source_id to data_source)
    op.create_table(
        "dataset",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("type", sa.Enum("limited", name="dbtype"), nullable=False),
        sa.Column("size", sa.Integer(), nullable=False),
        sa.Column("active", sa.Boolean(), nullable=False),
        sa.Column("data_source", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(["data_source"], ["data_source.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_dataset_id"), "dataset", ["id"], unique=False)

    # Attribute table (renamed from dataset_attribute, composite PK)
    # Note: no autoincrement because SQLite doesn't support it with composite keys
    op.create_table(
        "attribute",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("unique_values_size", sa.Integer(), nullable=False),
        sa.Column("active", sa.Boolean(), nullable=False),
        sa.Column("dataset", sa.Integer(), nullable=False),
        sa.Column("field", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(["dataset"], ["dataset.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["field"], ["field.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id", "dataset"),
    )

    # Task tracking tables
    op.create_table(
        "task",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("task_id", sa.Uuid(), nullable=False),
        sa.Column("name", sa.String(length=100), nullable=False),
        sa.Column(
            "status",
            sa.Enum("pending", "scheduled", "started", "success", "failure", name="taskstatusenum"),
            nullable=False,
        ),
        sa.Column("status_message", sa.String(length=255), nullable=True),
        sa.Column("data_source_id", sa.Integer(), nullable=True),
        sa.Column("result_id", sa.Integer(), nullable=True),
        sa.ForeignKeyConstraint(["data_source_id"], ["data_source.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["result_id"], ["task_result.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_task_task_id"), "task", ["task_id"], unique=True)


def downgrade() -> None:
    op.drop_index(op.f("ix_task_task_id"), table_name="task")
    op.drop_table("task")
    op.drop_table("attribute")
    op.drop_index(op.f("ix_dataset_id"), table_name="dataset")
    op.drop_table("dataset")
    op.drop_index(op.f("ix_field_numeric_detail_id"), table_name="field_numeric_detail")
    op.drop_table("field_numeric_detail")
    op.drop_table("field")
    op.drop_index(op.f("ix_data_source_id"), table_name="data_source")
    op.drop_table("data_source")
    op.drop_table("data_type")
    op.drop_table("null_value")
    op.drop_index(op.f("ix_chunk_id"), table_name="chunk")
    op.drop_table("chunk")
    op.drop_table("upload")
    op.drop_table("task_result")
    op.drop_table("preview_upload")
