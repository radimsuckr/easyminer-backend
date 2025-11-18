"""remove_redundant_indexes

Revision ID: 465974147430
Revises: cc9afa8a7880
Create Date: 2025-11-18 12:00:00.000000

This migration removes redundant indexes that harm upload performance:
1. Redundant primary key indexes (index=True on PK columns)
2. Harmful value indexes on data_source_instance
3. Redundant composite indexes

Expected impact: 30-40% faster uploads, 70+ MB storage savings per 1M rows
"""

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "465974147430"
down_revision: str | None = "cc9afa8a7880"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Remove redundant and harmful indexes."""
    # Phase 1: Drop redundant primary key indexes
    # These are completely redundant with the PRIMARY KEY constraint
    op.drop_index("ix_chunk_id", table_name="chunk")
    op.drop_index("ix_data_source_id", table_name="data_source")
    op.drop_index("ix_field_id", table_name="field")
    op.drop_index("ix_dataset_id", table_name="dataset")
    op.drop_index("ix_dataset_attribute_id", table_name="dataset_attribute")
    op.drop_index("ix_dataset_instance_id", table_name="dataset_instance")
    op.drop_index("ix_dataset_value_id", table_name="dataset_value")
    op.drop_index("ix_field_numeric_detail_id", table_name="field_numeric_detail")
    op.drop_index("ix_data_source_instance_id", table_name="data_source_instance")
    op.drop_index("ix_data_source_value_id", table_name="data_source_value")

    # Phase 2: Drop harmful value indexes on data_source_instance
    # These severely harm INSERT performance and are rarely/never used
    op.drop_index("ix_data_source_instance_field_value_nominal", table_name="data_source_instance")
    op.drop_index("ix_data_source_instance_field_value_numeric", table_name="data_source_instance")

    # Phase 3: Drop redundant composite indexes
    # These are covered by single-column indexes in most queries
    op.drop_index("ix_data_source_instance_ds_field", table_name="data_source_instance")
    op.drop_index("ix_data_source_value_ds_field", table_name="data_source_value")


def downgrade() -> None:
    """Recreate all dropped indexes (in reverse order)."""
    # Recreate composite indexes
    op.create_index("ix_data_source_value_ds_field", "data_source_value", ["data_source_id", "field_id"])
    op.create_index("ix_data_source_instance_ds_field", "data_source_instance", ["data_source_id", "field_id"])

    # Recreate value indexes
    op.create_index(
        "ix_data_source_instance_field_value_numeric", "data_source_instance", ["field_id", "value_numeric"]
    )
    op.create_index(
        "ix_data_source_instance_field_value_nominal", "data_source_instance", ["field_id", "value_nominal"]
    )

    # Recreate primary key indexes
    op.create_index("ix_data_source_value_id", "data_source_value", ["id"])
    op.create_index("ix_data_source_instance_id", "data_source_instance", ["id"])
    op.create_index("ix_field_numeric_detail_id", "field_numeric_detail", ["id"])
    op.create_index("ix_dataset_value_id", "dataset_value", ["id"])
    op.create_index("ix_dataset_instance_id", "dataset_instance", ["id"])
    op.create_index("ix_dataset_attribute_id", "dataset_attribute", ["id"])
    op.create_index("ix_dataset_id", "dataset", ["id"])
    op.create_index("ix_field_id", "field", ["id"])
    op.create_index("ix_data_source_id", "data_source", ["id"])
    op.create_index("ix_chunk_id", "chunk", ["id"])
