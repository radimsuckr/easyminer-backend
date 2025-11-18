"""remove_leftmost_prefix_redundant_index

Revision ID: 5e6b96d43f33
Revises: 465974147430
Create Date: 2025-11-18 12:30:00.000000

This migration removes the redundant single-column index on data_source_id
from data_source_instance table, as it's covered by the leftmost prefix
of the composite index ix_data_source_instance_ds_row (data_source_id, row_id).

In MySQL/MariaDB, a composite index can be used for queries filtering by
just the leftmost column(s), making the separate single-column index redundant.
"""

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "5e6b96d43f33"
down_revision: str | None = "465974147430"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Remove redundant single-column index covered by composite index."""
    # Drop the redundant data_source_id index
    # The composite index ix_data_source_instance_ds_row (data_source_id, row_id)
    # can be used for queries filtering by just data_source_id
    op.drop_index("ix_data_source_instance_data_source_id", table_name="data_source_instance")


def downgrade() -> None:
    """Recreate the single-column index."""
    op.create_index("ix_data_source_instance_data_source_id", "data_source_instance", ["data_source_id"])
