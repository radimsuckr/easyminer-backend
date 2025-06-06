import logging
from collections import defaultdict
from decimal import Decimal

from sqlalchemy import exists, select
from sqlalchemy.dialects.postgresql import insert as pginsert
from sqlalchemy.orm import joinedload

from easyminer.database import get_sync_db_session
from easyminer.models import data as mdata
from easyminer.models import preprocessing as mprep
from easyminer.parsers.pmml.preprocessing import (
    Attribute,
    EquidistantIntervalsAttribute,
    EquifrequentIntervalsAttribute,
    EquisizedIntervalsAttribute,
    NominalEnumerationAttribute,
    NumericIntervalsAttribute,
    SimpleAttribute,
    TransformationDictionary,
    create_attribute_from_pmml,
)
from easyminer.schemas.data import FieldType
from easyminer.worker import app

logger = logging.getLogger(__name__)


@app.task
def create_attributes(dataset_id: int, xml: str):
    """Create dataset attributes from PMML transformation definitions.

    This task:
    1. Parses PMML XML to extract transformation definitions
    2. Creates database attribute records for each transformation
    3. Applies transformations to all data source instances
    4. Calculates frequencies and stores transformed values
    """
    if len(xml) == 0:
        raise ValueError("PMML cannot be empty")

    # Parse PMML to get transformation definitions
    pmml = TransformationDictionary.from_xml_string(xml)

    if not pmml.derived_fields:
        logger.warning("No derived fields found in PMML")
        return

    with get_sync_db_session() as db:
        # Get dataset and validate it exists
        dataset = db.get(mprep.Dataset, dataset_id, options=[joinedload(mprep.Dataset.data_source)])
        if not dataset:
            raise ValueError(f"Dataset with id {dataset_id} not found")

        # Process each derived field from PMML
        for field_def in pmml.derived_fields:
            attr_def = create_attribute_from_pmml(field_def)
            logger.info(
                f"Created attribute definition: {type(attr_def).__name__} - {attr_def.name} "
                + f"for field {attr_def.field_id}"
            )

            # Validate that the source field exists in the data source
            field_exists = db.execute(
                select(
                    exists().where(
                        mdata.Field.data_source_id == dataset.data_source_id, mdata.Field.id == attr_def.field_id
                    )
                )
            ).scalar_one()

            if not field_exists:
                raise ValueError(
                    f"Field with ID {attr_def.field_id} does not exist in data source ID {dataset.data_source_id}"
                )

            # Create database attribute record
            db_attr = mprep.Attribute(name=attr_def.name, dataset_id=dataset.id, field_id=attr_def.field_id)
            db.add(db_attr)
            db.flush()  # Get the ID

            # Get all instances for this field with field information
            instances_query = (
                select(mdata.DataSourceInstance)
                .options(joinedload(mdata.DataSourceInstance.field))
                .where(mdata.DataSourceInstance.field_id == attr_def.field_id)
            )
            instances = db.scalars(instances_query).all()

            if not instances:
                logger.warning(f"No instances found for field {attr_def.field_id}")
                continue

            logger.info(f"Processing {len(instances)} instances for attribute {attr_def.name}")

            # Transform values and count frequencies
            value_frequencies: dict[str, list[int]] = defaultdict(list)

            for instance in instances:
                # Get the raw value and prepare input for transformation based on field type
                if instance.field.data_type == FieldType.numeric:
                    # For numeric fields, convert Decimal to float for transformation
                    transform_input: float | str | None = (
                        float(instance.value_numeric) if instance.value_numeric is not None else None
                    )
                else:
                    # For nominal fields, use the string value directly
                    transform_input = instance.value_nominal

                # Apply transformation
                transformed_value = apply_transformation(attr_def, transform_input)

                # Count frequency of this transformed value
                value_frequencies[transformed_value].append(instance.row_id)

            # Store transformed values in database
            for value, tx_ids in value_frequencies.items():
                id = db.execute(
                    pginsert(mprep.DatasetValue)
                    .values(value=value, frequency=len(tx_ids), attribute_id=db_attr.id)
                    .on_conflict_do_nothing()
                    .returning(mprep.DatasetValue.id)
                ).scalar_one()
                while len(tx_ids) > 0:
                    # Create instances for this value
                    instance_tx_ids = tx_ids[:1000]
                    tx_ids = tx_ids[1000:]
                    instances_to_add = [
                        {"tx_id": tx_id, "value_id": id, "attribute_id": db_attr.id} for tx_id in instance_tx_ids
                    ]
                    _ = db.execute(pginsert(mprep.DatasetInstance).on_conflict_do_nothing(), instances_to_add)
                    db.flush()

            # Update attribute statistics
            db_attr.unique_values_size = len(value_frequencies)

            logger.info(
                f"Created attribute '{attr_def.name}' with {len(value_frequencies)} unique values "
                + f"from {len(instances)} instances"
            )

        db.commit()
        logger.info(f"Successfully created {len(pmml.derived_fields)} attributes for dataset {dataset_id}")


def apply_transformation(attr_def: Attribute, value: float | str | None) -> str:
    """Apply transformation to a value, handling None values and type conversions."""
    if value is None:
        return "None"

    try:
        transformed: str | float | None
        if isinstance(attr_def, SimpleAttribute):
            # Simple attributes pass through values unchanged
            transformed = attr_def.transform(value)
        elif isinstance(attr_def, NominalEnumerationAttribute):
            # Convert to string for nominal enumeration
            transformed = attr_def.transform(str(value))
        elif isinstance(
            attr_def,
            (
                EquidistantIntervalsAttribute,
                EquifrequentIntervalsAttribute,
                EquisizedIntervalsAttribute,
                NumericIntervalsAttribute,
            ),
        ):
            # Pyright warns about unnecessary isinstance call but it's only because the current
            # Attribute types are exhausted. When a new type is added, this will ensure it is
            # handled correctly (with the now dead else branch).
            if isinstance(value, Decimal):
                try:
                    float_value = float(value)
                    transformed = attr_def.transform(float_value)
                except (ValueError, TypeError) as e:
                    logger.warning(f"Failed to convert {value} to float: {e}")
                    transformed = None
            else:
                transformed = attr_def.transform(value)
        else:
            logger.error(f"Unknown attribute type: {type(attr_def)}")
            raise ValueError(f"Unknown attribute type: {type(attr_def)}")

        # Convert result to string, handle None
        if transformed is None:
            logger.warning(f"Transformation returned None for value {value}")
            return "None"
        else:
            return str(transformed)

    except Exception as e:
        logger.warning(f"Transformation failed for value {value} with {type(attr_def).__name__}: {e}")
        return "None"
