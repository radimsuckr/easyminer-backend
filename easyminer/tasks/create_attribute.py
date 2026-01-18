import logging
from collections import defaultdict
from decimal import Decimal

from sqlalchemy import exists, func, insert, select
from sqlalchemy.orm import joinedload

from easyminer.database import get_sync_db_session
from easyminer.models import data as mdata
from easyminer.models import preprocessing as mprep
from easyminer.models.dynamic_tables import (
    get_data_source_table,
    get_dataset_table,
    get_dataset_value_table,
)
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
def create_attributes(dataset_id: int, xml: str, db_url: str):
    """Create dataset attributes from PMML transformation definitions.

    This task:
    1. Parses PMML XML to extract transformation definitions
    2. Creates database attribute records for each transformation
    3. Applies transformations to all data source instances
    4. Calculates frequencies and stores transformed values
    """
    if len(xml) == 0:
        raise ValueError("PMML cannot be empty")

    pmml = TransformationDictionary.from_xml_string(xml)

    if not pmml.derived_fields:
        logger.warning("No derived fields found in PMML")
        return

    with get_sync_db_session(db_url) as db:
        dataset = db.get(mprep.Dataset, dataset_id, options=[joinedload(mprep.Dataset.data_source_rel)])
        if not dataset:
            raise ValueError(f"Dataset with id {dataset_id} not found")

        data_source_id = dataset.data_source

        # Get dynamic tables
        source_table = get_data_source_table(data_source_id)
        dataset_instance_table = get_dataset_table(dataset_id)
        dataset_value_table = get_dataset_value_table(dataset_id)

        # Get next attribute ID since composite PK doesn't support autoincrement
        max_attr_id = db.execute(
            select(func.coalesce(func.max(mprep.Attribute.id), 0)).where(mprep.Attribute.dataset == dataset_id)
        ).scalar_one()
        next_attr_id = max_attr_id + 1

        for field_def in pmml.derived_fields:
            attr_def = create_attribute_from_pmml(field_def)
            logger.info(
                f"Created attribute definition: {type(attr_def).__name__} - {attr_def.name} "
                + f"for field {attr_def.field_id}"
            )

            field_exists = db.execute(
                select(
                    exists().where(
                        mdata.Field.data_source == data_source_id,
                        mdata.Field.id == attr_def.field_id,
                    )
                )
            ).scalar_one()

            if not field_exists:
                raise ValueError(f"Field with ID {attr_def.field_id} does not exist in data source ID {data_source_id}")

            # Create attribute record with renamed columns and manually assigned ID
            db_attr = mprep.Attribute(id=next_attr_id, name=attr_def.name, dataset=dataset.id, field=attr_def.field_id)
            db.add(db_attr)
            db.flush()
            next_attr_id += 1

            # Query data from dynamic source table (using composite PK)
            field = db.execute(
                select(mdata.Field).where(
                    mdata.Field.id == attr_def.field_id, mdata.Field.data_source == data_source_id
                )
            ).scalar_one_or_none()
            if not field:
                raise ValueError(f"Field with ID {attr_def.field_id} not found in data source {data_source_id}")
            instances_query = select(
                source_table.c.id.label("row_id"),
                source_table.c.value_nominal,
                source_table.c.value_numeric,
            ).where(source_table.c.field == attr_def.field_id)

            instances = db.execute(instances_query).all()

            if not instances:
                logger.warning(f"No instances found for field {attr_def.field_id}")
                continue

            logger.info(f"Processing {len(instances)} instances for attribute {attr_def.name}")

            value_frequencies: dict[str, list[int]] = defaultdict(list)

            for instance in instances:
                if field.data_type == FieldType.numeric:
                    transform_input: float | str | None = (
                        float(instance.value_numeric) if instance.value_numeric is not None else None
                    )
                else:
                    transform_input = instance.value_nominal

                transformed_value = apply_transformation(attr_def, transform_input)

                value_frequencies[transformed_value].append(instance.row_id)

            for value, tx_ids in value_frequencies.items():
                # Insert into dynamic value table (pp_value_{ID})
                value_id = db.execute(
                    insert(dataset_value_table)
                    .values(value=value, frequency=len(tx_ids), attribute=db_attr.id)
                    .returning(dataset_value_table.c.id)
                ).scalar_one()

                # Insert into dynamic instance table (dataset_{ID})
                while len(tx_ids) > 0:
                    instance_tx_ids = tx_ids[:1000]
                    tx_ids = tx_ids[1000:]
                    instances_to_add = [
                        {"tid": tx_id, "value": value_id, "attribute": db_attr.id} for tx_id in instance_tx_ids
                    ]
                    _ = db.execute(insert(dataset_instance_table), instances_to_add)
                    db.flush()

            db_attr.unique_values_size = len(value_frequencies)
            db_attr.active = True

            logger.info(
                f"Created attribute '{attr_def.name}' with {len(value_frequencies)} unique values "
                + f"from {len(instances)} instances"
            )

        db.commit()
        logger.info(f"Successfully created {len(pmml.derived_fields)} attributes for dataset {dataset_id}")


def apply_transformation(attr_def: Attribute, value: float | str | None) -> str:
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
                EquidistantIntervalsAttribute
                | EquifrequentIntervalsAttribute
                | EquisizedIntervalsAttribute
                | NumericIntervalsAttribute
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

        if transformed is None:
            logger.warning(f"Transformation returned None for value {value}")
            return "None"
        else:
            return str(transformed)

    except Exception as e:
        logger.warning(f"Transformation failed for value {value} with {type(attr_def).__name__}: {e}")
        return "None"
