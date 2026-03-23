import logging
from collections import defaultdict
from decimal import Decimal

from sqlalchemy import and_, case, func, insert, literal, select
from sqlalchemy.orm import joinedload

from easyminer.database import get_sync_db_session
from easyminer.models import data as mdata
from easyminer.models import preprocessing as mprep
from easyminer.models.dynamic_tables import (
    get_data_source_table,
    get_data_source_value_table,
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
from easyminer.preprocessing.smoothing import (
    IntervalBorder,
    ValueFrequency,
    format_interval,
)
from easyminer.schemas.data import FieldType
from easyminer.schemas.preprocessing import AttributeResult
from easyminer.worker import app

logger = logging.getLogger(__name__)

MAX_BINS = 1000
MAX_NAME_LENGTH = 255
MIN_SUPPORT = 1.0 / 1000


def validate_attribute(attr_def: Attribute, field: mdata.Field) -> None:
    """Validate attribute definition against field and constraints."""
    if not attr_def.name or len(attr_def.name) > MAX_NAME_LENGTH:
        raise ValueError(f"Attribute name must be non-empty and at most {MAX_NAME_LENGTH} characters")

    if isinstance(attr_def, EquidistantIntervalsAttribute):
        if not (1 <= attr_def.bins <= MAX_BINS):
            raise ValueError(f"bins must be between 1 and {MAX_BINS}, got {attr_def.bins}")
        if field.data_type != FieldType.numeric:
            raise ValueError(f"Equidistant intervals require numeric field, got {field.data_type}")

    elif isinstance(attr_def, EquifrequentIntervalsAttribute):
        if not (1 <= attr_def.bins <= MAX_BINS):
            raise ValueError(f"bins must be between 1 and {MAX_BINS}, got {attr_def.bins}")
        if field.data_type != FieldType.numeric:
            raise ValueError(f"Equifrequent intervals require numeric field, got {field.data_type}")

    elif isinstance(attr_def, EquisizedIntervalsAttribute):
        if not (MIN_SUPPORT < attr_def.support <= 1.0):
            raise ValueError(f"support must be between {MIN_SUPPORT} and 1.0, got {attr_def.support}")
        if field.data_type != FieldType.numeric:
            raise ValueError(f"Equisized intervals require numeric field, got {field.data_type}")

    elif isinstance(attr_def, NominalEnumerationAttribute):
        if not attr_def.bins:
            raise ValueError("Nominal enumeration must have at least one bin")
        if len(attr_def.bins) > MAX_BINS:
            raise ValueError(f"Number of bins must be at most {MAX_BINS}")

    elif isinstance(attr_def, NumericIntervalsAttribute):
        if not attr_def.bins:
            raise ValueError("Numeric intervals must have at least one bin")
        if len(attr_def.bins) > MAX_BINS:
            raise ValueError(f"Number of bins must be at most {MAX_BINS}")


@app.task(pydantic=True)
def create_attributes(dataset_id: int, xml: str, db_url: str) -> list[AttributeResult]:
    """Create dataset attributes from PMML transformation definitions.

    Uses SQL-driven approach: queries aggregated value tables instead of
    iterating individual instances. Builds intervals for equifrequent/equisized
    from value frequencies, then uses INSERT...SELECT with CASE expressions.
    """
    if len(xml) == 0:
        raise ValueError("PMML cannot be empty")

    pmml = TransformationDictionary.from_xml_string(xml)

    if not pmml.derived_fields:
        logger.warning("No derived fields found in PMML")
        return []

    created_attributes: list[AttributeResult] = []

    with get_sync_db_session(db_url) as db:
        dataset = db.get(mprep.Dataset, dataset_id, options=[joinedload(mprep.Dataset.data_source_rel)])
        if not dataset:
            raise ValueError(f"Dataset with id {dataset_id} not found")

        data_source_id = dataset.data_source

        # Get dynamic tables
        source_table = get_data_source_table(data_source_id)
        value_table = get_data_source_value_table(data_source_id)
        dataset_instance_table = get_dataset_table(dataset_id)
        dataset_value_table = get_dataset_value_table(dataset_id)

        # Parse all attribute definitions and validate
        attr_defs: list[tuple[Attribute, mdata.Field]] = []
        for field_def in pmml.derived_fields:
            attr_def = create_attribute_from_pmml(field_def)

            field = db.execute(
                select(mdata.Field)
                .where(mdata.Field.id == attr_def.field_id, mdata.Field.data_source == data_source_id)
                .options(joinedload(mdata.Field.numeric_detail))
            ).scalar_one_or_none()
            if not field:
                raise ValueError(f"Field with ID {attr_def.field_id} not found in data source {data_source_id}")

            validate_attribute(attr_def, field)
            attr_defs.append((attr_def, field))

        # Get next attribute ID since composite PK doesn't support autoincrement
        max_attr_id = db.execute(
            select(func.coalesce(func.max(mprep.Attribute.id), 0)).where(mprep.Attribute.dataset == dataset_id)
        ).scalar_one()
        next_attr_id = max_attr_id + 1

        for attr_def, field in attr_defs:
            logger.info(
                f"Building attribute: {type(attr_def).__name__} - {attr_def.name} " + f"for field {attr_def.field_id}"
            )

            # Create attribute record
            db_attr = mprep.Attribute(id=next_attr_id, name=attr_def.name, dataset=dataset.id, field=attr_def.field_id)
            db.add(db_attr)
            db.flush()
            attr_id = next_attr_id
            next_attr_id += 1

            if isinstance(attr_def, (EquifrequentIntervalsAttribute, EquisizedIntervalsAttribute)):
                unique_values = _build_frequency_based_attribute(
                    db,
                    attr_def,
                    field,
                    attr_id,
                    dataset,
                    value_table,
                    source_table,
                    dataset_value_table,
                    dataset_instance_table,
                )
            elif isinstance(attr_def, EquidistantIntervalsAttribute):
                unique_values = _build_equidistant_attribute(
                    db,
                    attr_def,
                    field,
                    attr_id,
                    value_table,
                    source_table,
                    dataset_value_table,
                    dataset_instance_table,
                )
            else:
                unique_values = _build_value_mapped_attribute(
                    db,
                    attr_def,
                    field,
                    attr_id,
                    value_table,
                    source_table,
                    dataset_value_table,
                    dataset_instance_table,
                )

            db_attr.unique_values_size = unique_values
            db_attr.active = True

            logger.info(f"Created attribute '{attr_def.name}' with {unique_values} unique values")

            created_attributes.append(
                AttributeResult(
                    id=db_attr.id,
                    dataset=db_attr.dataset,
                    field=db_attr.field,
                    name=db_attr.name,
                    unique_values_size=db_attr.unique_values_size,
                )
            )

        db.commit()
        logger.info(f"Successfully created {len(attr_defs)} attributes for dataset {dataset_id}")

        return created_attributes


def _build_frequency_based_attribute(
    db, attr_def, field, attr_id, dataset, value_table, source_table, dataset_value_table, dataset_instance_table
) -> int:
    """Build equifrequent or equisized attribute using value frequencies + smoothing."""
    # Query sorted (value_numeric, frequency) from the value table
    rows = db.execute(
        select(value_table.c.value_numeric, value_table.c.frequency)
        .where(value_table.c.field == field.id, value_table.c.value_numeric.isnot(None))
        .order_by(value_table.c.value_numeric)
    ).all()

    if not rows:
        logger.warning(f"No numeric values for field {field.id}")
        return 0

    values = [ValueFrequency(value=float(row.value_numeric), frequency=row.frequency) for row in rows]
    dataset_size = dataset.size

    if isinstance(attr_def, EquifrequentIntervalsAttribute):
        attr_def = EquifrequentIntervalsAttribute.build(
            name=attr_def.name,
            field_id=attr_def.field_id,
            bins_count=attr_def.bins,
            values=values,
            unique_values_count=len(values),
            dataset_size=dataset_size,
        )
    else:
        attr_def = EquisizedIntervalsAttribute.build(
            name=attr_def.name,
            field_id=attr_def.field_id,
            support=attr_def.support,
            values=values,
            dataset_size=dataset_size,
        )

    assert attr_def.intervals is not None

    return _insert_intervals_and_instances(
        db,
        attr_def.intervals,
        attr_id,
        field.id,
        source_table,
        dataset_value_table,
        dataset_instance_table,
    )


def _build_equidistant_attribute(
    db, attr_def, field, attr_id, value_table, source_table, dataset_value_table, dataset_instance_table
) -> int:
    """Build equidistant attribute using FieldNumericDetail for authoritative min/max."""
    from easyminer.preprocessing.smoothing import AttributeInterval

    # Use FieldNumericDetail for authoritative min/max if available
    if field.numeric_detail:
        min_val = float(field.numeric_detail.min_value)
        max_val = float(field.numeric_detail.max_value)
    else:
        min_val = attr_def.min_value
        max_val = attr_def.max_value

    width = (max_val - min_val) / attr_def.bins

    # Compute intervals and aggregate frequencies from value table
    intervals = []
    for i in range(attr_def.bins):
        lower = min_val + i * width
        upper = lower + width
        intervals.append(
            AttributeInterval(
                from_border=IntervalBorder(lower, inclusive=True),
                to_border=IntervalBorder(upper, inclusive=False),
                frequency=0,
            )
        )

    # Query value frequencies and assign to intervals
    rows = db.execute(
        select(value_table.c.value_numeric, value_table.c.frequency).where(
            value_table.c.field == field.id, value_table.c.value_numeric.isnot(None)
        )
    ).all()

    for row in rows:
        val = float(row.value_numeric)
        for j, iv in enumerate(intervals):
            if iv.from_border.value <= val < iv.to_border.value or (
                j == len(intervals) - 1 and val == iv.to_border.value
            ):
                intervals[j] = AttributeInterval(
                    from_border=iv.from_border,
                    to_border=iv.to_border,
                    frequency=iv.frequency + row.frequency,
                )
                break

    return _insert_intervals_and_instances(
        db,
        intervals,
        attr_id,
        field.id,
        source_table,
        dataset_value_table,
        dataset_instance_table,
    )


def _insert_intervals_and_instances(
    db, intervals, attr_id, field_id, source_table, dataset_value_table, dataset_instance_table
) -> int:
    """Bulk insert interval values and populate dataset via INSERT...SELECT with CASE."""
    if not intervals:
        return 0

    # Bulk insert pp_values and collect their IDs
    pp_value_ids = []
    for iv in intervals:
        label = format_interval(iv.from_border, iv.to_border)
        value_id = db.execute(
            insert(dataset_value_table)
            .values(value=label, frequency=iv.frequency, attribute=attr_id)
            .returning(dataset_value_table.c.id)
        ).scalar_one()
        pp_value_ids.append(value_id)

    # Build CASE expression mapping numeric values to pp_value IDs
    case_whens = []
    for j, iv in enumerate(intervals):
        if iv.from_border.inclusive:
            from_cond = source_table.c.value_numeric >= iv.from_border.value
        else:
            from_cond = source_table.c.value_numeric > iv.from_border.value
        if iv.to_border.inclusive:
            to_cond = source_table.c.value_numeric <= iv.to_border.value
        else:
            to_cond = source_table.c.value_numeric < iv.to_border.value
        case_whens.append((and_(from_cond, to_cond), pp_value_ids[j]))

    value_expr = case(*case_whens)

    # INSERT...SELECT: populate dataset instances from source
    insert_select = insert(dataset_instance_table).from_select(
        ["tid", "attribute", "value"],
        select(
            source_table.c.id,
            literal(attr_id),
            value_expr,
        ).where(
            source_table.c.field == field_id,
            source_table.c.value_numeric.isnot(None),
        ),
    )
    db.execute(insert_select)

    return len(intervals)


def _build_value_mapped_attribute(
    db, attr_def, field, attr_id, value_table, source_table, dataset_value_table, dataset_instance_table
) -> int:
    """Build Simple, NominalEnumeration, or NumericIntervals attribute using value table."""
    is_numeric = field.data_type == FieldType.numeric
    value_col = value_table.c.value_numeric if is_numeric else value_table.c.value_nominal

    # Query aggregated values from the value table
    rows = db.execute(select(value_col, value_table.c.frequency).where(value_table.c.field == field.id)).all()

    if not rows:
        logger.warning(f"No values for field {field.id}")
        return 0

    # Transform each unique value and aggregate frequencies for same-output values
    transformed_freqs: dict[str, int] = defaultdict(int)
    # Map: original_value -> transformed_value (for building CASE expressions)
    value_mapping: dict[str | float | None, str] = {}

    for row in rows:
        original = float(row[0]) if is_numeric and row[0] is not None else row[0]
        transformed = apply_transformation(attr_def, original)
        transformed_freqs[transformed] += row.frequency
        value_mapping[original] = transformed

    # Insert pp_values and collect IDs
    pp_value_map: dict[str, int] = {}
    for value, freq in transformed_freqs.items():
        value_id = db.execute(
            insert(dataset_value_table)
            .values(value=value, frequency=freq, attribute=attr_id)
            .returning(dataset_value_table.c.id)
        ).scalar_one()
        pp_value_map[value] = value_id

    # Build CASE expression: source value -> pp_value_id
    source_value_col = source_table.c.value_numeric if is_numeric else source_table.c.value_nominal
    case_whens = []
    for original, transformed in value_mapping.items():
        if original is None:
            cond = source_value_col.is_(None)
        else:
            cond = source_value_col == original
        case_whens.append((cond, pp_value_map[transformed]))

    if not case_whens:
        return len(transformed_freqs)

    value_expr = case(*case_whens)

    # INSERT...SELECT
    insert_select = insert(dataset_instance_table).from_select(
        ["tid", "attribute", "value"],
        select(
            source_table.c.id,
            literal(attr_id),
            value_expr,
        ).where(source_table.c.field == field.id),
    )
    db.execute(insert_select)

    return len(transformed_freqs)


def apply_transformation(attr_def: Attribute, value: float | str | None) -> str:
    if value is None:
        return "None"

    try:
        transformed: str | float | None
        if isinstance(attr_def, SimpleAttribute):
            transformed = attr_def.transform(value)
        elif isinstance(attr_def, NominalEnumerationAttribute):
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
