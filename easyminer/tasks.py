import csv
import logging
from collections.abc import Sequence

import pandas
import pydantic as pd
from sqlalchemy import func, insert, select, update
from sqlalchemy.orm import Session

from easyminer.database import get_sync_db_session
from easyminer.models import DataSource, Field, FieldNumericDetail, FieldType, Instance
from easyminer.models.dataset import Dataset
from easyminer.models.upload import Chunk
from easyminer.parser import (
    Attribute,
    EquidistantIntervalsAttribute,
    EquifrequentIntervalsAttribute,
    EquisizedIntervalsAttribute,
    NominalEnumerationAttribute,
    NumericIntervalsAttribute,
    PmmlTaskParser,
    SimpleAttribute,
)
from easyminer.schemas import BaseSchema
from easyminer.schemas.data import MediaType, UploadResponseSchema
from easyminer.schemas.preprocessing import DatasetSchema
from easyminer.worker import app


def _create_fields(
    data_source_id: int,
    field_names: list[str],
) -> Sequence[Field]:
    logger = logging.getLogger(__name__)

    fields: list[Field] = []
    chunk_size = 1000
    with get_sync_db_session() as session:
        for ix in range(0, len(field_names), chunk_size):
            chunk = field_names[ix : ix + chunk_size]

            # Check if fields already exist
            existing_names_rows = (
                session.execute(
                    select(Field.name).filter(Field.data_source_id == data_source_id, Field.name.in_(chunk))
                )
                .scalars()
                .all()
            )
            field_names = [name for name in chunk if name not in existing_names_rows]

            stmt = insert(Field).returning(Field)
            # stmt = stmt.values(
            #     name=header,
            #     data_type=FieldType.numeric,
            #     data_source_id=data_source_id,
            # ).returning(Field)
            result = (
                session.execute(
                    stmt,
                    [
                        {"name": header, "data_type": FieldType.numeric, "data_source_id": data_source_id}
                        for header in field_names
                    ],
                )
                .scalars()
                .all()
            )
            session.commit()

            fields.extend(result)
            # logger.info(f"Inserted field {header} with ID {field}")

    return fields


def check_numeric_fields(
    session: Session,
    fields: list[Field],
    data_source_id: int,
):
    logger = logging.getLogger(__name__)
    for field in fields:
        if field.data_type == FieldType.numeric:
            logger.info(f"Checking field {field.name} for numeric instances")
            non_numeric_instances = (
                session.execute(
                    select(Instance)
                    .where(
                        Instance.field_id == field.id,
                        Instance.data_source_id == data_source_id,
                        Instance.value_numeric == None,  # noqa: E711
                    )
                    .limit(1)
                )
                .scalars()
                .all()
            )
            if non_numeric_instances:
                _ = session.execute(update(Field).where(Field.id == field.id).values(data_type=FieldType.nominal))
                logger.info(f"Field {field.name} is numeric, setting data type to nominal")


def calculate_numeric_field_detail(
    session: Session,
    field: Field,
):
    logger = logging.getLogger(__name__)
    logger.info(f"Calculating numeric field detail for field {field.name}")

    stats = (
        session.execute(
            select(
                func.min(Instance.value_numeric).label("min_value"),
                func.max(Instance.value_numeric).label("max_value"),
                func.avg(Instance.value_numeric).label("avg_value"),
            ).where(Instance.field_id == field.id, Instance.value_numeric != None)  # noqa: E711
        )
        .one()
        .tuple()
    )

    field_numeric_detail = session.execute(
        select(FieldNumericDetail).where(FieldNumericDetail.id == field.id)
    ).scalar_one_or_none()
    if not field_numeric_detail:
        field_numeric_detail = FieldNumericDetail(
            id=field.id, min_value=stats[0], max_value=stats[1], avg_value=stats[2]
        )
        session.add(field_numeric_detail)
    else:
        _ = session.execute(
            update(FieldNumericDetail)
            .where(FieldNumericDetail.id == field_numeric_detail.id)
            .values(
                min_value=stats[0],
                max_value=stats[1],
                avg_value=stats[2],
            )
        )


@app.task(pydantic=True)
def process_csv(
    data_source_id: int,
    upload_media_type: MediaType,
    encoding: str,
    separator: str,
    quote_char: str,
    create_fields: bool,
    chunk_id: int,
) -> UploadResponseSchema:
    logger = logging.getLogger(__name__)
    if upload_media_type != MediaType.csv:
        raise ValueError("Only CSV data sources are supported")

    with get_sync_db_session() as session:
        data_source_record = session.execute(
            select(DataSource).filter(DataSource.id == data_source_id)
        ).scalar_one_or_none()
        if not data_source_record:
            raise ValueError(f"Data source with ID {data_source_id} not found")
        logger.info(
            f"Looking for chunk {chunk_id} in data source {data_source_id} with upload ID {data_source_record.upload.id}"
        )
        chunk = session.execute(
            select(Chunk).filter(Chunk.id == chunk_id, Chunk.upload_id == data_source_record.upload.id)
        ).scalar_one()

        fields: Sequence[Field] = []
        row_counter: int = 1 if create_fields else 0  # Start from 1 because row 0 is the header
        logger.info(f"Processing chunk {chunk.id} of data source {data_source_id}")
        with open(chunk.path, encoding=encoding) as f:
            reader = csv.reader(f, delimiter=separator, quotechar=quote_char)
            if create_fields:
                # Get headers row
                field_names = next(reader)
                logger.info(f"Creating fields for data source {data_source_id} with headers {field_names}")
                fields = _create_fields(data_source_id, field_names)
            else:
                fields = session.execute(select(Field).filter(Field.data_source_id == data_source_id)).scalars().all()

            BATCH_SIZE = 1000
            instances_to_insert: list[dict[str, str | float | None]] = []
            for row in reader:
                floatified: list[tuple[str, float | None]] = []
                for col in row:
                    try:
                        col_float = float(col)
                    except ValueError:
                        col_float = None
                    floatified.append((col, col_float))

                instances_to_insert.extend(
                    {
                        "row_id": row_counter,
                        "field_id": fields[i].id,
                        "data_source_id": data_source_id,
                        "value_nominal": val,
                        "value_numeric": val_num,
                    }
                    for i, (val, val_num) in enumerate(floatified)
                )
                row_counter += 1

                if len(instances_to_insert) >= BATCH_SIZE:
                    logger.info(f"Inserting {len(instances_to_insert)} rows into Instance table")
                    _ = session.execute(insert(Instance), instances_to_insert)
                    instances_to_insert.clear()

            # Insert any remaining rows
            if instances_to_insert:
                logger.info(f"Inserting remaining {len(instances_to_insert)} rows into Instance table")
                _ = session.execute(insert(Instance), instances_to_insert)
            session.flush()

        fields = list(session.execute(select(Field).filter(Field.data_source_id == data_source_id)).scalars().all())
        # Check if all numeric fields have purely numeric instances
        check_numeric_fields(session, fields, data_source_id)

        for field in fields:
            if field.data_type == FieldType.numeric:
                calculate_numeric_field_detail(session, field)
        session.commit()

    return UploadResponseSchema(
        id=data_source_record.id,
        name=data_source_record.name,
        type=data_source_record.type,
        size=data_source_record.size,
    )


class Histogram(BaseSchema):
    from_: float = pd.Field(..., alias="from")
    to: float = pd.Field(...)
    from_inclusive: bool = pd.Field(...)
    to_inclusive: bool = pd.Field(...)
    frequency: int = pd.Field(...)


@app.task(pydantic=True)
def aggregate_field_values(
    data_source_id: int, field_id: int, bins: int, min: float, max: float, min_inclusive: bool, max_inclusive: bool
) -> list[Histogram]:
    logger = logging.getLogger(__name__)
    logger.info(f"Aggregating field values for field {field_id} in data source {data_source_id}")

    with get_sync_db_session() as session:
        field = session.execute(
            select(Field).filter(Field.id == field_id, Field.data_source_id == data_source_id)
        ).scalar_one_or_none()
        if not field:
            raise ValueError(f"Field with ID {field_id} not found in data source {data_source_id}")

        histograms: list[Histogram] = []
        bin_size = (max - min) / bins
        for i in range(bins):
            from_ = min + i * bin_size
            to = min + (i + 1) * bin_size
            from_inclusive = min_inclusive if i == 0 else True
            to_inclusive = max_inclusive if i == bins - 1 else False

            stmt = (
                select(func.count())
                .select_from(Instance)
                .where(
                    Instance.data_source_id == data_source_id,
                    Instance.field_id == field.id,
                    Instance.value_numeric != None,  # noqa: E711
                    Instance.value_numeric > from_ if from_inclusive else Instance.value_numeric >= from_,
                    Instance.value_numeric < to if to_inclusive else Instance.value_numeric <= to,
                )
            )
            frequency = session.execute(stmt).scalar_one()

            histograms.append(
                Histogram(
                    from_=from_,
                    to=to,
                    from_inclusive=from_inclusive,
                    to_inclusive=to_inclusive,
                    frequency=frequency,
                )
            )

    return histograms


def transform_data(data_source_id: int, df: pandas.DataFrame, attributes: list[Attribute]) -> pandas.DataFrame:
    logger = logging.getLogger(__name__)
    result_df = df.copy()

    with get_sync_db_session() as session:
        stmt = select(Field.id).filter(Field.data_source_id == data_source_id)
        field_ids = session.execute(stmt).scalars().all()

    for attr in attributes:
        # Get the original column name from field_id
        if attr.field_id not in field_ids:
            logger.error(f"Field ID {attr.field_id} not found in data source {data_source_id}")
            continue

        source_column = attr.field_id

        # Skip if the source column doesn't exist in the dataframe
        if source_column not in df.columns:
            logger.warning(f"Column {source_column} not found in dataframe")
            continue

        # Apply the appropriate transformation based on attribute type
        if isinstance(attr, SimpleAttribute):
            # For SimpleAttribute, we just rename the column
            result_df[attr.name] = df[source_column]

        elif isinstance(attr, NominalEnumerationAttribute):
            # For NominalEnumerationAttribute, we map values based on bins
            mapping = {}
            for bin_dict in attr.bins:
                bin_value = bin_dict["value"]
                items = bin_dict["items"]
                for item in items:
                    mapping[item] = bin_value

            result_df[attr.name] = df[source_column].map(mapping)

        elif isinstance(attr, EquidistantIntervalsAttribute):
            # For EquidistantIntervalsAttribute, we create equal-width bins
            numeric_column = pandas.to_numeric(df[source_column], errors="coerce")
            result_df[attr.name] = pandas.cut(
                numeric_column, bins=attr.bins, labels=[f"bin_{i}" for i in range(attr.bins)]
            )

        elif isinstance(attr, EquifrequentIntervalsAttribute):
            # For EquifrequentIntervalsAttribute, we create quantile-based bins
            numeric_column = pandas.to_numeric(df[source_column], errors="coerce")
            result_df[attr.name] = pandas.qcut(
                numeric_column, q=attr.bins, labels=[f"bin_{i}" for i in range(attr.bins)], duplicates="drop"
            )

        elif isinstance(attr, EquisizedIntervalsAttribute):
            # For EquisizedIntervalsAttribute, we create bins with approximately equal number of elements
            numeric_column = pandas.to_numeric(df[source_column], errors="coerce")
            # Calculate bin width based on support parameter
            min_val = numeric_column.min()
            max_val = numeric_column.max()
            range_val = max_val - min_val
            bin_width = attr.support * range_val

            if bin_width > 0:
                # Calculate number of bins
                num_bins = int(range_val / bin_width) + 1
                # Create bins
                result_df[attr.name] = pandas.cut(
                    numeric_column, bins=num_bins, labels=[f"bin_{i}" for i in range(num_bins)]
                )
            else:
                # Handle case where bin_width is 0 or negative
                result_df[attr.name] = "bin_0"

        elif isinstance(attr, NumericIntervalsAttribute):
            # For NumericIntervalsAttribute, we map values to bins based on intervals
            numeric_column = pandas.to_numeric(df[source_column], errors="coerce")

            # Create a new column with default value (for values that don't match any interval)
            result_df[attr.name] = None

            # Apply each bin's intervals
            for bin_info in attr.bins:
                bin_value = bin_info.bin_value

                # For each interval in the bin
                for interval in bin_info.intervals:
                    # Create a mask for values that fall within this interval
                    mask = pandas.Series(False, index=df.index)

                    # Handle lower bound
                    if interval.from_value != float("-inf"):
                        if interval.from_inclusive:
                            mask &= numeric_column >= interval.from_value
                        else:
                            mask &= numeric_column > interval.from_value
                    else:
                        # If from_value is -inf, all values pass the lower bound check
                        mask = pandas.Series(True, index=df.index)

                    # Handle upper bound
                    if interval.to_value != float("inf"):
                        if interval.to_inclusive:
                            mask &= numeric_column <= interval.to_value
                        else:
                            mask &= numeric_column < interval.to_value

                    # Assign bin value to matching rows
                    result_df.loc[mask, attr.name] = bin_value

    return result_df


@app.task(pydantic=True)
def create_dataset(data_source_id: int, name: str, pmml: str) -> DatasetSchema:
    logger = logging.getLogger(__name__)
    logger.info(f"Creating dataset {name} from data source {data_source_id}")

    parser = PmmlTaskParser(pmml)
    attributes = parser.parse()

    with get_sync_db_session() as session:
        data_source = session.execute(select(DataSource).filter(DataSource.id == data_source_id)).scalar_one_or_none()
        if not data_source:
            raise ValueError(f"Data source with ID {data_source_id} not found")

        dataset = session.execute(
            insert(Dataset)
            .values(
                name=name,
                type=data_source.type,
                size=data_source.size,
                data_source_id=data_source.id,
            )
            .returning(Dataset)
        ).scalar_one_or_none()
        session.commit()

        if not dataset:
            raise ValueError(f"Failed to create dataset {name}")

    logger.info(f"Dataset {name} created with ID {dataset.id}")

    with get_sync_db_session() as session:
        stmt = select(Field).filter(Field.data_source_id == data_source_id)
        df = pandas.read_sql(stmt, session.bind)
    transformed_df = transform_data(data_source_id, df, attributes)
    print(f"\n{transformed_df}")

    return DatasetSchema(
        id=dataset.id,
        name=dataset.name,
        type=dataset.type,
        size=dataset.size,
        data_source_id=dataset.data_source_id,
    )
