import csv
import logging

import pydantic as pd
from sqlalchemy import func, insert, select, update
from sqlalchemy.orm import Session

from easyminer.database import get_sync_db_session
from easyminer.models import DataSource, Field, FieldNumericDetail, FieldType, Instance
from easyminer.models.dataset import Dataset
from easyminer.schemas import BaseSchema
from easyminer.schemas.data import MediaType, UploadResponseSchema
from easyminer.schemas.preprocessing import DatasetSchema
from easyminer.worker import app


def create_fields(
    data_source_id: int,
    field_names: list[str],
) -> list[Field]:
    logger = logging.getLogger(__name__)

    fields: list[Field] = []
    with get_sync_db_session() as session:
        for ix, header in enumerate(field_names):
            logger.info(f"Header: {header}")
            # Check if field already exists
            existing_field_id = session.execute(
                select(Field.id).filter(Field.name == header, Field.data_source_id == data_source_id)
            ).scalar_one_or_none()
            if existing_field_id:
                logger.warning(f"Field {header} already exists, skipping.")
                continue
            stmt = insert(Field) if not existing_field_id else (update(Field).where(Field.id == existing_field_id))
            stmt = stmt.values(
                name=header,
                data_type=FieldType.numeric,
                data_source_id=data_source_id,
            ).returning(Field)
            field = session.execute(stmt).scalar_one()
            session.commit()
            if not field:
                logger.error(f"Failed to insert field {header}")
                continue
            fields.insert(ix, field)  # To ensure order with data rows
            logger.info(f"Inserted field {header} with ID {field}")

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
    data_source_id: int, upload_media_type: MediaType, encoding: str, separator: str, quote_char: str
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
        chunks = data_source_record.upload.chunks

        fields: list[Field] = []
        row_counter: int = 1  # Start from 1 because row 0 is the header
        for ix, chunk in enumerate(chunks):
            logger.info(f"Processing chunk {chunk.id} of data source {data_source_id}")
            with open(chunk.path, encoding=encoding) as f:
                reader = csv.reader(f, delimiter=separator, quotechar=quote_char)
                if ix == 0:
                    # Get headers row
                    field_names = next(reader)
                    fields = create_fields(data_source_id, field_names)

                instances: list[Instance] = []
                for row in reader:
                    for iz, col in enumerate(row):
                        f = fields[iz]
                        col_float: float | None = None
                        try:
                            col_float = float(col)
                        except ValueError:
                            pass
                        instances.append(
                            Instance(
                                row_id=row_counter,
                                field_id=f.id,
                                data_source_id=data_source_id,
                                value_nominal=col,
                                value_numeric=col_float,
                            )
                        )
                    row_counter += 1
                session.bulk_save_objects(instances)

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


@app.task(pydantic=True)
def create_dataset(data_source_id: int, name: str) -> DatasetSchema:
    logger = logging.getLogger(__name__)
    logger.info(f"Creating dataset {name} from data source {data_source_id}")

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

    return DatasetSchema(
        id=dataset.id,
        name=dataset.name,
        type=dataset.type,
        size=dataset.size,
        data_source_id=dataset.data_source_id,
    )
