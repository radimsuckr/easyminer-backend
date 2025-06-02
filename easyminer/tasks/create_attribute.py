import logging

from sqlalchemy import exists, select
from sqlalchemy.dialects.postgresql import insert as pginsert
from sqlalchemy.orm import joinedload

from easyminer.database import get_sync_db_session
from easyminer.models import data as mdata
from easyminer.models import preprocessing as mprep
from easyminer.parser import Attribute, PmmlTaskParser
from easyminer.schemas.data import FieldType
from easyminer.worker import app

logger = logging.getLogger(__name__)


@app.task
def create_attributes(dataset_id: int, xml: str):
    if len(xml) == 0:
        raise ValueError("PMML cannot be empty")

    with get_sync_db_session() as db:
        dataset = db.get(mprep.Dataset, dataset_id, options=[joinedload(mprep.Dataset.data_source)])
        if not dataset:
            raise ValueError(f"Dataset with id {dataset_id} not found")

        parser = PmmlTaskParser(xml)
        parsed_attributes = parser.parse()

        attributes: dict[mprep.Attribute, Attribute] = {}
        for attribute in parsed_attributes:
            field_exists = db.execute(
                select(
                    exists().where(
                        mdata.Field.data_source_id == dataset.data_source_id, mdata.Field.id == attribute.field_id
                    )
                )
            ).scalar_one()
            if not field_exists:
                raise ValueError(
                    f"Field with ID {attribute.field_id} does not exist in data source ID {dataset.data_source_id}"
                )
            item = mprep.Attribute(name=attribute.name, dataset_id=dataset.id, field_id=attribute.field_id)
            db.add(item)
            db.flush()
            attributes[item] = attribute

        values_to_insert: dict[str, list[int]] = {}
        for dbattr, attr in attributes.items():
            instances = db.scalars(
                select(mdata.DataSourceInstance).where(mdata.DataSourceInstance.field_id == dbattr.field_id)
            ).all()
            for instance in instances:
                transformed = (
                    attr.transform(instance.value_numeric)
                    if instance.field.data_type == FieldType.numeric
                    else attr.transform(instance.value_nominal)
                )
                if transformed is None:
                    transformed = "None"
                else:
                    transformed = str(transformed)
                if transformed not in values_to_insert:
                    values_to_insert[transformed] = [dbattr.id]
                else:
                    values_to_insert[transformed].append(dbattr.id)

        for value, attrs_ids in values_to_insert.items():
            frequency = len(attrs_ids)
            for attr_id in attrs_ids:
                _ = db.execute(
                    pginsert(mprep.DatasetValue)
                    .values(value=value, frequency=frequency, attribute_id=attr_id)
                    .on_conflict_do_nothing()
                )
            db.flush()
        db.commit()
