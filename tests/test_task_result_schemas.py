import pytest
from pydantic import ValidationError

from easyminer.schemas.data import DbType, IntervalResult
from easyminer.schemas.preprocessing import AttributeResult, DatasetResult


class TestDatasetResultSchema:
    """Test that Dataset result schema matches Swagger specification."""

    def test_dataset_result_schema_fields(self):
        """Verify DatasetResult has all required fields with correct types."""
        dataset = DatasetResult(id=1, name="Test Dataset", data_source=123, type=DbType.limited, size=1000)

        assert dataset.id == 1
        assert dataset.name == "Test Dataset"
        assert dataset.data_source == 123
        assert dataset.type == "limited"
        assert dataset.size == 1000

    def test_dataset_result_serialization_camel_case(self):
        """Verify DatasetResult serializes to camelCase JSON."""
        dataset = DatasetResult(id=1, name="Test Dataset", data_source=123, type=DbType.limited, size=1000)

        json_dict = dataset.model_dump(by_alias=True)

        assert "dataSource" in json_dict
        assert json_dict["dataSource"] == 123
        assert json_dict["id"] == 1
        assert json_dict["name"] == "Test Dataset"
        assert json_dict["type"] == "limited"
        assert json_dict["size"] == 1000

    def test_dataset_result_required_fields(self):
        """Verify all fields are required."""
        with pytest.raises(ValidationError):
            DatasetResult(id=1, name="Test")  # Missing required fields


class TestAttributeResultSchema:
    """Test that Attribute result schema matches Swagger specification."""

    def test_attribute_result_schema_fields(self):
        """Verify AttributeResult has all required fields with correct types."""
        attribute = AttributeResult(
            id=1,
            dataset=123,
            field=456,
            name="Test Attribute",
            unique_values_size=10,
        )

        assert attribute.id == 1
        assert attribute.dataset == 123
        assert attribute.field == 456
        assert attribute.name == "Test Attribute"
        assert attribute.unique_values_size == 10

    def test_attribute_result_serialization_camel_case(self):
        """Verify AttributeResult serializes to camelCase JSON."""
        attribute = AttributeResult(
            id=1,
            dataset=123,
            field=456,
            name="Test Attribute",
            unique_values_size=10,
        )

        json_dict = attribute.model_dump(by_alias=True)

        assert "dataset" in json_dict
        assert "field" in json_dict
        assert "uniqueValuesSize" in json_dict
        assert json_dict["id"] == 1
        assert json_dict["dataset"] == 123
        assert json_dict["field"] == 456
        assert json_dict["name"] == "Test Attribute"
        assert json_dict["uniqueValuesSize"] == 10

    def test_attribute_result_required_fields(self):
        """Verify all fields are required."""
        with pytest.raises(ValidationError):
            AttributeResult(id=1, name="Test")  # Missing required fields

    def test_attribute_result_array(self):
        """Verify that a list of AttributeResult can be created (task returns array)."""
        attributes = [
            AttributeResult(
                id=1,
                dataset=123,
                field=456,
                name="Attribute 1",
                unique_values_size=10,
            ),
            AttributeResult(
                id=2,
                dataset=123,
                field=457,
                name="Attribute 2",
                unique_values_size=5,
            ),
        ]

        assert len(attributes) == 2
        assert attributes[0].id == 1
        assert attributes[1].id == 2


class TestIntervalResultSchema:
    """Test that Interval result schema matches Swagger specification."""

    def test_interval_result_schema_fields(self):
        """Verify IntervalResult has all required fields with correct types."""
        interval = IntervalResult(from_=0.0, to=10.0, from_inclusive=True, to_inclusive=False, frequency=42)

        assert interval.from_ == 0.0
        assert interval.to == 10.0
        assert interval.from_inclusive is True
        assert interval.to_inclusive is False
        assert interval.frequency == 42

    def test_interval_result_serialization_camel_case(self):
        """Verify IntervalResult serializes to camelCase JSON."""
        interval = IntervalResult(from_=0.0, to=10.0, from_inclusive=True, to_inclusive=False, frequency=42)

        json_dict = interval.model_dump(by_alias=True)

        assert "from" in json_dict
        assert "to" in json_dict
        assert "fromInclusive" in json_dict
        assert "toInclusive" in json_dict
        assert "frequency" in json_dict
        assert json_dict["from"] == 0.0
        assert json_dict["to"] == 10.0
        assert json_dict["fromInclusive"] is True
        assert json_dict["toInclusive"] is False
        assert json_dict["frequency"] == 42

    def test_interval_result_null_values(self):
        """Verify IntervalResult supports null from/to for missing values."""
        interval = IntervalResult(from_=None, to=None, from_inclusive=True, to_inclusive=True, frequency=3)

        assert interval.from_ is None
        assert interval.to is None
        assert interval.frequency == 3

        json_dict = interval.model_dump(by_alias=True)
        assert json_dict["from"] is None
        assert json_dict["to"] is None
        assert json_dict["frequency"] == 3

    def test_interval_result_array(self):
        """Verify that a list of IntervalResult can be created (task returns array)."""
        intervals = [
            IntervalResult(from_=0.0, to=10.0, from_inclusive=True, to_inclusive=False, frequency=42),
            IntervalResult(from_=10.0, to=20.0, from_inclusive=True, to_inclusive=False, frequency=38),
            IntervalResult(from_=None, to=None, from_inclusive=True, to_inclusive=True, frequency=5),
        ]

        assert len(intervals) == 3
        assert intervals[0].from_ == 0.0
        assert intervals[1].from_ == 10.0
        assert intervals[2].from_ is None


class TestTaskResultFormat:
    """Test that task results can be properly deserialized from Celery task returns."""

    def test_dataset_from_dict(self):
        """Verify DatasetResult can be created from dict returned by create_dataset task."""
        task_result = {"id": 1, "name": "Test Dataset", "dataSource": 123, "type": "limited", "size": 1000}

        dataset = DatasetResult.model_validate(task_result)

        assert dataset.id == 1
        assert dataset.data_source == 123

    def test_attributes_from_list_of_dicts(self):
        """Verify AttributeResult list can be created from list returned by create_attributes task."""
        task_result = [
            {"id": 1, "dataset": 123, "field": 456, "name": "Attribute 1", "uniqueValuesSize": 10},
            {"id": 2, "dataset": 123, "field": 457, "name": "Attribute 2", "uniqueValuesSize": 5},
        ]

        attributes = [AttributeResult.model_validate(attr) for attr in task_result]

        assert len(attributes) == 2
        assert attributes[0].unique_values_size == 10
        assert attributes[1].unique_values_size == 5

    def test_intervals_from_list_of_dicts(self):
        """Verify IntervalResult list can be created from list returned by aggregate_field_values task."""
        task_result = [
            {"from": 0.0, "to": 10.0, "fromInclusive": True, "toInclusive": False, "frequency": 42},
            {"from": None, "to": None, "fromInclusive": True, "toInclusive": True, "frequency": 3},
        ]

        intervals = [IntervalResult.model_validate(interval) for interval in task_result]

        assert len(intervals) == 2
        assert intervals[0].from_ == 0.0
        assert intervals[1].from_ is None
