from unittest.mock import patch

import pytest
from pytest import raises

from easyminer.models.preprocessing import Dataset
from easyminer.tasks.create_attribute import create_attributes


def test_task_throws_valueerror_on_empty_pmml():
    id = -1
    pmml = ""

    with raises(ValueError, match="PMML cannot be empty"):
        create_attributes(id, pmml)


def test_task_throws_valueerror_on_invalid_dataset_id():
    id = -1
    pmml = "something"

    with raises(ValueError, match=f"Dataset with id {id} not found"):
        create_attributes(id, pmml)


@pytest.mark.skip
@patch("easyminer.tasks.create_attribute._get_dataset")
def test_task_succeeds(_get_dataset_mock):
    id = 4
    pmml = """<?xml version="1.0" encoding="UTF-8"?>
    <PMML version="4.2" xmlns="http://www.dmg.org/PMML-4_2">
        <Header copyright="Minimal Example"/>
        <DataDictionary>
            <DataField name="SourceFieldID_or_Name" optype="categorical" dataType="string"/>
            <!-- Assume this field corresponds to ID 1 -->
        </DataDictionary>
        <TransformationDictionary>
            <!-- Minimal DerivedField for SimpleAttribute mapping -->
            <DerivedField name="NewAttributeName" optype="categorical" dataType="string">
                <MapValues>
                    <!-- The parser expects an integer ID here -->
                    <FieldColumnPair field="4"/>
                    <!-- NO InlineTable or other elements inside MapValues -->
                </MapValues>
            </DerivedField>
        </TransformationDictionary>
    </PMML>"""
    _get_dataset_mock.return_value = Dataset(attributes=[])

    _ = create_attributes(id, pmml)
