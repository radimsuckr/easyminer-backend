from easyminer.parsers.pmml.preprocessing import (
    EquidistantIntervalsAttribute,
    SimpleAttribute,
    TransformationDictionary,
    create_attribute_from_pmml,
)
from easyminer.tasks.create_attribute import apply_transformation


def test_simple_attribute_transformation():
    """Test simple attribute transformation logic."""

    # Sample PMML with simple attribute
    simple_xml = """<?xml version="1.0" encoding="UTF-8"?>
    <TransformationDictionary xmlns="http://www.dmg.org/PMML-4_2">
        <DerivedField name="test-simple">
            <MapValues outputColumn="field">
                <FieldColumnPair field="123" column="column" />
            </MapValues>
        </DerivedField>
    </TransformationDictionary>"""

    # Parse and create attribute
    pmml = TransformationDictionary.from_xml_string(simple_xml)
    attr = create_attribute_from_pmml(pmml.derived_fields[0])

    # Verify attribute properties
    assert isinstance(attr, SimpleAttribute)
    assert attr.name == "test-simple"
    assert attr.field_id == 123

    # Test transformations
    test_cases = [
        ("test_string", "test_string"),
        (123.45, "123.45"),
        (None, "None"),
        ("456", "456"),
    ]

    for input_value, expected in test_cases:
        result = apply_transformation(attr, input_value)
        assert result == expected
        assert isinstance(result, str)


def test_numeric_intervals_transformation():
    """Test numeric intervals attribute transformation logic."""

    # Sample PMML with numeric intervals
    intervals_xml = """<?xml version="1.0" encoding="UTF-8"?>
    <TransformationDictionary xmlns="http://www.dmg.org/PMML-4_2">
        <DerivedField name="test-intervals">
            <Discretize field="456">
                <DiscretizeBin binValue="low">
                    <Interval closure="closedOpen" leftMargin="0" rightMargin="10" />
                </DiscretizeBin>
                <DiscretizeBin binValue="high">
                    <Interval closure="closedClosed" leftMargin="10" rightMargin="20" />
                </DiscretizeBin>
            </Discretize>
        </DerivedField>
    </TransformationDictionary>"""

    pmml = TransformationDictionary.from_xml_string(intervals_xml)
    attr = create_attribute_from_pmml(pmml.derived_fields[0])

    # Verify attribute properties
    assert isinstance(attr, EquidistantIntervalsAttribute)
    assert attr.name == "test-intervals"
    assert attr.field_id == 456

    # Test transformations
    test_cases = [
        (5.0, "[0.00, 10.00)"),
        (15.0, "[10.00, 20.00)"),
        (25.0, "out_of_range"),  # Outside the defined intervals
        (None, "None"),
        ("12.5", "[10.00, 20.00)"),  # String input that can be converted to float
    ]

    for input_value, expected in test_cases:
        result = apply_transformation(attr, input_value)
        assert result == expected
        assert isinstance(result, str)


def test_transformation_error_handling():
    """Test error handling in transformation logic."""

    # Create a simple attribute for testing
    simple_xml = """<?xml version="1.0" encoding="UTF-8"?>
    <TransformationDictionary xmlns="http://www.dmg.org/PMML-4_2">
        <DerivedField name="error-test">
            <MapValues outputColumn="field">
                <FieldColumnPair field="789" column="column" />
            </MapValues>
        </DerivedField>
    </TransformationDictionary>"""

    pmml = TransformationDictionary.from_xml_string(simple_xml)
    attr = create_attribute_from_pmml(pmml.derived_fields[0])

    # Test with None input
    result = apply_transformation(attr, None)
    assert result == "None"

    # Test with empty string
    result = apply_transformation(attr, "")
    assert result == ""

    # Verify all results are strings
    assert isinstance(result, str)


def test_transformation_with_numeric_intervals_invalid_input():
    """Test numeric intervals with invalid string input."""

    intervals_xml = """<?xml version="1.0" encoding="UTF-8"?>
    <TransformationDictionary xmlns="http://www.dmg.org/PMML-4_2">
        <DerivedField name="test-intervals">
            <Discretize field="999">
                <DiscretizeBin binValue="bin1">
                    <Interval closure="closedOpen" leftMargin="0" rightMargin="10" />
                </DiscretizeBin>
            </Discretize>
        </DerivedField>
    </TransformationDictionary>"""

    pmml = TransformationDictionary.from_xml_string(intervals_xml)
    attr = create_attribute_from_pmml(pmml.derived_fields[0])

    # Test with string that cannot be converted to float
    result = apply_transformation(attr, "not_a_number")
    assert result == "None"  # Should handle conversion error gracefully


def test_user_salary_equidistant_intervals():
    """Test the specific PMML provided by the user."""

    user_xml = """<?xml version="1.0" encoding="UTF-8"?>
<TransformationDictionary xmlns="http://www.dmg.org/PMML-4_2">
	<DerivedField name="salary-equidistant-intervals">
		<Discretize field="12">
			<DiscretizeBin binValue="[8110;9587)">
				<Interval closure="closedOpen" leftMargin="8110" rightMargin="9587" />
			</DiscretizeBin>
			<DiscretizeBin binValue="[9587;11064)">
				<Interval closure="closedOpen" leftMargin="9587" rightMargin="11064" />
			</DiscretizeBin>
			<DiscretizeBin binValue="[11064;12541]">
				<Interval closure="closedClosed" leftMargin="11064" rightMargin="12541" />
			</DiscretizeBin>
		</Discretize>
	</DerivedField>
</TransformationDictionary>"""

    pmml = TransformationDictionary.from_xml_string(user_xml)
    attr = create_attribute_from_pmml(pmml.derived_fields[0])

    # Verify it creates the correct attribute type
    assert isinstance(attr, EquidistantIntervalsAttribute)
    assert attr.name == "salary-equidistant-intervals"
    assert attr.field_id == 12
    assert attr.bins == 3
    assert attr.min_value == 8110.0
    assert attr.max_value == 12541.0

    # Test specific transformations that were failing before
    test_cases = [
        (8500.0, "[8110.00, 9587.00)"),
        (10000.0, "[9587.00, 11064.00)"),
        (12000.0, "[11064.00, 12541.00)"),
        (7000.0, "out_of_range"),  # Below minimum
        (13000.0, "out_of_range"),  # Above maximum
        (None, "None"),  # Null value
    ]

    for input_value, expected in test_cases:
        result = apply_transformation(attr, input_value)
        assert result == expected, f"For input {input_value}, expected {expected} but got {result}"
        assert isinstance(result, str)
