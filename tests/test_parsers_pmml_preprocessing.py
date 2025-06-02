from easyminer.parsers.pmml.preprocessing import (
    EquidistantIntervalsAttribute,
    EquifrequentIntervalsAttribute,
    EquisizedIntervalsAttribute,
    NominalEnumerationAttribute,
    NumericIntervalsAttribute,
    SimpleAttribute,
    TransformationDictionary,
    create_attribute_from_pmml,
)


def test_salary_eachone_simple_attribute():
    """Test parsing of salary-eachone.xml - simple MapValues transformation"""
    xml_content = """<?xml version="1.0" encoding="UTF-8"?>
    <TransformationDictionary xmlns="http://www.dmg.org/PMML-4_2">
        <DerivedField name="salary-eachone">
            <MapValues outputColumn="field">
                <FieldColumnPair field="734" column="column" />
            </MapValues>
        </DerivedField>
    </TransformationDictionary>"""

    pmml = TransformationDictionary.from_xml_string(xml_content)
    attribute = create_attribute_from_pmml(pmml.derived_fields[0])

    assert isinstance(attribute, SimpleAttribute)
    assert attribute.name == "salary-eachone"
    assert attribute.field_id == 734
    assert attribute.transform(100.0) == 100.0
    assert attribute.transform("test") == "test"


def test_salary_eachone_simple_attribute_bytes():
    """Test parsing from bytes with encoding declaration"""
    xml_bytes = b"""<?xml version="1.0" encoding="UTF-8"?>
    <TransformationDictionary xmlns="http://www.dmg.org/PMML-4_2">
        <DerivedField name="salary-eachone">
            <MapValues outputColumn="field">
                <FieldColumnPair field="734" column="column" />
            </MapValues>
        </DerivedField>
    </TransformationDictionary>"""

    pmml = TransformationDictionary.from_xml_bytes(xml_bytes)
    attribute = create_attribute_from_pmml(pmml.derived_fields[0])

    assert isinstance(attribute, SimpleAttribute)
    assert attribute.name == "salary-eachone"
    assert attribute.field_id == 734


def test_salary_equidistant_intervals():
    """Test parsing of salary-equidistant-intervals.xml"""
    xml_content = """<?xml version="1.0" encoding="UTF-8"?>
    <TransformationDictionary xmlns="http://www.dmg.org/PMML-4_2">
        <DerivedField name="salary-equidistant-intervals">
            <Discretize field="734">
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

    pmml = TransformationDictionary.from_xml_string(xml_content)
    attribute = create_attribute_from_pmml(pmml.derived_fields[0])

    assert isinstance(attribute, EquidistantIntervalsAttribute)
    assert attribute.name == "salary-equidistant-intervals"
    assert attribute.field_id == 734
    assert attribute.bins == 3
    assert attribute.min_value == 8110.0
    assert attribute.max_value == 12541.0

    # Test transformations
    assert attribute.transform(8500.0) == "[8110.00, 9587.00)"
    assert attribute.transform(10000.0) == "[9587.00, 11064.00)"
    assert attribute.transform(12000.0) == "[11064.00, 12541.00)"


def test_salary_equifrequent_intervals():
    """Test parsing of salary-equifrequent-intervals.xml"""
    xml_content = """<?xml version="1.0" encoding="UTF-8"?>
    <TransformationDictionary xmlns="http://www.dmg.org/PMML-4_2">
        <DerivedField name="salary-equifrequent-intervals">
            <Discretize field="734">
                <Extension name="algorithm" value="equifrequent-intervals" />
                <Extension name="bins" value="5" />
                <Extension name="leftMargin" value="8110" />
                <Extension name="rightMargin" value="12541" />
            </Discretize>
        </DerivedField>
    </TransformationDictionary>"""

    pmml = TransformationDictionary.from_xml_string(xml_content)
    cutpoints = [9000.0, 10000.0, 11000.0, 12000.0]
    attribute = create_attribute_from_pmml(pmml.derived_fields[0], cutpoints=cutpoints)

    assert isinstance(attribute, EquifrequentIntervalsAttribute)
    assert attribute.name == "salary-equifrequent-intervals"
    assert attribute.field_id == 734
    assert attribute.bins == 5
    assert attribute.cutpoints == cutpoints

    # Test transformations
    assert attribute.transform(8500.0) == "bin_0"
    assert attribute.transform(9500.0) == "bin_1"
    assert attribute.transform(12500.0) == "bin_4"


def test_salary_equisized_intervals():
    """Test parsing of salary-equisized-intervals.xml"""
    xml_content = """<?xml version="1.0" encoding="UTF-8"?>
    <TransformationDictionary xmlns="http://www.dmg.org/PMML-4_2">
        <DerivedField name="salary-equisized-intervals">
            <Discretize field="734">
                <Extension name="algorithm" value="equisized-intervals" />
                <Extension name="support" value="0.2" />
                <Extension name="leftMargin" value="8110" />
                <Extension name="rightMargin" value="12541" />
            </Discretize>
        </DerivedField>
    </TransformationDictionary>"""

    pmml = TransformationDictionary.from_xml_string(xml_content)
    attribute = create_attribute_from_pmml(pmml.derived_fields[0])

    assert isinstance(attribute, EquisizedIntervalsAttribute)
    assert attribute.name == "salary-equisized-intervals"
    assert attribute.field_id == 734
    assert attribute.support == 0.2
    assert attribute.min_value == 8110.0
    assert attribute.max_value == 12541.0

    # Test transformation
    result = attribute.transform(9000.0)
    assert result.startswith("[")
    assert result.endswith(")")


def test_salary_interval_enumeration():
    """Test parsing of salary-interval-enumeration.xml"""
    xml_content = """<?xml version="1.0" encoding="UTF-8"?>
    <TransformationDictionary xmlns="http://www.dmg.org/PMML-4_2">
        <DerivedField name="salar-interval-enumeraton">
            <Discretize field="734">
                <DiscretizeBin binValue="outliers">
                    <Interval closure="closedOpen" leftMargin="8110" rightMargin="9000" />
                </DiscretizeBin>
                <DiscretizeBin binValue="outliers">
                    <Interval closure="closedClosed" leftMargin="11000" rightMargin="12541" />
                </DiscretizeBin>
                <DiscretizeBin binValue="main">
                    <Interval closure="closedClosed" leftMargin="10000" rightMargin="10500" />
                </DiscretizeBin>
            </Discretize>
        </DerivedField>
    </TransformationDictionary>"""

    pmml = TransformationDictionary.from_xml_string(xml_content)
    attribute = create_attribute_from_pmml(pmml.derived_fields[0])

    assert isinstance(attribute, NumericIntervalsAttribute)
    assert attribute.name == "salar-interval-enumeraton"
    assert attribute.field_id == 734
    assert len(attribute.bins) == 2  # Two unique bin values: "outliers" and "main"

    # Find the outliers bin (should have 2 intervals)
    outliers_bin = next(bin for bin in attribute.bins if bin.bin_value == "outliers")
    main_bin = next(bin for bin in attribute.bins if bin.bin_value == "main")

    assert len(outliers_bin.intervals) == 2  # Two intervals for outliers
    assert len(main_bin.intervals) == 1  # One interval for main

    # Test transformations
    assert attribute.transform(8500.0) == "outliers"
    assert attribute.transform(10250.0) == "main"
    assert attribute.transform(11500.0) == "outliers"
    assert attribute.transform(9500.0) is None  # Outside all intervals


def test_salary_nominal_enumeration():
    """Test parsing of salary-nominal-enumeration.xml"""
    xml_content = """<?xml version="1.0" encoding="UTF-8"?>
    <TransformationDictionary xmlns="http://www.dmg.org/PMML-4_2">
        <DerivedField name="salary-nominal-enumeration">
            <MapValues outputColumn="field">
                <FieldColumnPair field="734" />
                <InlineTable>
                    <row>
                        <column>8110</column>
                        <field>left</field>
                    </row>
                    <row>
                        <column>10000</column>
                        <field>2 selected values</field>
                    </row>
                    <row>
                        <column>12541</column>
                        <field>2 selected values</field>
                    </row>
                </InlineTable>
            </MapValues>
        </DerivedField>
    </TransformationDictionary>"""

    pmml = TransformationDictionary.from_xml_string(xml_content)
    attribute = create_attribute_from_pmml(pmml.derived_fields[0])

    assert isinstance(attribute, NominalEnumerationAttribute)
    assert attribute.name == "salary-nominal-enumeration"
    assert attribute.field_id == 734
    assert len(attribute.bins) == 2

    # Test transformations
    assert attribute.transform("8110") == "left"
    assert attribute.transform("10000") == "2 selected values"
    assert attribute.transform("12541") == "2 selected values"
    assert attribute.transform("9999") is None


def test_factory_with_overrides():
    """Test factory function with parameter overrides"""
    xml_content = """<?xml version="1.0" encoding="UTF-8"?>
    <TransformationDictionary xmlns="http://www.dmg.org/PMML-4_2">
        <DerivedField name="test-intervals">
            <Discretize field="123">
                <DiscretizeBin binValue="[0;10)">
                    <Interval closure="closedOpen" leftMargin="0" rightMargin="10" />
                </DiscretizeBin>
            </Discretize>
        </DerivedField>
    </TransformationDictionary>"""

    pmml = TransformationDictionary.from_xml_string(xml_content)

    # Override min/max values
    attribute = create_attribute_from_pmml(pmml.derived_fields[0], min_value=5.0, max_value=15.0)

    # Single bin should be treated as NumericIntervalsAttribute since it doesn't meet the criteria for equidistant
    assert isinstance(attribute, NumericIntervalsAttribute)
    assert len(attribute.bins) == 1
    assert attribute.bins[0].bin_value == "[0;10)"


def test_xml_round_trip():
    """Test that we can parse XML and serialize it back"""
    xml_content = """<?xml version="1.0" encoding="UTF-8"?>
    <TransformationDictionary xmlns="http://www.dmg.org/PMML-4_2">
        <DerivedField name="test">
            <MapValues outputColumn="field">
                <FieldColumnPair field="123" />
            </MapValues>
        </DerivedField>
    </TransformationDictionary>"""

    pmml = TransformationDictionary.from_xml_string(xml_content)
    serialized = pmml.to_xml()

    # Parse it again
    assert isinstance(serialized, bytes)
    pmml2 = TransformationDictionary.from_xml_bytes(serialized)

    assert pmml2.derived_fields[0].name == "test"
    assert pmml2.derived_fields[0].map_values
    assert pmml2.derived_fields[0].map_values.field_column_pair.field == "123"


def test_numeric_intervals_closure_types():
    """Test different closure types for numeric intervals"""
    xml_content = """<?xml version="1.0" encoding="UTF-8"?>
    <TransformationDictionary xmlns="http://www.dmg.org/PMML-4_2">
        <DerivedField name="test-closures">
            <Discretize field="456">
                <DiscretizeBin binValue="open">
                    <Interval closure="openOpen" leftMargin="0" rightMargin="10" />
                </DiscretizeBin>
                <DiscretizeBin binValue="closed">
                    <Interval closure="closedClosed" leftMargin="10" rightMargin="20" />
                </DiscretizeBin>
            </Discretize>
        </DerivedField>
    </TransformationDictionary>"""

    pmml = TransformationDictionary.from_xml_string(xml_content)
    attribute = create_attribute_from_pmml(pmml.derived_fields[0])

    # These intervals are consecutive (0-10 and 10-20) so they should be treated as equidistant
    assert isinstance(attribute, EquidistantIntervalsAttribute)
    assert attribute.name == "test-closures"
    assert attribute.field_id == 456
    assert attribute.bins == 2
    assert attribute.min_value == 0.0
    assert attribute.max_value == 20.0

    # Test transformations using the equidistant logic
    assert attribute.transform(5.0) == "[0.00, 10.00)"
    assert attribute.transform(15.0) == "[10.00, 20.00)"


def test_xml_with_different_encodings():
    """Test parsing XML with different encoding declarations"""
    # Test UTF-8
    xml_utf8 = """<?xml version="1.0" encoding="UTF-8"?>
    <TransformationDictionary xmlns="http://www.dmg.org/PMML-4_2">
        <DerivedField name="test-utf8">
            <MapValues outputColumn="field">
                <FieldColumnPair field="123" />
            </MapValues>
        </DerivedField>
    </TransformationDictionary>"""

    pmml_utf8 = TransformationDictionary.from_xml_string(xml_utf8)
    assert pmml_utf8.derived_fields[0].name == "test-utf8"

    # Test ISO-8859-1
    xml_iso = """<?xml version="1.0" encoding="ISO-8859-1"?>
    <TransformationDictionary xmlns="http://www.dmg.org/PMML-4_2">
        <DerivedField name="test-iso">
            <MapValues outputColumn="field">
                <FieldColumnPair field="456" />
            </MapValues>
        </DerivedField>
    </TransformationDictionary>"""

    pmml_iso = TransformationDictionary.from_xml_string(xml_iso)
    assert pmml_iso.derived_fields[0].name == "test-iso"


def test_unsupported_algorithm_raises_not_implemented_error():
    """Test that unsupported discretization algorithms raise NotImplementedError"""
    xml_content = """<?xml version="1.0" encoding="UTF-8"?>
    <TransformationDictionary xmlns="http://www.dmg.org/PMML-4_2">
        <DerivedField name="unsupported-algorithm">
            <Discretize field="123">
                <Extension name="algorithm" value="unsupported-algorithm" />
                <Extension name="someParam" value="someValue" />
            </Discretize>
        </DerivedField>
    </TransformationDictionary>"""

    pmml = TransformationDictionary.from_xml_string(xml_content)

    # Should raise NotImplementedError for unsupported algorithm
    try:
        _ = create_attribute_from_pmml(pmml.derived_fields[0])
        assert False, "Expected NotImplementedError to be raised"
    except NotImplementedError as e:
        assert "unsupported-algorithm" in str(e)
    except Exception as e:
        assert False, f"Expected NotImplementedError, got {type(e).__name__}: {e}"


def test_multiple_attributes_in_single_pmml():
    """Test that multiple DerivedField elements can be parsed in a single PMML document"""
    xml_content = """<?xml version="1.0" encoding="UTF-8"?>
    <TransformationDictionary xmlns="http://www.dmg.org/PMML-4_2">
        <DerivedField name="simple-attribute">
            <MapValues outputColumn="field">
                <FieldColumnPair field="100" column="column" />
            </MapValues>
        </DerivedField>
        <DerivedField name="equidistant-intervals">
            <Discretize field="200">
                <DiscretizeBin binValue="[0;10)">
                    <Interval closure="closedOpen" leftMargin="0" rightMargin="10" />
                </DiscretizeBin>
                <DiscretizeBin binValue="[10;20]">
                    <Interval closure="closedClosed" leftMargin="10" rightMargin="20" />
                </DiscretizeBin>
            </Discretize>
        </DerivedField>
        <DerivedField name="nominal-enumeration">
            <MapValues outputColumn="field">
                <FieldColumnPair field="300" />
                <InlineTable>
                    <row>
                        <column>A</column>
                        <field>category1</field>
                    </row>
                    <row>
                        <column>B</column>
                        <field>category2</field>
                    </row>
                </InlineTable>
            </MapValues>
        </DerivedField>
    </TransformationDictionary>"""

    pmml = TransformationDictionary.from_xml_string(xml_content)

    # Should have 3 derived fields
    assert len(pmml.derived_fields) == 3

    # Test each field can be converted to attributes
    attribute1 = create_attribute_from_pmml(pmml.derived_fields[0])
    attribute2 = create_attribute_from_pmml(pmml.derived_fields[1])
    attribute3 = create_attribute_from_pmml(pmml.derived_fields[2])

    # Verify the types and properties
    assert isinstance(attribute1, SimpleAttribute)
    assert attribute1.name == "simple-attribute"
    assert attribute1.field_id == 100

    assert isinstance(attribute2, EquidistantIntervalsAttribute)
    assert attribute2.name == "equidistant-intervals"
    assert attribute2.field_id == 200
    assert attribute2.bins == 2

    assert isinstance(attribute3, NominalEnumerationAttribute)
    assert attribute3.name == "nominal-enumeration"
    assert attribute3.field_id == 300
    assert len(attribute3.bins) == 2

    # Test transformations work for each
    assert attribute1.transform("test") == "test"
    assert attribute2.transform(5.0) == "[0.00, 10.00)"
    assert attribute3.transform("A") == "category1"
