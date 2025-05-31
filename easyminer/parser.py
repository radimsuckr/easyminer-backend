from collections import defaultdict
from typing import Protocol, runtime_checkable
from xml.etree.ElementTree import Element

import defusedxml.ElementTree as ET
from pydantic import BaseModel


@runtime_checkable
class Transformable(Protocol):
    """Protocol for objects that can transform values"""

    def transform(self, value: str | float) -> str | float | None: ...


class SimpleAttribute(BaseModel):
    name: str
    field_id: int

    def transform(self, value: str | float) -> str | float:
        return value


class NominalEnumerationAttribute(BaseModel):
    name: str
    field_id: int
    bins: list[dict[str, list[str]]]

    def transform(self, value: str | float) -> str | None:
        str_value = str(value)
        for bin_dict in self.bins:
            for bin_label, items in bin_dict.items():
                if str_value in items:
                    return bin_label
        return None


class EquidistantIntervalsAttribute(BaseModel):
    name: str
    field_id: int
    bins: int
    min_value: float
    max_value: float

    def transform(self, value: str | float) -> str:
        if isinstance(value, str):
            try:
                numeric_value = float(value)
            except ValueError:
                return "invalid_value"
        else:
            numeric_value = value

        width = (self.max_value - self.min_value) / self.bins

        for i in range(self.bins):
            lower = self.min_value + i * width
            upper = lower + width
            if lower <= numeric_value < upper or (i == self.bins - 1 and numeric_value == upper):
                return f"[{lower:.2f}, {upper:.2f})"
        return "out_of_range"


class EquifrequentIntervalsAttribute(BaseModel):
    name: str
    field_id: int
    bins: int
    cutpoints: list[float]

    def transform(self, value: str | float) -> str:
        if isinstance(value, str):
            try:
                numeric_value = float(value)
            except ValueError:
                return "invalid_value"
        else:
            numeric_value = value

        for i, cut in enumerate(self.cutpoints):
            if numeric_value < cut:
                return f"bin_{i}"
        return f"bin_{len(self.cutpoints)}"


class EquisizedIntervalsAttribute(BaseModel):
    name: str
    field_id: int
    support: float
    min_value: float
    max_value: float

    def transform(self, value: str | float) -> str:
        if isinstance(value, str):
            try:
                numeric_value = float(value)
            except ValueError:
                return "invalid_value"
        else:
            numeric_value = value

        if self.max_value <= self.min_value:
            return "invalid_range"

        range_ = self.max_value - self.min_value
        bin_width = range_ * self.support
        if bin_width <= 0:
            return "invalid_bin_width"

        bin_index = int((numeric_value - self.min_value) / bin_width)
        max_bin = int(1 / self.support) - 1
        bin_index = min(max(bin_index, 0), max_bin)
        bin_start = self.min_value + bin_width * bin_index
        bin_end = bin_start + bin_width
        return f"[{bin_start:.2f}, {bin_end:.2f})"


class NumericIntervalsAttribute(BaseModel):
    class Interval(BaseModel):
        from_value: float
        from_inclusive: bool
        to_value: float
        to_inclusive: bool

        def contains(self, value: float) -> bool:
            if self.from_inclusive:
                if value < self.from_value:
                    return False
            else:
                if value <= self.from_value:
                    return False
            if self.to_inclusive:
                if value > self.to_value:
                    return False
            else:
                if value >= self.to_value:
                    return False
            return True

    class Bin(BaseModel):
        bin_value: str
        intervals: list["NumericIntervalsAttribute.Interval"]

    name: str
    field_id: int
    bins: list["NumericIntervalsAttribute.Bin"]

    def transform(self, value: str | float) -> str | None:
        if isinstance(value, str):
            try:
                numeric_value = float(value)
            except ValueError:
                return None
        else:
            numeric_value = value

        for bin_item in self.bins:
            for interval in bin_item.intervals:
                if interval.contains(numeric_value):
                    return bin_item.bin_value
        return None


Attribute = (
    SimpleAttribute
    | NominalEnumerationAttribute
    | EquidistantIntervalsAttribute
    | EquifrequentIntervalsAttribute
    | EquisizedIntervalsAttribute
    | NumericIntervalsAttribute
)


class PmmlTaskParser:
    def __init__(self, xml_string: str):
        self.root: Element = ET.fromstring(xml_string)
        self.ns: dict[str, str] = {"pmml": "http://www.dmg.org/PMML-4_0"}

    def parse(self) -> list[Attribute]:
        attributes: list[Attribute] = []

        for node in self.root.findall(".//pmml:DerivedField", self.ns):
            name: str = node.attrib.get("name", "")
            try:
                # Try each parser in order
                parsers = [
                    self._parse_nominal_enum,
                    self._parse_numeric_intervals,
                    self._parse_equidistant,
                    self._parse_equifrequent,
                    self._parse_equisized,
                    self._parse_simple,
                ]

                for parser in parsers:
                    attr = parser(node, name)
                    if attr is not None:
                        attributes.append(attr)
                        break
            except Exception as e:
                print(f"Failed to parse {name}: {e}")

        return attributes

    def _parse_simple(self, node: Element, name: str) -> SimpleAttribute | None:
        field_column = node.find(".//pmml:MapValues/pmml:FieldColumnPair", self.ns)
        if field_column is not None and len(field_column) == 0:
            field_id = self._to_int(field_column.attrib.get("field"))
            if field_id is not None:
                return SimpleAttribute(name=name, field_id=field_id)
        return None

    def _parse_nominal_enum(self, node: Element, name: str) -> NominalEnumerationAttribute | None:
        if node.find(".//pmml:MapValues/pmml:InlineTable/pmml:row", self.ns) is None:
            return None
        field_column = node.find(".//pmml:MapValues/pmml:FieldColumnPair", self.ns)
        if field_column is None or len(field_column) > 0:
            return None

        field_id = self._to_int(field_column.attrib.get("field")) or 0
        rows = node.findall(".//pmml:MapValues/pmml:InlineTable/pmml:row", self.ns)
        raw_bins: dict[str, list[str]] = defaultdict(list)

        for row in rows:
            column = row.find("pmml:column", self.ns)
            field = row.find("pmml:field", self.ns)
            if column is not None and field is not None and column.text and field.text:
                raw_bins[field.text].append(column.text)

        bins = [{bin_label: items} for bin_label, items in raw_bins.items()]
        return NominalEnumerationAttribute(name=name, field_id=field_id, bins=bins)

    def _parse_equidistant(self, node: Element, name: str) -> EquidistantIntervalsAttribute | None:
        discretize = node.find("pmml:Discretize", self.ns)
        if discretize is None:
            return None

        field_id = self._to_int(discretize.attrib.get("field"))
        bins = self._get_extension(discretize, "bins", int)
        min_value = self._get_extension(discretize, "min_value", float)
        max_value = self._get_extension(discretize, "max_value", float)
        algorithm = self._get_extension(discretize, "algorithm", str)

        if (
            algorithm == "equidistant-intervals"
            and field_id is not None
            and bins is not None
            and min_value is not None
            and max_value is not None
        ):
            return EquidistantIntervalsAttribute(
                name=name, field_id=field_id, bins=bins, min_value=min_value, max_value=max_value
            )
        return None

    def _parse_equifrequent(self, node: Element, name: str) -> EquifrequentIntervalsAttribute | None:
        discretize = node.find("pmml:Discretize", self.ns)
        if discretize is None:
            return None

        field_id = self._to_int(discretize.attrib.get("field"))
        bins = self._get_extension(discretize, "bins", int)
        algorithm = self._get_extension(discretize, "algorithm", str)

        # Parse cutpoints from extensions
        cutpoints = []
        for ext in discretize.findall("pmml:Extension", self.ns):
            if ext.attrib.get("name", "").startswith("cutpoint_"):
                try:
                    cutpoint = float(ext.attrib.get("value", ""))
                    cutpoints.append(cutpoint)
                except ValueError:
                    continue

        if algorithm == "equifrequent-intervals" and field_id is not None and bins is not None and cutpoints:
            return EquifrequentIntervalsAttribute(name=name, field_id=field_id, bins=bins, cutpoints=sorted(cutpoints))
        return None

    def _parse_equisized(self, node: Element, name: str) -> EquisizedIntervalsAttribute | None:
        discretize = node.find("pmml:Discretize", self.ns)
        if discretize is None:
            return None

        field_id = self._to_int(discretize.attrib.get("field"))
        support = self._get_extension(discretize, "support", float)
        min_value = self._get_extension(discretize, "min_value", float)
        max_value = self._get_extension(discretize, "max_value", float)
        algorithm = self._get_extension(discretize, "algorithm", str)

        if (
            algorithm == "equisized-intervals"
            and field_id is not None
            and support is not None
            and min_value is not None
            and max_value is not None
        ):
            return EquisizedIntervalsAttribute(
                name=name, field_id=field_id, support=support, min_value=min_value, max_value=max_value
            )
        return None

    def _parse_numeric_intervals(self, node: Element, name: str) -> NumericIntervalsAttribute | None:
        discretize = node.find("pmml:Discretize", self.ns)
        if discretize is None:
            return None

        field_id = self._to_int(discretize.attrib.get("field"))
        if field_id is None:
            return None

        bins_by_value: dict[str, list[NumericIntervalsAttribute.Interval]] = defaultdict(list)

        for bin_node in discretize.findall("pmml:DiscretizeBin", self.ns):
            bin_value = bin_node.attrib.get("binValue", "")
            for interval_node in bin_node.findall("pmml:Interval", self.ns):
                from_value = self._to_float(interval_node.attrib.get("leftMargin")) or float("-inf")
                to_value = self._to_float(interval_node.attrib.get("rightMargin")) or float("inf")
                closure = interval_node.attrib.get("closure", "closedClosed")

                from_inclusive = closure in ["closedClosed", "closedOpen"]
                to_inclusive = closure in ["closedClosed", "openClosed"]

                bins_by_value[bin_value].append(
                    NumericIntervalsAttribute.Interval(
                        from_value=from_value,
                        from_inclusive=from_inclusive,
                        to_value=to_value,
                        to_inclusive=to_inclusive,
                    )
                )

        bins = [NumericIntervalsAttribute.Bin(bin_value=k, intervals=v) for k, v in sorted(bins_by_value.items())]

        return NumericIntervalsAttribute(name=name, field_id=field_id, bins=bins)

    def _get_extension(self, node: Element, name: str, cast_type: type) -> str | int | float | None:
        for ext in node.findall("pmml:Extension", self.ns):
            if ext.attrib.get("name") == name:
                try:
                    value = ext.attrib.get("value")
                    if value is not None:
                        return cast_type(value)
                except (ValueError, TypeError):
                    continue
        return None

    @staticmethod
    def _to_int(value: str | None) -> int | None:
        if value is None:
            return None
        try:
            return int(value)
        except ValueError:
            return None

    @staticmethod
    def _to_float(value: str | None) -> float | None:
        if value is None:
            return None
        try:
            return float(value)
        except ValueError:
            return None


# Minimal example
if __name__ == "__main__":
    # Sample XML with numeric intervals
    xml_payload = """<?xml version="1.0" encoding="UTF-8"?>
<PMML version="4.2" xmlns="http://www.dmg.org/PMML-4_0">
    <Header copyright="Example"/>
    <DataDictionary>
        <DataField name="numeric_column" optype="continuous" dataType="double"/>
    </DataDictionary>
    <TransformationDictionary>
        <DerivedField name="binned_numeric" optype="categorical" dataType="string">
            <Discretize field="4" defaultValue="unknown">
                <DiscretizeBin binValue="low">
                    <Interval closure="closedOpen" leftMargin="0" rightMargin="10"/>
                </DiscretizeBin>
                <DiscretizeBin binValue="medium">
                    <Interval closure="closedOpen" leftMargin="10" rightMargin="20"/>
                </DiscretizeBin>
                <DiscretizeBin binValue="high">
                    <Interval closure="closedClosed" leftMargin="20" rightMargin="30"/>
                </DiscretizeBin>
            </Discretize>
        </DerivedField>
    </TransformationDictionary>
</PMML>"""

    # Parse the XML
    parser = PmmlTaskParser(xml_payload)
    attributes = parser.parse()

    print("Parsed Attributes:")
    for attr in attributes:
        print(f"- {attr.name} (type: {type(attr).__name__})")

    # Test transformations
    if attributes:
        attr = attributes[0]
        test_values = [5.0, 15.0, 25.0, 35.0, "12.5"]

        print(f"\nTesting {attr.name} transformations:")
        for value in test_values:
            result = attr.transform(value)
            print(f"  {value} -> {result}")

    # Example with manual attribute creation
    print("\n" + "=" * 50)
    print("Manual Attribute Examples:")

    # Create sample attributes manually
    sample_attributes: list[Attribute] = [
        SimpleAttribute(name="simple_field", field_id=1),
        NominalEnumerationAttribute(
            name="color_category", field_id=2, bins=[{"warm": ["red", "orange"]}, {"cool": ["blue", "green"]}]
        ),
        EquidistantIntervalsAttribute(name="age_groups", field_id=3, bins=3, min_value=0.0, max_value=90.0),
        NumericIntervalsAttribute(
            name="score_bins",
            field_id=4,
            bins=[
                NumericIntervalsAttribute.Bin(
                    bin_value="low",
                    intervals=[
                        NumericIntervalsAttribute.Interval(
                            from_value=0.0, from_inclusive=True, to_value=50.0, to_inclusive=False
                        )
                    ],
                ),
                NumericIntervalsAttribute.Bin(
                    bin_value="high",
                    intervals=[
                        NumericIntervalsAttribute.Interval(
                            from_value=50.0, from_inclusive=True, to_value=100.0, to_inclusive=True
                        )
                    ],
                ),
            ],
        ),
    ]

    # Test data
    test_data = {1: "hello", 2: "red", 3: 45.0, 4: 75.0}

    print("\nTransformation Results:")
    for attr in sample_attributes:
        if attr.field_id in test_data:
            value = test_data[attr.field_id]
            result = attr.transform(value)
            print(f"{attr.name}: {value} -> {result}")
