from collections import defaultdict
from xml.etree.ElementTree import Element

import defusedxml.ElementTree as ET
from pydantic import BaseModel


class SimpleAttribute(BaseModel):
    name: str
    field_id: int


class NominalEnumerationAttribute(BaseModel):
    name: str
    field_id: int
    bins: list[dict[str, list[str]]]


class EquidistantIntervalsAttribute(BaseModel):
    name: str
    field_id: int
    bins: int


class EquifrequentIntervalsAttribute(BaseModel):
    name: str
    field_id: int
    bins: int


class EquisizedIntervalsAttribute(BaseModel):
    name: str
    field_id: int
    support: float


class NumericIntervalsAttribute(BaseModel):
    class Interval(BaseModel):
        from_value: float
        from_inclusive: bool
        to_value: float
        to_inclusive: bool

    class Bin(BaseModel):
        bin_value: str
        intervals: list["NumericIntervalsAttribute.Interval"]

    name: str
    field_id: int
    bins: list["NumericIntervalsAttribute.Bin"]


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
        self.ns: dict[str, str] = {"pmml": "http://www.dmg.org/PMML-4_2"}

    def parse(self) -> list[Attribute]:
        attributes: list[Attribute] = []

        for node in self.root.findall(".//pmml:DerivedField", self.ns):
            name: str = node.attrib.get("name", "")
            try:
                for method in [
                    self._parse_nominal_enum,
                    self._parse_numeric_intervals,
                    self._parse_equidistant,
                    self._parse_equifrequent,
                    self._parse_equisized,
                    self._parse_simple,
                ]:
                    attr: Attribute | None = method(node, name)
                    if attr:
                        attributes.append(attr)
                        break
            except Exception as e:
                print(f"Failed to parse {name}: {e}")

        return attributes

    def _parse_simple(self, node: Element, name: str) -> SimpleAttribute | None:
        field_column: Element | None = node.find(".//pmml:MapValues/pmml:FieldColumnPair", self.ns)
        if field_column is not None and len(field_column) == 0:
            field_id: int | None = self._to_int(field_column.attrib.get("field"))
            if field_id is not None:
                return SimpleAttribute(name=name, field_id=field_id)
        return None

    def _parse_nominal_enum(self, node: Element, name: str) -> NominalEnumerationAttribute | None:
        if node.find(".//pmml:MapValues/pmml:InlineTable/pmml:row", self.ns) is None:
            return None
        field_column: Element | None = node.find(".//pmml:MapValues/pmml:FieldColumnPair", self.ns)
        if field_column is None or len(field_column) > 0:
            return None

        field_id: int = self._to_int(field_column.attrib.get("field")) or 0
        rows: list[Element] = node.findall(".//pmml:MapValues/pmml:InlineTable/pmml:row", self.ns)
        raw_bins: dict[str, list[str]] = defaultdict(list)

        for row in rows:
            column = row.find("pmml:column", self.ns)
            field = row.find("pmml:field", self.ns)
            if column is not None and field is not None:
                raw_bins[field.text].append(column.text)

        bins: list[dict[str, str | list[str]]] = [{"value": k, "items": v} for k, v in raw_bins.items()]
        return NominalEnumerationAttribute(name=name, field_id=field_id, bins=bins)

    def _parse_equidistant(self, node: Element, name: str) -> EquidistantIntervalsAttribute | None:
        return self._parse_interval_with_bins(node, name, "equidistant-intervals", EquidistantIntervalsAttribute)

    def _parse_equifrequent(self, node: Element, name: str) -> EquifrequentIntervalsAttribute | None:
        return self._parse_interval_with_bins(node, name, "equifrequent-intervals", EquifrequentIntervalsAttribute)

    def _parse_equisized(self, node: Element, name: str) -> EquisizedIntervalsAttribute | None:
        discretize: Element | None = node.find("pmml:Discretize", self.ns)
        if discretize is None:
            return None

        field_id: int | None = self._to_int(discretize.attrib.get("field"))
        support: float | None = self._get_extension(discretize, "support", float)
        algorithm: str | None = self._get_extension(discretize, "algorithm", str)

        if algorithm == "equisized-intervals" and support is not None:
            return EquisizedIntervalsAttribute(name=name, field_id=field_id or 0, support=support)

        return None

    def _parse_interval_with_bins(
        self,
        node: Element,
        name: str,
        algo_name: str,
        cls: type[EquidistantIntervalsAttribute] | type[EquifrequentIntervalsAttribute],
    ) -> Attribute | None:
        discretize: Element | None = node.find("pmml:Discretize", self.ns)
        if discretize is None:
            return None

        field_id: int | None = self._to_int(discretize.attrib.get("field"))
        bins: int | None = self._get_extension(discretize, "bins", int)
        algorithm: str | None = self._get_extension(discretize, "algorithm", str)

        if algorithm == algo_name and field_id is not None and bins is not None:
            return cls(name=name, field_id=field_id, bins=bins)

        return None

    def _parse_numeric_intervals(self, node: Element, name: str) -> NumericIntervalsAttribute | None:
        discretize: Element | None = node.find("pmml:Discretize", self.ns)
        if discretize is None:
            return None

        field_id: int | None = self._to_int(discretize.attrib.get("field"))
        if field_id is None:
            return None

        bins_by_value: dict[str, list[NumericIntervalsAttribute.Interval]] = defaultdict(list)

        for bin_node in discretize.findall("pmml:DiscretizeBin", self.ns):
            bin_value: str = bin_node.attrib.get("binValue", "")
            for interval_node in bin_node.findall("pmml:Interval", self.ns):
                from_value: float = self._to_float(interval_node.attrib.get("leftMargin")) or float("-inf")
                to_value: float = self._to_float(interval_node.attrib.get("rightMargin")) or float("inf")
                closure: str = interval_node.attrib.get("closure", "closedClosed")

                from_inclusive: bool = "closed" in closure or closure == "closedOpen"
                to_inclusive: bool = "closed" in closure or closure == "openClosed"

                bins_by_value[bin_value].append(
                    NumericIntervalsAttribute.Interval(
                        from_value=from_value,
                        from_inclusive=from_inclusive,
                        to_value=to_value,
                        to_inclusive=to_inclusive,
                    )
                )

        bins: list[NumericIntervalsAttribute.Bin] = [
            NumericIntervalsAttribute.Bin(bin_value=k, intervals=v)
            for k, v in sorted(bins_by_value.items(), key=lambda x: x[0])
        ]

        return NumericIntervalsAttribute(name=name, field_id=field_id, bins=bins)

    def _get_extension(self, node: Element, name: str, cast: type) -> str | int | float | None:
        for ext in node.findall("pmml:Extension", self.ns):
            if ext.attrib.get("name") == name:
                try:
                    return cast(ext.attrib.get("value"))
                except Exception:
                    return None
        return None

    @staticmethod
    def _to_int(value: str | None) -> int | None:
        try:
            return int(value) if value is not None else None
        except ValueError:
            return None

    @staticmethod
    def _to_float(value: str | None) -> float | None:
        try:
            return float(value) if value is not None else None
        except ValueError:
            return None


if __name__ == "__main__":
    # Corrected XML payload with version 4.2 and an integer 'field' attribute
    xml_payload = """<?xml version="1.0" encoding="UTF-8"?>
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

    # Instantiate the parser
    parser = PmmlTaskParser(xml_payload)

    # Parse the XML
    parsed_attributes = parser.parse()

    # Print the result
    print("Parsed Attributes:")
    print(parsed_attributes)
