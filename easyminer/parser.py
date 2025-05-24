from collections import defaultdict
from xml.etree.ElementTree import Element

import defusedxml.ElementTree as ET
from pydantic import BaseModel


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
        for bin in self.bins:
            for bin_label, items in bin.items():
                if value in items:
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
            raise NotImplementedError()  # TODO: improve

        width = (self.max_value - self.min_value) / self.bins

        for i in range(self.bins):
            lower = self.min_value + i * width
            upper = lower + width
            if lower <= value < upper or (i == self.bins - 1 and value == upper):
                return f"[{lower}, {upper})"
        return "out_of_range"


class EquifrequentIntervalsAttribute(BaseModel):
    name: str
    field_id: int
    bins: int
    cutpoints: list[float]

    def transform(self, value: str | float) -> str:
        if isinstance(value, str):
            raise NotImplementedError()  # TODO: improve

        for i, cut in enumerate(self.cutpoints):
            if value < cut:
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
            raise NotImplementedError()  # TODO: improve

        if self.max_value <= self.min_value:
            raise ValueError("Invalid min/max values")

        range_ = self.max_value - self.min_value
        bin_width = range_ * float(self.support)
        if bin_width <= 0:
            raise ValueError("Invalid bin width")

        bin_index = int((value - self.min_value) / bin_width)
        max_bin = int(1 / self.support) - 1  # last bin inclusive
        bin_index = min(max(bin_index, 0), max_bin)
        bin_start = self.min_value + bin_width * bin_index
        bin_end = bin_start + bin_width
        return f"[{bin_start}, {bin_end})"


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
            raise NotImplementedError()  # TODO: improve

        for bin in self.bins:
            for interval in bin.intervals:
                if interval.contains(value):
                    return bin.bin_value
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
    xml_payload = """<?xml version="1.0" encoding="UTF-8"?>
<PMML version="4.2" xmlns="http://www.dmg.org/PMML-4_2">
	<Header copyright="NumericIntervals Example"/>
	<DataDictionary>
		<DataField name="numeric_column" optype="continuous" dataType="double"/>
	</DataDictionary>
	<TransformationDictionary>
		<DerivedField name="binned_numeric_column" optype="categorical" dataType="string">
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

    # Instantiate the parser
    parser = PmmlTaskParser(xml_payload)

    # Parse the XML
    parsed_attributes = parser.parse()

    # Print the result
    print("Parsed Attributes:")
    print(parsed_attributes)
    attr = parsed_attributes[0]
    print(attr.name)
    print(attr.transform(float("123.45")))

    # # Simulated user-uploaded data row
    # sample_row = {
    #     1: "green",
    #     2: float("3.5"),
    #     3: float("8.7"),
    #     4: float("10.0"),
    #     5: float("2.3"),
    #     6: float("12.1"),
    # }
    #
    # # Define attributes
    # attrs = [
    #     SimpleAttribute(name="simple", field_id=1),
    #     NominalEnumerationAttribute(
    #         name="nominal_enum", field_id=1, bins=[{"color": ["red", "green"]}, {"other": ["blue", "yellow"]}]
    #     ),
    #     NumericIntervalsAttribute(
    #         name="numeric_intervals",
    #         field_id=5,
    #         bins=[
    #             NumericIntervalsAttribute.Bin(
    #                 bin_value="low",
    #                 intervals=[
    #                     NumericIntervalsAttribute.Interval(
    #                         from_value=0.0, from_inclusive=True, to_value=3.0, to_inclusive=True
    #                     )
    #                 ],
    #             ),
    #             NumericIntervalsAttribute.Bin(
    #                 bin_value="high",
    #                 intervals=[
    #                     NumericIntervalsAttribute.Interval(
    #                         from_value=3.0, from_inclusive=False, to_value=10.0, to_inclusive=False
    #                     )
    #                 ],
    #             ),
    #         ],
    #     ),
    # ]
    #
    # # Apply transformations
    # for attr in attrs:
    #     try:
    #         value = sample_row.get(attr.field_id)
    #         if value is None:
    #             print(f"{attr.name} not found in sample row")
    #             continue
    #         transformed = attr.transform(value)
    #         print(f"{attr.name} -> {transformed}")
    #     except Exception as e:
    #         print(f"{attr.name} transform failed: {e}")
    #
    # print("---")
    #
    # from collections import defaultdict
    #
    # # Simulate dataset: list of rows
    # dataset = [
    #     {1: "red", 2: float("1.0"), 3: float("3.0"), 4: float("1.0"), 5: float("2.0")},
    #     {1: "blue", 2: float("2.0"), 3: float("6.0"), 4: float("6.0"), 5: float("5.0")},
    #     {1: "green", 2: float("3.0"), 3: float("9.0"), 4: float("11.0"), 5: float("7.5")},
    # ]
    #
    # # 1. SimpleAttribute
    # simple = SimpleAttribute(name="simple", field_id=1)
    #
    # # 2. NominalEnumerationAttribute
    # nominal = NominalEnumerationAttribute(
    #     name="nom_enum",
    #     field_id=1,
    #     bins=[
    #         {"color1": ["red", "green"]},
    #         {"color2": ["blue"]},
    #     ],
    # )
    #
    # # 3. EquidistantIntervalsAttribute — must include min and max
    # equidistant = EquidistantIntervalsAttribute(
    #     name="eqdist",
    #     field_id=2,
    #     bins=3,
    #     min_value=float("1.0"),
    #     max_value=float("3.0"),
    # )
    #
    # # 4. EquifrequentIntervalsAttribute — must include cutpoints
    # equifrequent = EquifrequentIntervalsAttribute(
    #     name="eqfreq",
    #     field_id=3,
    #     bins=3,
    #     cutpoints=[float("4.5"), float("7.5")],  # splits values: <=4.5, 4.5–7.5, >7.5
    # )
    #
    # # 5. EquisizedIntervalsAttribute — must include min and max
    # equisized = EquisizedIntervalsAttribute(
    #     name="eqsize",
    #     field_id=4,
    #     support=float("5.0"),
    #     min_value=float("1.0"),
    #     max_value=float("11.0"),
    # )
    #
    # # 6. NumericIntervalsAttribute — uses hand-crafted intervals
    # numeric = NumericIntervalsAttribute(
    #     name="numint",
    #     field_id=5,
    #     bins=[
    #         NumericIntervalsAttribute.Bin(
    #             bin_value="low",
    #             intervals=[
    #                 NumericIntervalsAttribute.Interval(
    #                     from_value=1.0, from_inclusive=True, to_value=4.0, to_inclusive=False
    #                 )
    #             ],
    #         ),
    #         NumericIntervalsAttribute.Bin(
    #             bin_value="medium",
    #             intervals=[
    #                 NumericIntervalsAttribute.Interval(
    #                     from_value=4.0, from_inclusive=True, to_value=6.0, to_inclusive=True
    #                 )
    #             ],
    #         ),
    #         NumericIntervalsAttribute.Bin(
    #             bin_value="high",
    #             intervals=[
    #                 NumericIntervalsAttribute.Interval(
    #                     from_value=6.0, from_inclusive=False, to_value=10.0, to_inclusive=True
    #                 )
    #             ],
    #         ),
    #     ],
    # )
    #
    # attributes = [simple, nominal, equidistant, equifrequent, equisized, numeric]
    #
    # # Run transformation
    # for row in dataset:
    #     for col in row:
    #         for attr in attributes:
    #             result = attr.transform(col)
    #             print(f"{attr.name}({row[attr.field_id]}) -> {result}")
    #     print("--------------")
