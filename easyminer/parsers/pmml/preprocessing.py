from decimal import Decimal
from typing import Protocol, cast, runtime_checkable

from pydantic import BaseModel, Field
from pydantic_xml import BaseXmlModel, attr, element

PMML_NS: str = "http://www.dmg.org/PMML-4_2"
NSMAP: dict[str, str] = {
    "": PMML_NS  # Default namespace
}


class Extension(BaseXmlModel, tag="Extension", nsmap=NSMAP):
    name: str = attr()
    value: str = attr()


class Interval(BaseXmlModel, tag="Interval", nsmap=NSMAP):
    closure: str = attr()
    left_margin: Decimal | None = attr(name="leftMargin", default=None)
    right_margin: Decimal | None = attr(name="rightMargin", default=None)


class DiscretizeBin(BaseXmlModel, tag="DiscretizeBin", nsmap=NSMAP):
    bin_value: str = attr(name="binValue")
    interval: Interval = element()


class Discretize(BaseXmlModel, tag="Discretize", nsmap=NSMAP):
    field: str = attr()
    extensions: list[Extension] = element(default_factory=list)
    discretize_bins: list[DiscretizeBin] = element(tag="DiscretizeBin", default_factory=list)


class Row(BaseXmlModel, tag="row", nsmap=NSMAP):
    column: str | None = element(default=None)
    field: str | None = element(default=None)


class InlineTable(BaseXmlModel, tag="InlineTable", nsmap=NSMAP):
    rows: list[Row] = element(tag="row", default_factory=list)


class FieldColumnPair(BaseXmlModel, tag="FieldColumnPair", nsmap=NSMAP):
    field: str = attr()
    column: str | None = attr(default=None)


class MapValues(BaseXmlModel, tag="MapValues", nsmap=NSMAP):
    output_column: str = attr(name="outputColumn")
    field_column_pair: FieldColumnPair = element()
    inline_table: InlineTable | None = element(default=None)


class DerivedField(BaseXmlModel, tag="DerivedField", nsmap=NSMAP):
    name: str = attr()
    discretize: Discretize | None = element(default=None)
    map_values: MapValues | None = element(default=None)


class TransformationDictionary(BaseXmlModel, nsmap=NSMAP, tag="TransformationDictionary"):
    derived_fields: list[DerivedField] = element(tag="DerivedField")

    @classmethod
    def from_xml_bytes(cls, xml_bytes: bytes) -> "TransformationDictionary":
        """Parse XML from bytes, supporting full XML documents with encoding declarations"""
        return cls.from_xml(xml_bytes)

    @classmethod
    def from_xml_string(cls, xml_string: str) -> "TransformationDictionary":
        """Parse XML from string, converting to bytes first to support encoding declarations"""
        return cls.from_xml(xml_string.encode("utf-8"))


# Transformation Protocol and Classes
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
    bins: list[dict[str, list[str]]] = Field(exclude=True)

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
    cutpoints: list[float] = Field(exclude=True)

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
        intervals: list["NumericIntervalsAttribute.Interval"] = Field(exclude=True)

    name: str
    field_id: int
    bins: list["NumericIntervalsAttribute.Bin"] = Field(exclude=True)

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


def create_attribute_from_pmml(
    derived_field: DerivedField,
    min_value: float | None = None,
    max_value: float | None = None,
    cutpoints: list[float] | None = None,
) -> Attribute:
    """Convert PMML DerivedField to transformation attribute with optional overrides"""
    if derived_field.discretize:
        field_id = int(derived_field.discretize.field)
    elif derived_field.map_values:
        field_id = int(derived_field.map_values.field_column_pair.field)
    else:
        raise ValueError("DerivedField must have either discretize or map_values")

    if derived_field.map_values:
        # Handle MapValues (nominal enumeration or simple attribute)
        if derived_field.map_values.inline_table:
            # Nominal enumeration
            bins: list[dict[str, list[str]]] = []
            value_map: dict[str, list[str]] = {}
            for row in derived_field.map_values.inline_table.rows:
                if row.column and row.field:
                    if row.field not in value_map:
                        value_map[row.field] = []
                    value_map[row.field].append(row.column)

            for bin_label, values in value_map.items():
                bins.append({bin_label: values})

            return NominalEnumerationAttribute(name=derived_field.name, field_id=field_id, bins=bins)
        else:
            # Simple attribute
            return SimpleAttribute(name=derived_field.name, field_id=field_id)

    elif derived_field.discretize:
        # Check for extensions to determine algorithm type
        extensions = {ext.name: ext.value for ext in derived_field.discretize.extensions}

        if "algorithm" in extensions:
            if extensions["algorithm"] == "equifrequent-intervals":
                return EquifrequentIntervalsAttribute(
                    name=derived_field.name,
                    field_id=field_id,
                    bins=int(extensions.get("bins", 5)),
                    cutpoints=cutpoints or [],
                )
            elif extensions["algorithm"] == "equisized-intervals":
                # Require leftMargin and rightMargin to be present, don't assume defaults
                if "leftMargin" not in extensions or "rightMargin" not in extensions:
                    raise ValueError(
                        "equisized-intervals algorithm requires leftMargin and rightMargin extensions "
                        + f"for field {derived_field.name}"
                    )

                return EquisizedIntervalsAttribute(
                    name=derived_field.name,
                    field_id=field_id,
                    support=float(extensions.get("support", 0.2)),
                    min_value=min_value or float(extensions["leftMargin"]),
                    max_value=max_value or float(extensions["rightMargin"]),
                )
            else:
                # Unsupported algorithm
                raise NotImplementedError(
                    f"Unsupported discretization algorithm '{extensions['algorithm']}' for field {derived_field.name}"
                )

        elif derived_field.discretize.discretize_bins:
            # Handle explicit bins (equidistant or numeric intervals)
            discretize_bins = derived_field.discretize.discretize_bins

            # Check if all bins have valid intervals
            if all(
                bin_item.interval.left_margin is not None and bin_item.interval.right_margin is not None
                for bin_item in discretize_bins
            ):
                # Check if this is equidistant intervals:
                # 1. All bins must have unique bin values (no overlapping bin labels)
                # 2. Intervals must be adjacent with no gaps
                bin_values = [bin_item.bin_value for bin_item in discretize_bins]
                if len(set(bin_values)) == len(bin_values) and len(discretize_bins) > 1:
                    # Sort bins by left margin to check if they are consecutive
                    # Since we already checked that left_margin is not None, we can safely cast
                    sorted_bins = sorted(
                        discretize_bins,
                        key=lambda b: float(cast(Decimal, b.interval.left_margin)),
                    )

                    # Check if intervals are consecutive with no gaps
                    is_consecutive = True
                    for i in range(len(sorted_bins) - 1):
                        # We know these are not None due to the check above
                        assert sorted_bins[i].interval.right_margin is not None
                        assert sorted_bins[i + 1].interval.left_margin is not None
                        current_right = float(cast(Decimal, sorted_bins[i].interval.right_margin))
                        next_left = float(cast(Decimal, sorted_bins[i + 1].interval.left_margin))
                        if abs(current_right - next_left) > 1e-10:  # Allow for small floating point errors
                            is_consecutive = False
                            break

                    if is_consecutive:
                        # We know these are not None due to the check above
                        assert sorted_bins[0].interval.left_margin is not None
                        assert sorted_bins[-1].interval.right_margin is not None
                        parsed_min = float(sorted_bins[0].interval.left_margin)
                        parsed_max = float(sorted_bins[-1].interval.right_margin)

                        return EquidistantIntervalsAttribute(
                            name=derived_field.name,
                            field_id=field_id,
                            bins=len(discretize_bins),
                            min_value=min_value or parsed_min,
                            max_value=max_value or parsed_max,
                        )

                # If not equidistant, treat as numeric intervals
                # Group bins by bin_value (multiple intervals can have the same bin value)
                bin_groups: dict[str, list[NumericIntervalsAttribute.Interval]] = {}
                for bin_item in discretize_bins:
                    if bin_item.bin_value not in bin_groups:
                        bin_groups[bin_item.bin_value] = []

                    if bin_item.interval.left_margin is not None and bin_item.interval.right_margin is not None:
                        interval = NumericIntervalsAttribute.Interval(
                            from_value=float(bin_item.interval.left_margin),
                            from_inclusive=bin_item.interval.closure in ["closedOpen", "closedClosed"],
                            to_value=float(bin_item.interval.right_margin),
                            to_inclusive=bin_item.interval.closure in ["openClosed", "closedClosed"],
                        )
                        bin_groups[bin_item.bin_value].append(interval)

                attribute_bins: list[NumericIntervalsAttribute.Bin] = []
                for bin_value, intervals in bin_groups.items():
                    attribute_bins.append(NumericIntervalsAttribute.Bin(bin_value=bin_value, intervals=intervals))

                return NumericIntervalsAttribute(name=derived_field.name, field_id=field_id, bins=attribute_bins)
            else:
                # Some bins have missing interval data
                raise ValueError(
                    f"Some bins in field {derived_field.name} have missing leftMargin or rightMargin values"
                )

    raise NotImplementedError(
        f"DerivedField {derived_field.name} does not have a valid discretize or map_values configuration"
    )
