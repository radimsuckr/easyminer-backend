from decimal import Decimal
from math import ceil
from typing import Protocol, cast, runtime_checkable

from pydantic import BaseModel, Field
from pydantic_xml import BaseXmlModel, attr, element

from easyminer.preprocessing.smoothing import (
    AttributeInterval,
    IntervalBorder,
    ValueFrequency,
    finalize_intervals,
    format_interval,
    init_equifrequent_intervals,
    init_equisized_intervals,
    smooth_equifrequent,
    smooth_equisized,
)

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
                return format_interval(
                    IntervalBorder(lower, inclusive=True),
                    IntervalBorder(upper, inclusive=False),
                )
        return "out_of_range"


class EquifrequentIntervalsAttribute(BaseModel):
    name: str
    field_id: int
    bins: int
    intervals: list[AttributeInterval] | None = Field(default=None, exclude=True)

    @classmethod
    def build(
        cls,
        name: str,
        field_id: int,
        bins_count: int,
        values: list[ValueFrequency],
        unique_values_count: int,
        dataset_size: int,
    ) -> "EquifrequentIntervalsAttribute":
        """Build equifrequent intervals from sorted ascending value frequencies."""
        max_frequency = ceil(dataset_size / bins_count)
        intervals = init_equifrequent_intervals(values, bins_count, unique_values_count, dataset_size)
        if len(intervals) > 1:
            values_desc = list(reversed(values))
            smooth_equifrequent(intervals, values_desc, max_frequency)
            finalize_intervals(intervals)
        return cls(name=name, field_id=field_id, bins=bins_count, intervals=intervals)

    def transform(self, value: str | float) -> str:
        if self.intervals is None:
            raise RuntimeError("Intervals not built — call build() with value frequencies first")
        if isinstance(value, str):
            try:
                numeric_value = float(value)
            except ValueError:
                return "invalid_value"
        else:
            numeric_value = value

        for interval in self.intervals:
            from_ok = (
                numeric_value >= interval.from_border.value
                if interval.from_border.inclusive
                else numeric_value > interval.from_border.value
            )
            to_ok = (
                numeric_value <= interval.to_border.value
                if interval.to_border.inclusive
                else numeric_value < interval.to_border.value
            )
            if from_ok and to_ok:
                return format_interval(interval.from_border, interval.to_border)
        return "out_of_range"


class EquisizedIntervalsAttribute(BaseModel):
    name: str
    field_id: int
    support: float
    intervals: list[AttributeInterval] | None = Field(default=None, exclude=True)

    @classmethod
    def build(
        cls,
        name: str,
        field_id: int,
        support: float,
        values: list[ValueFrequency],
        dataset_size: int,
    ) -> "EquisizedIntervalsAttribute":
        """Build equisized intervals from sorted ascending value frequencies."""
        min_frequency = dataset_size * support
        intervals = init_equisized_intervals(values, min_frequency)
        if len(intervals) > 1:
            values_desc = list(reversed(values))
            smooth_equisized(intervals, values_desc, min_frequency)
            finalize_intervals(intervals)
        return cls(name=name, field_id=field_id, support=support, intervals=intervals)

    def transform(self, value: str | float) -> str:
        if self.intervals is None:
            raise RuntimeError("Intervals not built — call build() with value frequencies first")
        if isinstance(value, str):
            try:
                numeric_value = float(value)
            except ValueError:
                return "invalid_value"
        else:
            numeric_value = value

        for interval in self.intervals:
            from_ok = (
                numeric_value >= interval.from_border.value
                if interval.from_border.inclusive
                else numeric_value > interval.from_border.value
            )
            to_ok = (
                numeric_value <= interval.to_border.value
                if interval.to_border.inclusive
                else numeric_value < interval.to_border.value
            )
            if from_ok and to_ok:
                return format_interval(interval.from_border, interval.to_border)
        return "out_of_range"


class NumericIntervalsAttribute(BaseModel):
    class Interval(BaseModel):
        from_value: float
        from_inclusive: bool
        to_value: float
        to_inclusive: bool

        def contains(self, value: float) -> bool:
            if self.from_value != float("-inf"):
                if self.from_inclusive:
                    if value < self.from_value:
                        return False
                else:
                    if value <= self.from_value:
                        return False
            if self.to_value != float("inf"):
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
                )
            elif extensions["algorithm"] == "equisized-intervals":
                return EquisizedIntervalsAttribute(
                    name=derived_field.name,
                    field_id=field_id,
                    support=float(extensions.get("support", 0.2)),
                )
            elif extensions["algorithm"] == "equidistant-intervals":
                return EquidistantIntervalsAttribute(
                    name=derived_field.name,
                    field_id=field_id,
                    bins=int(extensions.get("bins", 5)),
                    min_value=min_value or float(extensions.get("leftMargin", 0)),
                    max_value=max_value or float(extensions.get("rightMargin", 0)),
                )
            else:
                raise NotImplementedError(
                    f"Unsupported discretization algorithm '{extensions['algorithm']}' for field {derived_field.name}"
                )

        elif derived_field.discretize.discretize_bins:
            # Handle explicit bins (equidistant or numeric intervals)
            discretize_bins = derived_field.discretize.discretize_bins

            # Check if all bins have finite intervals (for equidistant detection)
            all_finite = all(
                bin_item.interval.left_margin is not None and bin_item.interval.right_margin is not None
                for bin_item in discretize_bins
            )

            if all_finite:
                # Check if this is equidistant intervals:
                # 1. All bins must have unique bin values (no overlapping bin labels)
                # 2. Intervals must be adjacent with no gaps
                bin_values = [bin_item.bin_value for bin_item in discretize_bins]
                if len(set(bin_values)) == len(bin_values) and len(discretize_bins) > 1:
                    sorted_bins = sorted(
                        discretize_bins,
                        key=lambda b: float(cast(Decimal, b.interval.left_margin)),
                    )

                    is_consecutive = True
                    for i in range(len(sorted_bins) - 1):
                        assert sorted_bins[i].interval.right_margin is not None
                        assert sorted_bins[i + 1].interval.left_margin is not None
                        current_right = float(cast(Decimal, sorted_bins[i].interval.right_margin))
                        next_left = float(cast(Decimal, sorted_bins[i + 1].interval.left_margin))
                        if abs(current_right - next_left) > 1e-10:
                            is_consecutive = False
                            break

                    if is_consecutive:
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

            # Numeric intervals (supports open-ended intervals with -inf/+inf)
            bin_groups: dict[str, list[NumericIntervalsAttribute.Interval]] = {}
            for bin_item in discretize_bins:
                if bin_item.bin_value not in bin_groups:
                    bin_groups[bin_item.bin_value] = []

                from_value = (
                    float(bin_item.interval.left_margin) if bin_item.interval.left_margin is not None else float("-inf")
                )
                to_value = (
                    float(bin_item.interval.right_margin)
                    if bin_item.interval.right_margin is not None
                    else float("inf")
                )

                interval = NumericIntervalsAttribute.Interval(
                    from_value=from_value,
                    from_inclusive=bin_item.interval.closure in ["closedOpen", "closedClosed"],
                    to_value=to_value,
                    to_inclusive=bin_item.interval.closure in ["openClosed", "closedClosed"],
                )
                bin_groups[bin_item.bin_value].append(interval)

            attribute_bins: list[NumericIntervalsAttribute.Bin] = []
            for bin_value, intervals_list in bin_groups.items():
                attribute_bins.append(NumericIntervalsAttribute.Bin(bin_value=bin_value, intervals=intervals_list))

            return NumericIntervalsAttribute(name=derived_field.name, field_id=field_id, bins=attribute_bins)

    raise NotImplementedError(
        f"DerivedField {derived_field.name} does not have a valid discretize or map_values configuration"
    )
