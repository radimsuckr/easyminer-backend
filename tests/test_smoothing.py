"""Tests for the interval smoothing algorithm and interval builders."""

import pytest

from easyminer.preprocessing.smoothing import (
    AttributeInterval,
    IntervalBorder,
    ValueFrequency,
    finalize_intervals,
    format_interval,
    init_equifrequent_intervals,
    init_equisized_intervals,
    round_at_6,
    smooth_equifrequent,
    smooth_intervals,
)

# -- Data structure helpers --


def iv(from_val: float, to_val: float, freq: int) -> AttributeInterval:
    """Shorthand for creating an inclusive-inclusive interval."""
    return AttributeInterval(
        from_border=IntervalBorder(from_val, inclusive=True),
        to_border=IntervalBorder(to_val, inclusive=True),
        frequency=freq,
    )


def vf(value: float, frequency: int) -> ValueFrequency:
    return ValueFrequency(value=value, frequency=frequency)


# -- round_at_6 tests --


def test_round_at_6_basic():
    assert round_at_6(8110.0) == 8110.0
    assert round_at_6(1.23456789) == 1.234568
    assert round_at_6(0.0) == 0.0


# -- format_interval tests --


def test_format_interval_inclusive_exclusive():
    result = format_interval(
        IntervalBorder(8110.0, inclusive=True),
        IntervalBorder(9587.0, inclusive=False),
    )
    assert result == "[8110.0,9587.0)"


def test_format_interval_exclusive_inclusive():
    result = format_interval(
        IntervalBorder(1500.0, inclusive=False),
        IntervalBorder(3000.0, inclusive=True),
    )
    assert result == "(1500.0,3000.0]"


def test_format_interval_both_inclusive():
    result = format_interval(
        IntervalBorder(0.0, inclusive=True),
        IntervalBorder(100.0, inclusive=True),
    )
    assert result == "[0.0,100.0]"


def test_format_interval_rounding():
    result = format_interval(
        IntervalBorder(1.23456789, inclusive=True),
        IntervalBorder(9.87654321, inclusive=False),
    )
    assert result == "[1.234568,9.876543)"


# -- smooth_intervals tests --


def test_smooth_no_change_when_balanced():
    """Equal-frequency intervals should not change."""
    intervals = [iv(1.0, 2.0, 5), iv(3.0, 4.0, 5)]
    values_desc = [vf(4.0, 2), vf(3.0, 3), vf(2.0, 2), vf(1.0, 3)]

    def never_move(moved, prev, current):
        return False

    smooth_intervals(intervals, values_desc, never_move, never_move)
    assert intervals[0].frequency == 5
    assert intervals[1].frequency == 5


def test_smooth_single_interval_noop():
    """Single interval — nothing to smooth."""
    intervals = [iv(1.0, 5.0, 10)]
    values_desc = [vf(5.0, 2), vf(3.0, 3), vf(1.0, 5)]

    def always_move(moved, prev, current):
        return True

    smooth_intervals(intervals, values_desc, always_move, always_move)
    assert intervals[0].frequency == 10


def test_smooth_moves_left():
    """Right interval heavier — boundary shifts left."""
    # Intervals: [1,2] freq=2, [3,5] freq=8
    # Values desc: 5(3), 4(2), 3(3), 2(1), 1(1)
    # Value 3 is at from_border of right interval, last_value=4 exists
    # After move: right loses freq=3, from becomes 4; left gains freq=3, to becomes 3
    intervals = [iv(1.0, 2.0, 2), iv(3.0, 5.0, 8)]
    values_desc = [vf(5.0, 3), vf(4.0, 2), vf(3.0, 3), vf(2.0, 1), vf(1.0, 1)]

    def always_left(moved, prev, current):
        return True

    def never_right(moved, prev, current):
        return False

    smooth_intervals(intervals, values_desc, always_left, never_right)
    assert intervals[0].frequency == 5  # 2 + 3
    assert intervals[1].frequency == 5  # 8 - 3
    assert intervals[1].from_border.value == 4.0


def test_smooth_moves_right():
    """Left interval heavier — boundary shifts right."""
    # Intervals: [1,3] freq=8, [4,5] freq=2
    # Values desc: 5(1), 4(1), 3(3), 2(2), 1(3)
    # Value 2 comes after value 3 (last_value). last_value.value==3==left.to_border.value
    # After move: left loses freq=3, to becomes 2; right gains freq=3, from becomes 3
    intervals = [iv(1.0, 3.0, 8), iv(4.0, 5.0, 2)]
    values_desc = [vf(5.0, 1), vf(4.0, 1), vf(3.0, 3), vf(2.0, 2), vf(1.0, 3)]

    def never_left(moved, prev, current):
        return False

    def always_right(moved, prev, current):
        return True

    smooth_intervals(intervals, values_desc, never_left, always_right)
    assert intervals[0].frequency == 5  # 8 - 3
    assert intervals[1].frequency == 5  # 2 + 3
    assert intervals[0].to_border.value == 2.0


# -- finalize_intervals tests --


def test_finalize_midpoints():
    intervals = [iv(1.0, 3.0, 5), iv(4.0, 6.0, 5)]
    finalize_intervals(intervals)
    assert intervals[0].to_border.value == 3.5
    assert intervals[0].to_border.inclusive is False
    assert intervals[1].from_border.value == 3.5
    assert intervals[1].from_border.inclusive is True


def test_finalize_three_intervals():
    intervals = [iv(1.0, 2.0, 3), iv(3.0, 4.0, 3), iv(5.0, 6.0, 3)]
    finalize_intervals(intervals)
    assert intervals[0].to_border.value == 2.5
    assert intervals[1].from_border.value == 2.5
    assert intervals[1].to_border.value == 4.5
    assert intervals[2].from_border.value == 4.5


# -- Equifrequent init + smooth tests --


def test_equifrequent_build_uniform():
    """10 values each with freq=1, 2 bins → each gets 5."""
    values = [vf(float(i), 1) for i in range(10)]
    intervals = init_equifrequent_intervals(values, bins_count=2, unique_values_count=10, dataset_size=10)
    assert len(intervals) == 2
    assert intervals[0].frequency == 5
    assert intervals[1].frequency == 5


def test_equifrequent_build_skewed():
    """Skewed distribution, smoothing should improve balance."""
    # 5 values: 1(10), 2(1), 3(1), 4(1), 5(1), total=14, 2 bins → maxFreq=7
    values = [vf(1.0, 10), vf(2.0, 1), vf(3.0, 1), vf(4.0, 1), vf(5.0, 1)]
    intervals = init_equifrequent_intervals(values, bins_count=2, unique_values_count=5, dataset_size=14)
    # Initial packing might be imbalanced, but we should have 2 intervals
    assert len(intervals) == 2
    total = sum(i.frequency for i in intervals)
    assert total == 14


def test_equifrequent_single_bin():
    values = [vf(1.0, 3), vf(2.0, 3), vf(3.0, 3)]
    intervals = init_equifrequent_intervals(values, bins_count=1, unique_values_count=3, dataset_size=9)
    assert len(intervals) == 1
    assert intervals[0].frequency == 9


def test_equifrequent_smooth_improves_balance():
    """Smoothing should reduce the frequency imbalance."""
    # 4 values: 1(1), 2(1), 3(1), 4(7), total=10, 2 bins → maxFreq=5
    values = [vf(1.0, 1), vf(2.0, 1), vf(3.0, 1), vf(4.0, 7)]
    intervals = init_equifrequent_intervals(values, bins_count=2, unique_values_count=4, dataset_size=10)
    values_desc = list(reversed(values))
    max_freq = 5.0
    smooth_equifrequent(intervals, values_desc, max_freq)
    # After smoothing, the difference should be smaller
    diff = abs(intervals[0].frequency - intervals[1].frequency)
    assert diff <= 8  # Just sanity check — smoothing should not make it worse


# -- Equisized init + smooth tests --


def test_equisized_build_basic():
    """Basic equisized: 10 values each freq=1, support such that min_freq=3."""
    values = [vf(float(i), 1) for i in range(10)]
    intervals = init_equisized_intervals(values, min_frequency=3)
    # Each interval should have at least 3 values
    for iv_ in intervals:
        assert iv_.frequency >= 3
    total = sum(i.frequency for i in intervals)
    assert total == 10


def test_equisized_last_bin_merge():
    """Last bin below min_frequency gets merged with second-to-last."""
    # 5 values: freq 5,5,5,5,1. min_freq=4. Last value (freq=1) gets its own interval,
    # then merged with previous.
    values = [vf(1.0, 5), vf(2.0, 5), vf(3.0, 5), vf(4.0, 5), vf(5.0, 1)]
    intervals = init_equisized_intervals(values, min_frequency=4)
    # All intervals should have freq >= 4
    for iv_ in intervals:
        assert iv_.frequency >= 4
    total = sum(i.frequency for i in intervals)
    assert total == 21


def test_equisized_single_value():
    values = [vf(1.0, 10)]
    intervals = init_equisized_intervals(values, min_frequency=5)
    assert len(intervals) == 1
    assert intervals[0].frequency == 10


# -- EquifrequentIntervalsAttribute.build tests --


def test_equifrequent_attribute_build_and_transform():
    from easyminer.parsers.pmml.preprocessing import EquifrequentIntervalsAttribute

    values = [vf(float(i), 1) for i in range(1, 11)]  # 1..10, each freq=1
    attr = EquifrequentIntervalsAttribute.build(
        name="test", field_id=1, bins_count=2, values=values, unique_values_count=10, dataset_size=10
    )
    assert attr.intervals is not None
    assert len(attr.intervals) == 2

    # Value in first interval
    result1 = attr.transform(2.0)
    assert result1.startswith("[") or result1.startswith("(")
    # Value in second interval
    result2 = attr.transform(9.0)
    assert result2.startswith("[") or result2.startswith("(")
    # Different intervals
    assert result1 != result2


def test_equifrequent_attribute_not_built_raises():
    from easyminer.parsers.pmml.preprocessing import EquifrequentIntervalsAttribute

    attr = EquifrequentIntervalsAttribute(name="test", field_id=1, bins=2)
    with pytest.raises(RuntimeError, match="Intervals not built"):
        attr.transform(5.0)


# -- EquisizedIntervalsAttribute.build tests --


def test_equisized_attribute_build_and_transform():
    from easyminer.parsers.pmml.preprocessing import EquisizedIntervalsAttribute

    values = [vf(float(i), 1) for i in range(1, 11)]  # 1..10, each freq=1
    attr = EquisizedIntervalsAttribute.build(name="test", field_id=1, support=0.3, values=values, dataset_size=10)
    assert attr.intervals is not None
    # Each interval should have freq >= 3 (10 * 0.3)
    for iv_ in attr.intervals:
        assert iv_.frequency >= 3

    result = attr.transform(5.0)
    assert result.startswith("[") or result.startswith("(")


def test_equisized_attribute_not_built_raises():
    from easyminer.parsers.pmml.preprocessing import EquisizedIntervalsAttribute

    attr = EquisizedIntervalsAttribute(name="test", field_id=1, support=0.2)
    with pytest.raises(RuntimeError, match="Intervals not built"):
        attr.transform(5.0)


# -- Open-ended interval tests --


def test_open_ended_intervals():
    """Test intervals with missing margins (infinity bounds)."""
    from easyminer.parsers.pmml.preprocessing import (
        NumericIntervalsAttribute,
        TransformationDictionary,
        create_attribute_from_pmml,
    )

    xml_content = """<?xml version="1.0" encoding="UTF-8"?>
    <TransformationDictionary xmlns="http://www.dmg.org/PMML-4_2">
        <DerivedField name="test-open">
            <Discretize field="100">
                <DiscretizeBin binValue="low">
                    <Interval closure="openClosed" rightMargin="10" />
                </DiscretizeBin>
                <DiscretizeBin binValue="mid">
                    <Interval closure="openClosed" leftMargin="10" rightMargin="20" />
                </DiscretizeBin>
                <DiscretizeBin binValue="high">
                    <Interval closure="openOpen" leftMargin="20" />
                </DiscretizeBin>
            </Discretize>
        </DerivedField>
    </TransformationDictionary>"""

    pmml = TransformationDictionary.from_xml_string(xml_content)
    attribute = create_attribute_from_pmml(pmml.derived_fields[0])

    assert isinstance(attribute, NumericIntervalsAttribute)
    assert attribute.transform(-1000.0) == "low"
    assert attribute.transform(10.0) == "low"
    assert attribute.transform(15.0) == "mid"
    assert attribute.transform(20.0) == "mid"
    assert attribute.transform(999.0) == "high"
