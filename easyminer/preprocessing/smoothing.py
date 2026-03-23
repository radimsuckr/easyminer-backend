"""Interval smoothing algorithm for discretization, ported from Scala DbAttributeBuilder.

Adjusts interval boundaries iteratively to balance frequencies across intervals.
"""

from collections.abc import Callable
from dataclasses import dataclass
from math import ceil


@dataclass
class IntervalBorder:
    value: float
    inclusive: bool


@dataclass
class AttributeInterval:
    from_border: IntervalBorder
    to_border: IntervalBorder
    frequency: int


@dataclass
class ValueFrequency:
    value: float
    frequency: int


def smooth_intervals(
    intervals: list[AttributeInterval],
    values_desc: list[ValueFrequency],
    can_move_left: Callable[[ValueFrequency, AttributeInterval, AttributeInterval], bool],
    can_move_right: Callable[[ValueFrequency, AttributeInterval, AttributeInterval], bool],
) -> None:
    """Smooth interval boundaries to balance frequencies. Mutates intervals in place.

    Port of DbAttributeBuilder.smoothAttributeIntervals (tail-recursive →  iterative).

    Args:
        intervals: Mutable list of intervals to smooth.
        values_desc: All numeric values sorted DESCENDING by value.
        can_move_left: Callback deciding if a value can move from right interval to left.
            Called as can_move_left(moved_value, prev_interval, current_interval).
        can_move_right: Callback deciding if a value can move from left interval to right.
            Called as can_move_right(moved_value, prev_interval, current_interval).
    """
    while True:
        last_value: ValueFrequency | None = None
        pointer = len(intervals) - 1
        is_changed = False

        for value in values_desc:
            if pointer <= 0:
                break

            current = intervals[pointer]
            prev = intervals[pointer - 1]

            if (
                current.frequency > prev.frequency
                and value.value == current.from_border.value
                and last_value is not None
            ):
                # Right side heavier - try to move boundary left
                if can_move_left(value, prev, current):
                    intervals[pointer - 1] = AttributeInterval(
                        from_border=prev.from_border,
                        to_border=IntervalBorder(current.from_border.value, current.from_border.inclusive),
                        frequency=prev.frequency + value.frequency,
                    )
                    intervals[pointer] = AttributeInterval(
                        from_border=IntervalBorder(last_value.value, True),
                        to_border=current.to_border,
                        frequency=current.frequency - value.frequency,
                    )
                    last_value = None
                    pointer -= 1
                    is_changed = True
                else:
                    last_value = None
                    pointer -= 1

            elif (
                current.frequency < prev.frequency
                and last_value is not None
                and last_value.value == prev.to_border.value
            ):
                # Left side heavier - try to move boundary right
                next_last_value = None if prev.from_border.value == value.value else value
                if can_move_right(last_value, prev, current):
                    intervals[pointer - 1] = AttributeInterval(
                        from_border=prev.from_border,
                        to_border=IntervalBorder(value.value, True),
                        frequency=prev.frequency - last_value.frequency,
                    )
                    intervals[pointer] = AttributeInterval(
                        from_border=IntervalBorder(prev.to_border.value, prev.to_border.inclusive),
                        to_border=current.to_border,
                        frequency=current.frequency + last_value.frequency,
                    )
                    last_value = next_last_value
                    pointer -= 1
                    is_changed = True
                else:
                    last_value = next_last_value
                    pointer -= 1

            elif current.frequency == prev.frequency and value.value == current.from_border.value:
                # Equal frequencies at boundary - no move
                last_value = None
                pointer -= 1

            elif value.value == prev.from_border.value:
                # Reached left edge of prev interval - advance window
                last_value = None
                pointer -= 1

            else:
                # Interior value - remember for potential future move
                last_value = value

        if not is_changed:
            break


def finalize_intervals(intervals: list[AttributeInterval]) -> None:
    """Replace adjacent interval borders with midpoint cutpoints. Mutates in place.

    For each pair of adjacent intervals, computes the midpoint between the right border
    of the left interval and the left border of the right interval, then sets:
    - left interval's to_border = exclusive midpoint
    - right interval's from_border = inclusive midpoint
    """
    for i in range(len(intervals) - 1):
        left = intervals[i]
        right = intervals[i + 1]
        midpoint = (left.to_border.value + right.from_border.value) / 2.0
        intervals[i] = AttributeInterval(
            from_border=left.from_border,
            to_border=IntervalBorder(midpoint, inclusive=False),
            frequency=left.frequency,
        )
        intervals[i + 1] = AttributeInterval(
            from_border=IntervalBorder(midpoint, inclusive=True),
            to_border=right.to_border,
            frequency=right.frequency,
        )


def round_at_6(n: float) -> float:
    """Round to 6 decimal places, matching Scala's BasicFunction.roundAt(6)."""
    s = 10**6
    return round(n * s) / s


def format_interval(from_border: IntervalBorder, to_border: IntervalBorder) -> str:
    """Format an interval as a string matching Scala's toIntervalString.

    Examples: [8110.0,9587.0)  (1500.0,3000.0]
    """
    left = "[" if from_border.inclusive else "("
    right = "]" if to_border.inclusive else ")"
    return f"{left}{round_at_6(from_border.value)},{round_at_6(to_border.value)}{right}"


def init_equifrequent_intervals(
    values: list[ValueFrequency],
    bins_count: int,
    unique_values_count: int,
    dataset_size: int,
) -> list[AttributeInterval]:
    """Build initial equifrequent intervals from sorted ascending values.

    Port of MysqlEquifrequentIntervalsAttributeBuilder.initIntervals.
    """
    max_frequency = ceil(dataset_size / bins_count)
    intervals: list[AttributeInterval] = []
    processed = 0

    for vf in values:
        fields_minus_bins = unique_values_count - processed - bins_count + len(intervals)

        if (
            intervals
            and fields_minus_bins > 0
            and (
                len(intervals) == bins_count
                or abs(max_frequency - intervals[-1].frequency - vf.frequency)
                < abs(max_frequency - intervals[-1].frequency)
            )
        ):
            # Merge into last interval
            last = intervals[-1]
            intervals[-1] = AttributeInterval(
                from_border=last.from_border,
                to_border=IntervalBorder(vf.value, inclusive=True),
                frequency=last.frequency + vf.frequency,
            )
        else:
            # New interval
            intervals.append(
                AttributeInterval(
                    from_border=IntervalBorder(vf.value, inclusive=True),
                    to_border=IntervalBorder(vf.value, inclusive=True),
                    frequency=vf.frequency,
                )
            )
        processed += 1

    return intervals


def smooth_equifrequent(
    intervals: list[AttributeInterval],
    values_desc: list[ValueFrequency],
    max_frequency: float,
) -> None:
    """Apply equifrequent smoothing callbacks."""

    def can_move_left(moved: ValueFrequency, prev: AttributeInterval, current: AttributeInterval) -> bool:
        current_score = abs(max_frequency - current.frequency) + abs(max_frequency - prev.frequency)
        next_score = abs(max_frequency - current.frequency + moved.frequency) + abs(
            max_frequency - prev.frequency - moved.frequency
        )
        current_diff = abs(current.frequency - prev.frequency)
        next_diff = abs((current.frequency - moved.frequency) - (prev.frequency + moved.frequency))
        return next_score <= current_score and next_diff < current_diff

    def can_move_right(moved: ValueFrequency, prev: AttributeInterval, current: AttributeInterval) -> bool:
        current_score = abs(max_frequency - current.frequency) + abs(max_frequency - prev.frequency)
        next_score = abs(max_frequency - current.frequency - moved.frequency) + abs(
            max_frequency - prev.frequency + moved.frequency
        )
        current_diff = abs(current.frequency - prev.frequency)
        next_diff = abs((current.frequency + moved.frequency) - (prev.frequency - moved.frequency))
        return next_score <= current_score and next_diff < current_diff

    smooth_intervals(intervals, values_desc, can_move_left, can_move_right)


def init_equisized_intervals(
    values: list[ValueFrequency],
    min_frequency: float,
) -> list[AttributeInterval]:
    """Build initial equisized intervals from sorted ascending values.

    Port of MysqlEquisizedIntervalsAttributeBuilder.initIntervals.
    """
    intervals: list[AttributeInterval] = []

    for vf in values:
        if intervals and intervals[-1].frequency < min_frequency:
            # Merge into last interval
            last = intervals[-1]
            intervals[-1] = AttributeInterval(
                from_border=last.from_border,
                to_border=IntervalBorder(vf.value, inclusive=True),
                frequency=last.frequency + vf.frequency,
            )
        else:
            # New interval
            intervals.append(
                AttributeInterval(
                    from_border=IntervalBorder(vf.value, inclusive=True),
                    to_border=IntervalBorder(vf.value, inclusive=True),
                    frequency=vf.frequency,
                )
            )

    # Post-processing: merge last interval if below min_frequency
    if len(intervals) > 1 and intervals[-1].frequency < min_frequency:
        last = intervals.pop()
        prev = intervals[-1]
        intervals[-1] = AttributeInterval(
            from_border=prev.from_border,
            to_border=last.to_border,
            frequency=prev.frequency + last.frequency,
        )

    return intervals


def smooth_equisized(
    intervals: list[AttributeInterval],
    values_desc: list[ValueFrequency],
    min_frequency: float,
) -> None:
    """Apply equisized smoothing callbacks."""

    def can_move_left(moved: ValueFrequency, prev: AttributeInterval, current: AttributeInterval) -> bool:
        current_diff = abs(current.frequency - prev.frequency)
        decreased_freq = current.frequency - moved.frequency
        next_diff = abs(decreased_freq - (prev.frequency + moved.frequency))
        return decreased_freq >= min_frequency and next_diff < current_diff

    def can_move_right(moved: ValueFrequency, prev: AttributeInterval, current: AttributeInterval) -> bool:
        current_diff = abs(current.frequency - prev.frequency)
        decreased_freq = prev.frequency - moved.frequency
        next_diff = abs((current.frequency + moved.frequency) - decreased_freq)
        return decreased_freq >= min_frequency and next_diff < current_diff

    smooth_intervals(intervals, values_desc, can_move_left, can_move_right)
