from pytest import LogCaptureFixture

from easyminer.parsers.pmml.miner import (
    BBASetting,
    BBASettings,
    Coefficient,
    CoefficientType,
    DBASetting,
    DBASettings,
    DBASettingType,
    LiteralSign,
    TaskSetting,
)
from easyminer.tasks.mine import resolve_dba_to_attributes


def _create_bba(bba_id: str, name: str, field_ref: str) -> BBASetting:
    return BBASetting(
        id=bba_id,
        text=f"{name}(value)",
        name=name,
        field_ref=field_ref,
        coefficient=Coefficient(type=CoefficientType.one_category, category="value"),
    )


def _create_dba(dba_id: str, ba_refs: list[str], dba_type: DBASettingType = DBASettingType.conjunction) -> DBASetting:
    return DBASetting(
        id=dba_id,
        type=dba_type,
        ba_refs=ba_refs,
        literal_sign=LiteralSign.positive if dba_type == DBASettingType.literal else LiteralSign.positive,
    )


def test_flat_structure_single_bba():
    """DBA directly references a single BBA"""
    # Setup: DBA(1) -> BBA(10, name="age")
    from easyminer.parsers.pmml.miner import BBASettings, DBASettings

    bba1 = _create_bba("10", "age", "age_field")
    dba1 = _create_dba("1", ["10"], DBASettingType.literal)

    task_setting = TaskSetting(
        bba_settings_container=BBASettings(bba_settings=[bba1]),
        dba_settings_container=DBASettings(dba_settings=[dba1]),
        antecedent_setting=None,
        consequent_setting=None,
    )

    # Execute
    result = resolve_dba_to_attributes(dba1, task_setting)

    # Assert
    assert result == ["age"]


def test_flat_structure_multiple_bbas():
    """DBA references multiple BBAs"""
    # Setup: DBA(1) -> [BBA(10, "age"), BBA(11, "salary")]
    bba1 = _create_bba("10", "age", "age_field")
    bba2 = _create_bba("11", "salary", "salary_field")
    dba1 = _create_dba("1", ["10", "11"], DBASettingType.conjunction)

    task_setting = TaskSetting(
        bba_settings_container=BBASettings(bba_settings=[bba1, bba2]),
        dba_settings_container=DBASettings(dba_settings=[dba1]),
        antecedent_setting=None,
        consequent_setting=None,
    )

    # Execute
    result = resolve_dba_to_attributes(dba1, task_setting)

    # Assert
    assert result == ["age", "salary"]  # Sorted alphabetically


def test_two_level_hierarchy():
    """DBA references another DBA which references BBAs"""
    # Setup:
    #   DBA(1) -> DBA(2)
    #   DBA(2) -> [BBA(10, "age"), BBA(11, "salary")]
    bba1 = _create_bba("10", "age", "age_field")
    bba2 = _create_bba("11", "salary", "salary_field")
    dba2 = _create_dba("2", ["10", "11"], DBASettingType.conjunction)
    dba1 = _create_dba("1", ["2"], DBASettingType.conjunction)

    task_setting = TaskSetting(
        bba_settings_container=BBASettings(bba_settings=[bba1, bba2]),
        dba_settings_container=DBASettings(dba_settings=[dba1, dba2]),
        antecedent_setting=None,
        consequent_setting=None,
    )

    # Execute
    result = resolve_dba_to_attributes(dba1, task_setting)

    # Assert
    assert result == ["age", "salary"]


def test_three_level_hierarchy():
    """Full 3-level hierarchy as per specification"""
    # Setup (matches spec example):
    #   Level 1: DBA(10) -> [DBA(20), DBA(21)]
    #   Level 2: DBA(20) -> [DBA(30), DBA(31)]
    #            DBA(21) -> [DBA(32)]
    #   Level 3: DBA(30) -> BBA(1, "age")
    #            DBA(31) -> BBA(2, "salary")
    #            DBA(32) -> BBA(3, "status")

    # BBAs (leaf nodes)
    bba1 = _create_bba("1", "age", "age_field")
    bba2 = _create_bba("2", "salary", "salary_field")
    bba3 = _create_bba("3", "status", "status_field")

    # Level 3 DBAs (Literals)
    dba30 = _create_dba("30", ["1"], DBASettingType.literal)
    dba31 = _create_dba("31", ["2"], DBASettingType.literal)
    dba32 = _create_dba("32", ["3"], DBASettingType.literal)

    # Level 2 DBAs (Partial cedents)
    dba20 = _create_dba("20", ["30", "31"], DBASettingType.conjunction)
    dba21 = _create_dba("21", ["32"], DBASettingType.disjunction)

    # Level 1 DBA (Top cedent)
    dba10 = _create_dba("10", ["20", "21"], DBASettingType.conjunction)

    task_setting = TaskSetting(
        bba_settings_container=BBASettings(bba_settings=[bba1, bba2, bba3]),
        dba_settings_container=DBASettings(dba_settings=[dba10, dba20, dba21, dba30, dba31, dba32]),
        antecedent_setting=None,
        consequent_setting=None,
    )

    # Execute
    result = resolve_dba_to_attributes(dba10, task_setting)

    # Assert - all three attributes should be collected
    assert result == ["age", "salary", "status"]


def test_duplicate_attributes_deduplicated():
    """Duplicate attributes from different branches are deduplicated"""
    # Setup:
    #   DBA(1) -> [DBA(2), DBA(3)]
    #   DBA(2) -> BBA(10, "age")
    #   DBA(3) -> BBA(10, "age")  # Same BBA referenced again

    bba1 = _create_bba("10", "age", "age_field")
    dba2 = _create_dba("2", ["10"], DBASettingType.literal)
    dba3 = _create_dba("3", ["10"], DBASettingType.literal)
    dba1 = _create_dba("1", ["2", "3"], DBASettingType.conjunction)

    task_setting = TaskSetting(
        bba_settings_container=BBASettings(bba_settings=[bba1]),
        dba_settings_container=DBASettings(dba_settings=[dba1, dba2, dba3]),
        antecedent_setting=None,
        consequent_setting=None,
    )

    # Execute
    result = resolve_dba_to_attributes(dba1, task_setting)

    # Assert - "age" should appear only once
    assert result == ["age"]


def test_mixed_dba_and_bba_references():
    """DBA references both DBAs and BBAs directly"""
    # Setup:
    #   DBA(1) -> [DBA(2), BBA(11)]
    #   DBA(2) -> BBA(10, "age")
    #   BBA(11, "salary")

    bba1 = _create_bba("10", "age", "age_field")
    bba2 = _create_bba("11", "salary", "salary_field")
    dba2 = _create_dba("2", ["10"], DBASettingType.literal)
    dba1 = _create_dba("1", ["2", "11"], DBASettingType.conjunction)

    task_setting = TaskSetting(
        bba_settings_container=BBASettings(bba_settings=[bba1, bba2]),
        dba_settings_container=DBASettings(dba_settings=[dba1, dba2]),
        antecedent_setting=None,
        consequent_setting=None,
    )

    # Execute
    result = resolve_dba_to_attributes(dba1, task_setting)

    # Assert
    assert result == ["age", "salary"]


def test_empty_ba_refs():
    """DBA with no BA references returns empty list"""
    dba1 = _create_dba("1", [], DBASettingType.conjunction)

    task_setting = TaskSetting(
        bba_settings_container=BBASettings(bba_settings=[]),
        dba_settings_container=DBASettings(dba_settings=[dba1]),
        antecedent_setting=None,
        consequent_setting=None,
    )

    # Execute
    result = resolve_dba_to_attributes(dba1, task_setting)

    # Assert
    assert result == []


def test_missing_ba_ref_logs_warning(caplog: LogCaptureFixture):
    """Missing BA reference logs warning and continues"""
    # Setup: DBA(1) -> ["999"] (non-existent reference)
    dba1 = _create_dba("1", ["999"], DBASettingType.literal)

    task_setting = TaskSetting(
        bba_settings_container=BBASettings(bba_settings=[]),
        dba_settings_container=DBASettings(dba_settings=[dba1]),
        antecedent_setting=None,
        consequent_setting=None,
    )

    # Execute
    with caplog.at_level("WARNING"):
        result = resolve_dba_to_attributes(dba1, task_setting)

    # Assert
    assert result == []
    assert "BASettingRef '999' not found" in caplog.text
