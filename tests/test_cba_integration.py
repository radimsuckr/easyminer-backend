"""
Tests for CBA (Classification Based on Associations) integration
"""

import pytest

from easyminer.tasks.cba_utils import AssociationRule, process_cleverminer_results


def test_process_cleverminer_results():
    """Test processing cleverminer results into AssociationRule objects"""
    cleverminer_rules = [
        {
            "rule_id": 1,
            "cedents_struct": {
                "ante": {"district-eachone": ["Praha"]},
                "succ": {"salary-categories": ["high"]},
            },
            "params": {"conf": 0.95, "rel_base": 0.1, "lift": 1.5},
        },
        {
            "rule_id": 2,
            "cedents_struct": {
                "ante": {"district-eachone": ["Brno"], "age": ["young"]},
                "succ": {"salary-categories": ["low"]},
            },
            "params": {"conf": 0.85, "rel_base": 0.08, "lift": 1.2},
        },
    ]

    rules = process_cleverminer_results(cleverminer_rules)

    assert len(rules) == 2

    # Check first rule
    assert rules[0].rule_id == 1
    assert rules[0].antecedent == ["district-eachone=Praha"]
    assert rules[0].consequent == "high"
    assert rules[0].confidence == 0.95
    assert rules[0].support == 0.1
    assert rules[0].lift == 1.5

    # Check second rule
    assert rules[1].rule_id == 2
    assert set(rules[1].antecedent) == {"district-eachone=Brno", "age=young"}
    assert rules[1].consequent == "low"
    assert rules[1].confidence == 0.85
    assert rules[1].support == 0.08


def test_process_cleverminer_results_empty_consequent():
    """Test that rules with empty consequents are skipped"""
    cleverminer_rules = [
        {
            "rule_id": 1,
            "cedents_struct": {"ante": {"attr": ["val"]}, "succ": {}},
            "params": {"conf": 0.95, "rel_base": 0.1},
        }
    ]

    with pytest.warns(UserWarning, match="empty consequent"):
        rules = process_cleverminer_results(cleverminer_rules)

    assert len(rules) == 0


def test_association_rule_dataclass():
    """Test AssociationRule dataclass"""
    rule = AssociationRule(
        antecedent=["age=young", "district=Praha"],
        consequent="high",
        confidence=0.95,
        support=0.1,
        lift=1.5,
        rule_id=42,
    )

    assert rule.antecedent == ["age=young", "district=Praha"]
    assert rule.consequent == "high"
    assert rule.confidence == 0.95
    assert rule.support == 0.1
    assert rule.lift == 1.5
    assert rule.rule_id == 42


def test_cba_result_structure():
    """Test CBAResult dataclass structure"""
    from easyminer.tasks.cba_utils import CBAResult

    result = CBAResult(
        accuracy=0.952,
        pruned_rules_count=12,
        original_rules_count=25,
        default_class="high",
        target_attribute="salary",
        filtered_rules_count=20,
        pruned_rule_ids=[1, 5, 7, 10, 12, 15, 18, 20, 22, 23, 24, 25],
    )

    assert result.accuracy == 0.952
    assert result.pruned_rules_count == 12
    assert result.original_rules_count == 25
    assert result.default_class == "high"
    assert result.target_attribute == "salary"
    assert result.filtered_rules_count == 20
    assert len(result.pruned_rule_ids) == 12
    assert result.pruned_rule_ids == [1, 5, 7, 10, 12, 15, 18, 20, 22, 23, 24, 25]
