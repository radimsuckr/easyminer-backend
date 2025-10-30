import logging
import warnings
from dataclasses import dataclass
from typing import Any

import pandas as pd
from pyarc import TransactionDB
from pyarc.algorithms import M1Algorithm, M2Algorithm, createCARs
from sqlalchemy import select
from sqlalchemy.orm import joinedload

from easyminer.database import get_sync_db_session
from easyminer.models.preprocessing import Attribute, DatasetInstance

logger = logging.getLogger(__name__)


@dataclass
class AssociationRule:
    antecedent: list[str]
    consequent: str
    confidence: float
    support: float
    lift: float = 1.0
    rule_id: int | None = None


@dataclass
class CBAResult:
    accuracy: float
    pruned_rules_count: int
    original_rules_count: int
    default_class: str
    target_attribute: str
    filtered_rules_count: int
    pruned_rule_ids: list[int]  # IDs of rules kept after pruning


def process_cleverminer_results(cleverminer_rules: list[dict[str, Any]]) -> list[AssociationRule]:
    rules = []

    for i, res_rule in enumerate(cleverminer_rules):
        try:
            # Based on the structure in cm_result.json
            cedents_struct = res_rule.get("cedents_struct", {})

            raw_antecedent = cedents_struct.get("ante", {})
            antecedent = [f"{key}={value}" for key, values in raw_antecedent.items() for value in values]

            raw_consequent = cedents_struct.get("succ", {})
            if not raw_consequent:
                warnings.warn(f"Skipping rule with ID {res_rule.get('rule_id', i)} due to empty consequent.")
                continue

            # Assuming one attribute in consequent for classification
            consequent_key = list(raw_consequent.keys())[0]
            consequent_value = raw_consequent[consequent_key][0]

            params = res_rule.get("params", {})
            confidence = float(params.get("conf", 0.0))
            support = float(params.get("rel_base", 0.0))
            lift = float(params.get("lift", 1.0))

            rule = AssociationRule(
                antecedent=antecedent,
                consequent=str(consequent_value),
                confidence=confidence,
                support=support,
                lift=lift,
                rule_id=res_rule.get("rule_id", i),
            )

            rules.append(rule)

        except (KeyError, ValueError, TypeError, IndexError) as e:
            warnings.warn(f"Error processing rule {res_rule.get('rule_id', 'N/A')}: {e}")
            continue

    return rules


def _filter_rules(rules: list[AssociationRule], max_rule_length: int) -> list[AssociationRule]:
    filtered_rules = []

    for rule in rules:
        if len(rule.antecedent) <= max_rule_length:
            filtered_rules.append(rule)

    return filtered_rules


def _sort_rules(rules: list[AssociationRule]) -> list[AssociationRule]:
    return sorted(rules, key=lambda r: (-r.confidence, -r.support, len(r.antecedent)))


def _to_pyarc_cars(rules: list[AssociationRule], class_label: str) -> list[Any]:
    raw = []
    for r in rules:
        # Re-format antecedent from ["attr=val"] to ["attr:=:val"]
        new_ante = []
        for item in r.antecedent:
            parts = item.split("=", 1)
            if len(parts) == 2:
                new_ante.append(f"{parts[0]}:=:{parts[1]}")
            else:
                # This case should ideally not be hit if process_cleverminer_results is correct
                warnings.warn(f"Skipping malformed antecedent item: '{item}' in rule ID {r.rule_id}")

        ante = tuple(new_ante)
        cons = f"{class_label}:=:{r.consequent}"
        raw.append((cons, ante, r.support, r.confidence))
    return createCARs(raw)


def load_dataset_dataframe(dataset_id: int, db_url: str | None = None) -> pd.DataFrame:
    with get_sync_db_session(db_url) as db:
        attributes = db.scalars(select(Attribute).where(Attribute.dataset_id == dataset_id)).all()
        if not attributes:
            raise ValueError(f"No attributes found for dataset ID {dataset_id}")

        column_names = [f.name for f in attributes]
        df = pd.DataFrame(columns=column_names)

        for attribute in attributes:
            instances = db.scalars(
                select(DatasetInstance)
                .where(DatasetInstance.attribute_id == attribute.id)
                .order_by(DatasetInstance.tx_id)
                .options(joinedload(DatasetInstance.value))
            ).all()
            df[attribute.name] = [i.value.value for i in instances]

    return df


def apply_cba_classification(
    cleverminer_result: dict[str, Any],
    dataset_id: int,
    target_attribute: str,
    max_rule_length: int = 10,
    use_m1_m2: bool = True,
    db_url: str | None = None,
) -> CBAResult:
    """
    Note:
        Confidence and support filtering is already done by cleverminer's 4ft algorithm.
        CBA only applies additional filtering by rule antecedent length, M1/M2 pruning, and accuracy calculation.
    """
    logger.info(f"Applying CBA classification with target attribute: {target_attribute}")

    # Load dataset
    df = load_dataset_dataframe(dataset_id, db_url)
    logger.info(f"Loaded dataset with {len(df)} rows and {len(df.columns)} columns")

    # Validate target attribute exists
    if target_attribute not in df.columns:
        raise ValueError(f"Target attribute '{target_attribute}' not found in dataset columns: {list(df.columns)}")

    # Prepare data
    X = df.drop(columns=[target_attribute])
    y = df[target_attribute]

    # Process cleverminer rules
    rules = process_cleverminer_results(cleverminer_result["rules"])
    original_count = len(rules)
    logger.info(f"Processed {original_count} rules from cleverminer")

    # Filter rules by antecedent length (confidence/support already filtered by cleverminer)
    rules = _filter_rules(rules, max_rule_length)
    filtered_count = len(rules)
    if filtered_count < original_count:
        logger.info(
            f"Filtered rules by length: {original_count} → {filtered_count} (max_antecedent_length≤{max_rule_length})"
        )
    else:
        logger.info(f"All {original_count} rules passed length filter (max_antecedent_length≤{max_rule_length})")

    if filtered_count == 0:
        logger.warning("No rules passed filtering criteria")
        # Return basic result with majority class
        default_class = y.mode().iloc[0] if not y.empty else "unknown"
        return CBAResult(
            accuracy=0.0,
            pruned_rules_count=0,
            original_rules_count=original_count,
            default_class=str(default_class),
            target_attribute=target_attribute,
            filtered_rules_count=0,
            pruned_rule_ids=[],
        )

    # Sort rules by precedence
    rules = _sort_rules(rules)

    # Calculate default class
    default_class = y.mode().iloc[0] if not y.empty else "unknown"

    # Apply M1 and M2 pruning algorithms if enabled
    pruned_rules_count = filtered_count
    pruned_rules_for_accuracy = rules

    if use_m1_m2 and filtered_count > 0:
        logger.info("Applying M1/M2 pruning algorithms using pyarc...")

        # Prepare data for pyarc
        target_col_name = target_attribute
        df_for_pyarc = X.copy()
        df_for_pyarc[target_col_name] = y
        txn_db = TransactionDB.from_DataFrame(df_for_pyarc, target=target_col_name)

        # Convert our AssociationRule objects to pyarc's CAR format
        pyarc_rules = _to_pyarc_cars(rules, target_col_name)

        # Build the classifier with M1/M2 from pyarc
        m1_clf = M1Algorithm(pyarc_rules, txn_db).build()
        logger.info(f"M1 pruning: {filtered_count} → {len(m1_clf.rules)} rules")

        # M2 takes the M1-pruned rules
        m2_clf = M2Algorithm(m1_clf.rules, txn_db).build()
        pruned_rules_count = len(m2_clf.rules)
        logger.info(f"M2 pruning: {len(m1_clf.rules)} → {pruned_rules_count} rules")

        # Use first N sorted rules for accuracy calculation (CBA first-match strategy)
        pruned_rules_for_accuracy = rules[:pruned_rules_count]

    # Calculate accuracy on training data
    accuracy = _calculate_accuracy(pruned_rules_for_accuracy, X, y, str(default_class))
    logger.info(f"CBA classifier accuracy: {accuracy:.2%}")

    # Collect rule IDs of pruned rules for filtering output
    pruned_rule_ids = [rule.rule_id for rule in pruned_rules_for_accuracy if rule.rule_id is not None]

    return CBAResult(
        accuracy=accuracy,
        pruned_rules_count=pruned_rules_count,
        original_rules_count=original_count,
        default_class=str(default_class),
        target_attribute=target_attribute,
        filtered_rules_count=filtered_count,
        pruned_rule_ids=pruned_rule_ids,
    )


def _calculate_accuracy(rules: list[AssociationRule], X: pd.DataFrame, y: pd.Series, default_class: str) -> float:
    if len(y) == 0:
        return 0.0

    correct = 0
    for idx in range(len(X)):
        row = X.iloc[idx : idx + 1]
        prediction = default_class

        # Find first matching rule
        for rule in rules:
            if _rule_matches_instance(rule, row):
                prediction = rule.consequent
                break

        if prediction == str(y.iloc[idx]):
            correct += 1

    return correct / len(y)


def _rule_matches_instance(rule: AssociationRule, instance: pd.DataFrame) -> bool:
    for condition in rule.antecedent:
        if "=" in condition:
            attr, value = condition.split("=", 1)
            if attr in instance.columns:
                if str(instance[attr].iloc[0]) != value:
                    return False
            else:
                return False
        else:
            # Handle boolean conditions
            if condition in instance.columns:
                if not instance[condition].iloc[0]:
                    return False
            else:
                return False

    return True
