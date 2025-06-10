import sys
import warnings
from collections import Counter
from dataclasses import dataclass
from typing import Any, Literal

import numpy as np
import pandas as pd
from cleverminer.cleverminer import cleverminer
from pyarc import CBA, TransactionDB
from pyarc.algorithms import M1Algorithm, M2Algorithm, createCARs
from sqlalchemy import select
from sqlalchemy.orm import joinedload

from easyminer.database import get_sync_db_session
from easyminer.models.preprocessing import Attribute, DatasetInstance
from easyminer.parsers.pmml.miner import CoefficientType, DBASettingType, SimplePmmlParser

# class CBA_pyARC:
#     def __init__(self, cleverminer_rules: list[dict], algorithm: Literal["m1", "m2"] = "m1"):
#         self.rules = cleverminer_rules
#         self.algorithm = algorithm.lower()
#         self.clf: M1Algorithm | M2Algorithm | None = None
#         self.default_class: str | None = None
#
#     @staticmethod
#     def _to_pyarc_transactions(rows: list[dict], class_label: str) -> TransactionDB:
#         # TransactionDB accepts list of dicts with all attributes including class_label
#         return TransactionDB.from_dict(rows, default_label=class_label)
#
#     def _to_car_list(self) -> list:
#         """
#         Converts CleverMiner-like rules to pyarc's internal rule format.
#         Expected input format per rule:
#             {
#                 'antecedent': {'A': 'v1', 'B': 'v2'},
#                 'consequent': 'ClassLabel',
#                 'support': 0.15,
#                 'confidence': 0.8
#             }
#         """
#         raw = []
#         for r in self.rules:
#             ante = tuple(f"{k}={v}" for k, v in r["antecedent"].items())
#             cons = f"class={r['consequent']}"
#             raw.append((cons, ante, r["support"], r["confidence"]))
#         return createCARs(raw)
#
#     def fit(self, rows: list[dict], class_label: str):
#         txn_db = self._to_pyarc_transactions(rows, class_label)
#         cars = self._to_car_list()
#
#         if self.algorithm == "m2":
#             self.clf = M2Algorithm(cars, txn_db).build()
#         else:
#             self.clf = M1Algorithm(cars, txn_db).build()
#
#         self.default_class = Counter(r[class_label] for r in rows).most_common(1)[0][0]
#         return self
#
#     def predict(self, instance: dict):
#         if self.clf is None:
#             raise RuntimeError("Classifier has not been fitted yet. Call fit() first.")
#         return self.clf.predict(instance)
#
#     def evaluate(self, rows: list[dict], class_label: str) -> float:
#         correct = 0
#         for r in rows:
#             inst = {k: v for k, v in r.items() if k != class_label}
#             pred = self.predict(inst)
#             if pred == r[class_label]:
#                 correct += 1
#         return correct / len(rows) if rows else 0


@dataclass
class AssociationRule:
    """Represents an association rule from cleverminer or other sources"""

    antecedent: list[str]
    consequent: str
    confidence: float
    support: float
    lift: float = 1.0
    rule_id: int | None = None


class CBAClassifier:
    """
    Classification Based on Associations (CBA) classifier with M1/M2 algorithms

    This classifier processes association rules (e.g., from cleverminer) and builds
    a classification model using the CBA approach with M1 and M2 pruning algorithms.
    """

    def __init__(
        self,
        min_confidence: float = 0.5,
        min_support: float = 0.01,
        max_rule_length: int = 10,
        use_m1_m2: bool = True,
        default_class_strategy: str = "majority",
    ):
        """
        Initialize CBA Classifier

        Args:
            min_confidence: Minimum confidence threshold for rules
            min_support: Minimum support threshold for rules
            max_rule_length: Maximum length of rule antecedent
            use_m1_m2: Whether to use M1/M2 pruning algorithms
            default_class_strategy: Strategy for default class ('majority', 'weighted')
        """
        self.min_confidence = min_confidence
        self.min_support = min_support
        self.max_rule_length = max_rule_length
        self.use_m1_m2 = use_m1_m2
        self.default_class_strategy = default_class_strategy

        self.rules: list[AssociationRule] = []
        self.pruned_rules: list[AssociationRule] = []
        self.default_class: str | None = None
        self.class_distribution: dict[str, float] = {}
        self.is_fitted: bool = False

    def process_cleverminer_results(self, cleverminer_rules: list[dict]) -> list[AssociationRule]:
        """
        Process results from cleverminer module into AssociationRule objects

        Args:
            cleverminer_rules: list of dictionaries containing rule information

        Returns:
            list of AssociationRule objects
        """
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

    def _filter_rules(self, rules: list[AssociationRule]) -> list[AssociationRule]:
        """Filter rules based on confidence, support, and length thresholds"""
        filtered_rules = []

        for rule in rules:
            if (
                rule.confidence >= self.min_confidence
                and rule.support >= self.min_support
                and len(rule.antecedent) <= self.max_rule_length
            ):
                filtered_rules.append(rule)

        return filtered_rules

    def _sort_rules(self, rules: list[AssociationRule]) -> list[AssociationRule]:
        """Sort rules by confidence (descending), then support (descending), then length (ascending)"""
        return sorted(rules, key=lambda r: (-r.confidence, -r.support, len(r.antecedent)))

    def _m1_algorithm(self, rules: list[AssociationRule], X: pd.DataFrame) -> list[AssociationRule]:
        """
        M1 Algorithm: Remove rules that don't improve classification accuracy

        Args:
            rules: Sorted list of association rules
            X: Training data features

        Returns:
            Pruned list of rules
        """
        if not rules:
            return rules

        pruned_rules = []
        covered_cases: set[int] = set()

        for rule in rules:
            # Find cases covered by this rule
            rule_covers = self._get_covered_cases(rule, X)

            # Check if rule covers any new cases or improves accuracy
            new_cases = rule_covers - covered_cases

            if new_cases:  # Rule covers at least one new case
                pruned_rules.append(rule)
                covered_cases.update(rule_covers)

                # Stop if all cases are covered
                if len(covered_cases) >= len(X):
                    break

        return pruned_rules

    def _m2_algorithm(self, rules: list[AssociationRule], X: pd.DataFrame, y: pd.Series) -> list[AssociationRule]:
        """
        M2 Algorithm: Remove rules that have lower precedence and same consequent

        Args:
            rules: list of rules from M1 algorithm
            X: Training data features
            y: Training data labels

        Returns:
            Further pruned list of rules
        """
        if not rules:
            return rules

        pruned_rules = []

        for i, rule in enumerate(rules):
            keep_rule = True
            rule_covers = self._get_covered_cases(rule, X)

            # Check against higher precedence rules
            for j in range(i):
                higher_rule = rules[j]
                higher_covers = self._get_covered_cases(higher_rule, X)

                # If higher precedence rule covers all cases of current rule
                # and has same or better accuracy, remove current rule
                if rule_covers.issubset(higher_covers):
                    rule_accuracy = self._calculate_rule_accuracy(rule, X, y, rule_covers)
                    higher_accuracy = self._calculate_rule_accuracy(higher_rule, X, y, higher_covers)

                    if higher_accuracy >= rule_accuracy:
                        keep_rule = False
                        break

            if keep_rule:
                pruned_rules.append(rule)

        return pruned_rules

    def _get_covered_cases(self, rule: AssociationRule, X: pd.DataFrame) -> set:
        """Get indices of cases covered by a rule"""
        covered = set(range(len(X)))

        for condition in rule.antecedent:
            if "=" in condition:
                attr, value = condition.split("=", 1)
                if attr in X.columns:
                    mask = X[attr].astype(str) == value
                    covered = covered.intersection(set(X[mask].index))
            else:
                # Handle boolean conditions or other formats
                if condition in X.columns:
                    mask = X[condition]
                    covered = covered.intersection(set(X[mask].index))

        return covered

    def _calculate_rule_accuracy(
        self, rule: AssociationRule, X: pd.DataFrame, y: pd.Series, covered_cases: set
    ) -> float:
        """Calculate accuracy of a rule on covered cases"""
        if not covered_cases:
            return 0.0

        correct_predictions = 0
        for idx in covered_cases:
            if str(y.iloc[idx]) == rule.consequent:
                correct_predictions += 1

        return correct_predictions / len(covered_cases)

    def fit(self, X: pd.DataFrame, y: pd.Series, rules: list[AssociationRule] | None = None):
        """
        Fit the CBA classifier

        Args:
            X: Training data features
            y: Training data labels
            rules: Association rules (from cleverminer or other sources)
                  If None, will attempt to generate rules using pyARC
        """
        # Calculate class distribution for default class
        self.class_distribution = y.value_counts(normalize=True).to_dict()

        if self.default_class_strategy == "majority":
            self.default_class = y.mode().iloc[0]
        elif self.default_class_strategy == "weighted":
            self.default_class = max(self.class_distribution.items(), key=lambda x: x[1])[0]

        # Use provided rules (e.g., from cleverminer) or generate with pyARC
        if rules:
            print(f"Using {len(rules)} pre-computed rules (e.g., from cleverminer)")
            self.rules = rules
        else:
            raise ValueError("No rules provided. Please provide rules from cleverminer.")

        # Filter rules based on thresholds
        original_count = len(self.rules)
        self.rules = self._filter_rules(self.rules)
        print(
            f"Filtered rules: {original_count} → {len(self.rules)} "
            f"(based on confidence≥{self.min_confidence}, support≥{self.min_support})"
        )

        # Sort rules by precedence (confidence desc, support desc, length asc)
        self.rules = self._sort_rules(self.rules)

        # Apply M1 and M2 pruning algorithms if enabled
        if self.use_m1_m2:
            print("Applying M1/M2 pruning algorithms...")
            m1_rules = self._m1_algorithm(self.rules, X)
            print(f"M1 pruning: {len(self.rules)} → {len(m1_rules)} rules")

            self.pruned_rules = self._m2_algorithm(m1_rules, X, y)
            print(f"M2 pruning: {len(m1_rules)} → {len(self.pruned_rules)} rules")
        else:
            print("Skipping M1/M2 pruning")
            self.pruned_rules = self.rules.copy()

        print(f"Final classifier uses {len(self.pruned_rules)} rules with default class: {self.default_class}")
        self.is_fitted = True

    def _generate_rules_with_pyarc(self, X: pd.DataFrame, y: pd.Series):
        """
        Generate association rules using pyARC library

        This is an alternative to using cleverminer rules. pyARC can automatically
        mine association rules from your training data using the Apriori algorithm.
        """
        try:
            # Prepare data for pyARC (requires specific format)
            df = X.copy()
            df["class"] = y

            # Create transaction database
            txns = TransactionDB.from_DataFrame(df)

            # Generate classification rules using CBA approach
            cba = CBA(support=self.min_support, confidence=self.min_confidence, maxlen=self.max_rule_length)

            # Fit and extract rules
            cba.fit(txns)

            # Convert pyARC rules to our format
            self.rules = []
            for i, rule in enumerate(cba.rules):
                antecedent = [str(item) for item in rule.antecedent]
                consequent = str(rule.consequent)

                self.rules.append(
                    AssociationRule(
                        antecedent=antecedent,
                        consequent=consequent,
                        confidence=rule.confidence,
                        support=rule.support,
                        lift=getattr(rule, "lift", 1.0),
                        rule_id=i,
                    )
                )

            print(f"Generated {len(self.rules)} rules using pyARC")

        except Exception as e:
            warnings.warn(f"Error generating rules with pyARC: {e}")
            self.rules = []

    def predict(self, X: pd.DataFrame) -> list[str]:
        """
        Predict class labels for input data

        Args:
            X: Input features

        Returns:
            list of predicted class labels
        """
        if not self.is_fitted:
            raise ValueError("Classifier must be fitted before prediction")

        if self.default_class is None:
            raise ValueError("Default class is not set. The classifier might not have been fitted correctly.")

        predictions = []

        for idx in range(len(X)):
            row = X.iloc[idx : idx + 1]
            prediction: str = self.default_class

            # Find first matching rule
            for rule in self.pruned_rules:
                if self._rule_matches_instance(rule, row):
                    prediction = rule.consequent
                    break

            predictions.append(prediction)

        return predictions

    def _rule_matches_instance(self, rule: AssociationRule, instance: pd.DataFrame) -> bool:
        """Check if a rule matches a single instance"""
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

    def predict_proba(self, X: pd.DataFrame) -> np.ndarray:
        """
        Predict class probabilities

        Args:
            X: Input features

        Returns:
            Array of class probabilities
        """
        if not self.is_fitted:
            raise ValueError("Classifier must be fitted before prediction")

        classes = list(self.class_distribution.keys())
        probabilities = []

        for idx in range(len(X)):
            row = X.iloc[idx : idx + 1]
            probs = [0.0] * len(classes)

            # Find first matching rule
            matched = False
            for rule in self.pruned_rules:
                if self._rule_matches_instance(rule, row):
                    # Use rule confidence as probability
                    try:
                        class_idx = classes.index(rule.consequent)
                        probs[class_idx] = rule.confidence
                        matched = True
                        break
                    except ValueError:
                        continue

            # Use default class distribution if no rule matches
            if not matched:
                for i, cls in enumerate(classes):
                    probs[i] = self.class_distribution.get(cls, 0.0)

            probabilities.append(probs)

        return np.array(probabilities)

    def get_rule_summary(self) -> pd.DataFrame:
        """Get summary of rules used by the classifier"""
        if not self.is_fitted:
            raise ValueError("Classifier must be fitted before getting rule summary")

        summary_data = []
        for i, rule in enumerate(self.pruned_rules):
            summary_data.append(
                {
                    "rule_id": i,
                    "antecedent": " AND ".join(rule.antecedent),
                    "consequent": rule.consequent,
                    "confidence": rule.confidence,
                    "support": rule.support,
                    "lift": rule.lift,
                    "length": len(rule.antecedent),
                }
            )

        return pd.DataFrame(summary_data)

    def evaluate(self, X: pd.DataFrame, y: pd.Series) -> float:
        """
        Evaluate the classifier's accuracy on a test set.

        Args:
            X: Test data features
            y: Test data labels

        Returns:
            Accuracy score
        """
        if not self.is_fitted:
            raise ValueError("Classifier must be fitted before evaluation")

        predictions = self.predict(X)
        correct = sum(1 for pred, actual in zip(predictions, y) if pred == actual)

        return correct / len(y) if len(y) > 0 else 0.0


class MinerService:
    def __init__(self, dataset_id: int):
        self._df: pd.DataFrame
        self._ds_id: int = dataset_id

    def _load_data(self) -> None:
        with get_sync_db_session() as db:
            attributes = db.scalars(select(Attribute).where(Attribute.dataset_id == self._ds_id)).all()
            self._df = pd.DataFrame(columns=tuple(f.name for f in attributes))
            for attribute in attributes:
                instances = db.scalars(
                    select(DatasetInstance)
                    .where(DatasetInstance.attribute_id == attribute.id)
                    .order_by(DatasetInstance.tx_id)
                    .options(joinedload(DatasetInstance.value))
                ).all()
                self._df[attribute.name] = [i.value.value for i in instances]

    def mine_4ft(
        self, quantifiers: dict[str, float], antecedents: dict[str, Any], consequents: dict[str, Any]
    ) -> cleverminer:
        self._load_data()
        return cleverminer(df=self._df, proc="4ftMiner", quantifiers=quantifiers, ante=antecedents, succ=consequents)


if __name__ == "__main__":
    with open("./cursor.xml") as f:
        parser = SimplePmmlParser(f.read())
    pmml = parser.parse()
    ts = pmml.association_model.task_setting
    print(f"PMML Version: {pmml.version}")
    print(f"PMML Header: {pmml.header}")
    # print(pmml)
    print("-" * 10)

    base_candidates = list(filter(lambda x: x.interest_measure.lower() == "base", ts.interest_measure_settings))
    if len(base_candidates) > 1:
        print("More than 1 Base candidates")
    confidence_candidates = list(filter(lambda x: x.interest_measure.lower() == "conf", ts.interest_measure_settings))
    if len(confidence_candidates) > 1:
        print("More than 1 conf candidates")
    aad_candidates = list(filter(lambda x: x.interest_measure.lower() == "aad", ts.interest_measure_settings))
    if len(aad_candidates) > 1:
        print("More than 1 conf candidates")

    quantifiers: dict[str, float] = {}
    if base_candidates:
        quantifiers["Base"] = base_candidates[0].threshold
    if confidence_candidates:
        quantifiers["conf"] = confidence_candidates[0].threshold
    if aad_candidates:
        quantifiers["aad"] = aad_candidates[0].threshold

    antecedent_setting_id = ts.antecedent_setting
    if not antecedent_setting_id:
        print("Antecedent setting not found")
        sys.exit(1)

    consequent_setting_id = ts.consequent_setting
    if not consequent_setting_id:
        print("Consequent setting not found")
        sys.exit(1)

    antecedent = next(filter(lambda x: x.id == antecedent_setting_id, ts.dba_settings))
    consequent = next(filter(lambda x: x.id == consequent_setting_id, ts.dba_settings))

    antecedents = {
        "attributes": [
            {
                "name": next(filter(lambda x: x.id == bba_ref, ts.bba_settings)).name,
                "type": "seq"
                if next(filter(lambda x: x.id == bba_ref, ts.bba_settings)).coefficient.type == CoefficientType.sequence
                else "subset",
                "minlen": next(filter(lambda x: x.id == bba_ref, ts.bba_settings)).coefficient.minimal_length,
                "maxlen": next(filter(lambda x: x.id == bba_ref, ts.bba_settings)).coefficient.maximal_length,
            }
            for bba_ref in antecedent.ba_refs
        ],
        "minlen": antecedent.minimal_length,
        "maxlen": antecedent.maximal_length,
        "type": "con" if antecedent.type == DBASettingType.conjunction else "dis",
    }
    consequents = {
        "attributes": [
            {
                "name": next(filter(lambda x: x.id == bba_ref, ts.bba_settings)).name,
                "type": "seq"
                if next(filter(lambda x: x.id == bba_ref, ts.bba_settings)).coefficient.type == CoefficientType.sequence
                else "subset",
                "minlen": next(filter(lambda x: x.id == bba_ref, ts.bba_settings)).coefficient.minimal_length,
                "maxlen": next(filter(lambda x: x.id == bba_ref, ts.bba_settings)).coefficient.maximal_length,
            }
            for bba_ref in consequent.ba_refs
        ],
        "minlen": consequent.minimal_length,
        "maxlen": consequent.maximal_length,
        "type": "con" if consequent.type == DBASettingType.conjunction else "dis",
    }

    ds_id = int(pmml.header.extensions[0].value)
    svc = MinerService(ds_id)  # Assuming the dataset ID is stored in the first extension
    cm = svc.mine_4ft(quantifiers=quantifiers, antecedents=antecedents, consequents=consequents)
    cm.print_summary()
    cm.print_rulelist()

    # The more feature-complete CBAClassifier is used here instead of the simpler CBA_pyARC wrapper.
    # This part demonstrates how to use it with the data and rules from cleverminer.

    # The data is already loaded in the MinerService instance.
    df = svc._df

    # The target variable for classification is the consequent from the mining task.
    if (
        not isinstance(consequents, dict)
        or "attributes" not in consequents
        or not isinstance(consequents["attributes"], list)
        or not consequents["attributes"]
    ):
        print("Consequent attributes are not defined correctly in the PMML settings.")
        sys.exit(1)

    target_attribute = consequents["attributes"][0]["name"]
    X = df.drop(columns=[target_attribute])
    y = df[target_attribute]

    # Initialize the CBA classifier with parameters from the mining task.
    # cleverminer's 'Base' is an absolute count, so we convert it to relative support.
    min_support = quantifiers.get("Base", 75.0) / len(df) if len(df) > 0 else 0.01
    cba = CBAClassifier(min_confidence=quantifiers.get("conf", 0.95), min_support=min_support)

    # The 'result' attribute of the cleverminer object contains the rules in a processed format.
    if "rules" not in cm.result:
        print("No 'rules' key found in cleverminer result.")
        sys.exit(1)
    rules = cba.process_cleverminer_results(cm.result["rules"])

    # Fit the classifier with the data and the processed rules.
    cba.fit(X, y, rules=rules)

    # Display a summary of the rules that the classifier will use.
    print("\nCBA Classifier Rule Summary:")
    print(cba.get_rule_summary())

    # Show an example of predicting a single instance.
    if not X.empty:
        print("\nExample prediction for the first row of the dataset:")
        print("Input features (first row):")
        print(X.head(1).to_string())
        prediction = cba.predict(X.head(1))
        print(f"Predicted class: {prediction[0]}")
        print(f"Actual class:    {y.iloc[0]}")

    # Evaluate the classifier's accuracy on the entire dataset.
    accuracy = cba.evaluate(X, y)
    print(f"\nOverall Classifier Accuracy: {accuracy:.2%}")

    # breakpoint()
