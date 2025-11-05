import sys
import warnings
from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd
from cleverminer.cleverminer import cleverminer
from pyarc import TransactionDB
from pyarc.algorithms import M1Algorithm, M2Algorithm, createCARs
from sqlalchemy import select
from sqlalchemy.orm import joinedload

from easyminer.database import get_sync_db_session
from easyminer.models.preprocessing import Attribute, DatasetInstance
from easyminer.parsers.pmml.miner import BBASetting, CoefficientType, DBASetting, DBASettingType, SimplePmmlParser


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

    def _to_pyarc_cars(self, rules: list[AssociationRule], class_label: str) -> list:
        """Converts internal AssociationRule objects to pyarc's CAR format."""
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

    def _from_pyarc_cars(self, pyarc_rules: list) -> list[AssociationRule]:
        """Converts pyarc's CARs back to internal AssociationRule objects."""
        rules = []
        for i, r in enumerate(pyarc_rules):
            # In a built classifier, antecedent items are tuples of (attr, val)
            antecedent = [f"{item[0]}={item[1]}" for item in r.antecedent]

            # The consequent is a pyarc.Consequent object; we need its value.
            consequent = r.consequent.value

            rule = AssociationRule(
                antecedent=antecedent,
                consequent=str(consequent),
                confidence=r.confidence,
                support=r.support,
                lift=getattr(r, "lift", 1.0),
                rule_id=getattr(r, "rid", i),
            )
            rules.append(rule)
        return rules

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
            print("Applying M1/M2 pruning algorithms using pyarc...")

            # Prepare data for pyarc
            target_col_name = str(y.name or "class")
            df_for_pyarc = X.copy()
            df_for_pyarc[target_col_name] = y
            txn_db = TransactionDB.from_DataFrame(df_for_pyarc, target=target_col_name)

            # Convert our AssociationRule objects to pyarc's CAR format
            pyarc_rules = self._to_pyarc_cars(self.rules, target_col_name)

            # Build the classifier with M1/M2 from pyarc
            m1_clf = M1Algorithm(pyarc_rules, txn_db).build()
            print(f"M1 pruning: {len(self.rules)} → {len(m1_clf.rules)} rules")

            # M2 takes the M1-pruned rules
            m2_clf = M2Algorithm(m1_clf.rules, txn_db).build()
            print(f"M2 pruning: {len(m1_clf.rules)} → {len(m2_clf.rules)} rules")

            # Convert the pruned pyarc rules back to our AssociationRule format
            self.pruned_rules = self._from_pyarc_cars(m2_clf.rules)
        else:
            print("Skipping M1/M2 pruning")
            self.pruned_rules = self.rules.copy()

        print(f"Final classifier uses {len(self.pruned_rules)} rules with default class: {self.default_class}")
        self.is_fitted = True

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


class MinerService:
    def __init__(self, dataset_id: int):
        self.df: pd.DataFrame | None = None
        self.ds_id: int = dataset_id

    def load_data(self) -> None:
        with get_sync_db_session() as db:
            attributes = db.scalars(select(Attribute).where(Attribute.dataset_id == self.ds_id)).all()
            if not attributes:
                raise ValueError(f"No attributes found for dataset ID {self.ds_id}")
            column_names = [f.name for f in attributes]
            self.df = pd.DataFrame(columns=column_names)
            for attribute in attributes:
                instances = db.scalars(
                    select(DatasetInstance)
                    .where(DatasetInstance.attribute_id == attribute.id)
                    .order_by(DatasetInstance.tx_id)
                    .options(joinedload(DatasetInstance.value))
                ).all()
                self.df[attribute.name] = [i.value.value for i in instances]

    def mine_4ft(
        self, quantifiers: dict[str, float], antecedents: dict[str, Any], consequents: dict[str, Any]
    ) -> cleverminer:
        if self.df is None:
            raise ValueError("Dataframe not loaded. Call load_data() first.")
        return cleverminer(df=self.df, proc="4ftMiner", quantifiers=quantifiers, ante=antecedents, succ=consequents)


if __name__ == "__main__":
    with open("./cursor.xml") as f:
        parser = SimplePmmlParser(f.read())
    pmml = parser.parse()
    ts = pmml.association_model.task_setting
    print(f"PMML Version: {pmml.version}")
    print(f"PMML Header: {pmml.header}")
    print("-" * 10)

    base_candidates = list(filter(lambda x: x.interest_measure.lower() == "base", ts.interest_measure_settings))
    confidence_candidates = list(filter(lambda x: x.interest_measure.lower() == "conf", ts.interest_measure_settings))
    aad_candidates = list(filter(lambda x: x.interest_measure.lower() == "aad", ts.interest_measure_settings))
    quantifiers: dict[str, float] = {}
    if base_candidates:
        quantifiers["Base"] = base_candidates[0].threshold
    if confidence_candidates:
        quantifiers["conf"] = confidence_candidates[0].threshold
    if aad_candidates:
        quantifiers["aad"] = aad_candidates[0].threshold

    ds_id = int(pmml.header.extensions[0].value)
    svc = MinerService(ds_id)
    svc.load_data()
    if svc.df is None:
        print("Dataframe could not be loaded.")
        sys.exit(1)
    all_attribute_names = list(svc.df.columns)

    antecedent_setting_id = ts.antecedent_setting
    consequent_setting_id = ts.consequent_setting

    def build_dba_dict(dba_setting: DBASetting, bba_settings: list[BBASetting]) -> dict[str, Any]:
        return {
            "attributes": [
                {
                    "name": next(filter(lambda x: x.id == bba_ref, bba_settings)).name,
                    "type": "seq"
                    if next(filter(lambda x: x.id == bba_ref, bba_settings)).coefficient.type
                    == CoefficientType.sequence
                    else "subset",
                    "minlen": next(filter(lambda x: x.id == bba_ref, bba_settings)).coefficient.minimal_length,
                    "maxlen": next(filter(lambda x: x.id == bba_ref, bba_settings)).coefficient.maximal_length,
                }
                for bba_ref in dba_setting.ba_refs
            ],
            "minlen": dba_setting.minimal_length,
            "maxlen": dba_setting.maximal_length,
            "type": "con" if dba_setting.type == DBASettingType.conjunction else "dis",
        }

    if antecedent_setting_id and consequent_setting_id:
        print("Constraint: Antecedent and Consequent")
        antecedent_dba = next(filter(lambda x: x.id == antecedent_setting_id, ts.dba_settings))
        consequent_dba = next(filter(lambda x: x.id == consequent_setting_id, ts.dba_settings))
        antecedents = build_dba_dict(antecedent_dba, ts.bba_settings)
        consequents = build_dba_dict(consequent_dba, ts.bba_settings)
    elif antecedent_setting_id:
        print("Constraint: Antecedent only (Consequent can be any attribute)")
        antecedent_dba = next(filter(lambda x: x.id == antecedent_setting_id, ts.dba_settings))
        antecedents = build_dba_dict(antecedent_dba, ts.bba_settings)
        consequents = {
            "attributes": [{"name": attr, "type": "subset", "minlen": 1, "maxlen": 1} for attr in all_attribute_names],
            "minlen": 1,
            "maxlen": 1,
            "type": "con",
        }
    elif consequent_setting_id:
        print("Constraint: Consequent only (Antecedent can be any attribute)")
        consequent_dba = next(filter(lambda x: x.id == consequent_setting_id, ts.dba_settings))
        consequents = build_dba_dict(consequent_dba, ts.bba_settings)
        antecedents = {
            "attributes": [{"name": attr, "type": "subset", "minlen": 1, "maxlen": 1} for attr in all_attribute_names],
            "minlen": 1,
            "maxlen": 1,
            "type": "con",
        }
    else:
        print("Constraint: None (any attribute can be on any side)")
        antecedents = {
            "attributes": [{"name": attr, "type": "subset", "minlen": 1, "maxlen": 1} for attr in all_attribute_names],
            "minlen": 1,
            "maxlen": 1,
            "type": "con",
        }
        consequents = antecedents.copy()

    cm = svc.mine_4ft(quantifiers=quantifiers, antecedents=antecedents, consequents=consequents)
    cm.print_summary()
    cm.print_rulelist()

    if (
        not isinstance(consequents, dict)
        or "attributes" not in consequents
        or not isinstance(consequents["attributes"], list)
        or not consequents["attributes"]
    ):
        print("Consequent attributes are not defined correctly in the PMML settings.")
        sys.exit(1)

    target_attribute = consequents["attributes"][0]["name"]
    X = svc.df.drop(columns=[target_attribute])
    y = svc.df[target_attribute]

    min_support = quantifiers.get("Base", 75.0) / len(svc.df) if len(svc.df) > 0 else 0.01
    cba = CBAClassifier(min_confidence=quantifiers.get("conf", 0.95), min_support=min_support)

    if "rules" not in cm.result:
        print("No 'rules' key found in cleverminer result.")
        sys.exit(1)
    rules = cba.process_cleverminer_results(cm.result["rules"])

    cba.fit(X, y, rules=rules)

    print("\nCBA Classifier Rule Summary:")
    print(cba.get_rule_summary())

    if not X.empty:
        print("\nExample prediction for the first row of the dataset:")
        print("Input features (first row):")
        print(X.head(1).to_string())
        prediction = cba.predict(X.head(1))
        print(f"Predicted class: {prediction[0]}")
        print(f"Actual class:    {y.iloc[0]}")

    accuracy = cba.evaluate(X, y)
    print(f"\nOverall Classifier Accuracy: {accuracy:.2%}")
