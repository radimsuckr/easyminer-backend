"""
pyARC Mining Demo - Pure Python rule mining without CleverMiner

This demo showcases two approaches for association rule mining using pyARC:
1. CBA with top_rules (automatic threshold detection) - mimics R's rCBA::build()
2. fim.apriori direct mining (manual control) - mimics R's arules::apriori()

Both approaches are pure Python and don't require R/Rserve.
"""

import sys
from dataclasses import dataclass

import fim
import pandas as pd
from pyarc import CBA, TransactionDB
from pyarc.algorithms import createCARs
from sqlalchemy import select
from sqlalchemy.orm import joinedload

from easyminer.database import get_sync_db_session
from easyminer.models.preprocessing import Attribute, DatasetInstance
from easyminer.parsers.pmml.miner import SimplePmmlParser


@dataclass
class AssociationRule:
    """Represents an association rule"""

    antecedent: list[str]
    consequent: str
    confidence: float
    support: float
    lift: float = 1.0
    rule_id: int | None = None


def load_dataset_from_db(dataset_id: int) -> pd.DataFrame:
    """Load dataset from database into a pandas DataFrame"""
    with get_sync_db_session() as db:
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


def convert_pyarc_cars_to_rules(pyarc_rules: list) -> list[AssociationRule]:
    """Convert pyARC CAR objects to our AssociationRule format"""
    rules = []
    for i, r in enumerate(pyarc_rules):
        # pyARC antecedent items are tuples of (attr, val)
        antecedent = [f"{item[0]}={item[1]}" for item in r.antecedent]

        # Consequent is a pyarc.Consequent object
        consequent = r.consequent.value

        rule = AssociationRule(
            antecedent=antecedent,
            consequent=str(consequent),
            confidence=r.confidence / 100.0,  # pyARC uses percentages
            support=r.support / 100.0,  # pyARC uses percentages
            lift=getattr(r, "lift", 1.0),
            rule_id=i,
        )
        rules.append(rule)
    return rules


def convert_fim_rules_to_association_rules(fim_rules: list, target_attribute: str) -> list[AssociationRule]:
    """
    Convert fim.apriori output to AssociationRule format

    fim.apriori returns: [(consequent, antecedent, support, confidence), ...]
    where items are in format "attr:=:value"
    """
    rules = []
    for i, (cons, ante, support, confidence) in enumerate(fim_rules):
        # Parse consequent
        if ":=:" in cons:
            cons_attr, cons_val = cons.split(":=:")
            if cons_attr != target_attribute:
                continue  # Skip rules where consequent is not the target

        # Parse antecedent
        antecedent = []
        for item in ante:
            if ":=:" in item:
                attr, val = item.split(":=:")
                antecedent.append(f"{attr}={val}")

        rule = AssociationRule(
            antecedent=antecedent,
            consequent=cons_val if ":=:" in cons else cons,
            confidence=confidence / 100.0,  # fim uses percentages
            support=support / 100.0,  # fim uses percentages
            lift=1.0,  # fim doesn't provide lift in rule output
            rule_id=i,
        )
        rules.append(rule)

    return rules


def mine_with_cba_top_rules(
    df: pd.DataFrame,
    target_attribute: str,
    target_rule_count: int = 1000,
    init_support: float = 0.01,
    init_confidence: float = 0.5,
    max_rule_length: int = 10,
    algorithm: str = "m2",
) -> tuple[list[AssociationRule], float]:
    """
    Option 1: Mine rules using pyARC's CBA with automatic threshold detection.

    This approach mimics R's rCBA::build() - it automatically adjusts
    confidence/support to find approximately target_rule_count rules.

    Args:
        df: Input dataframe
        target_attribute: Target class attribute
        target_rule_count: Target number of rules to find
        init_support: Initial support threshold
        init_confidence: Initial confidence threshold
        max_rule_length: Maximum rule length
        algorithm: "m1" or "m2" for pruning

    Returns:
        Tuple of (rules, accuracy)
    """
    print("\n" + "=" * 70)
    print("OPTION 1: Mining with pyARC CBA + top_rules (automatic thresholds)")
    print("=" * 70)
    print(f"Target attribute: {target_attribute}")
    print(f"Target rule count: {target_rule_count}")
    print(f"Initial support: {init_support}, confidence: {init_confidence}")
    print(f"Max rule length: {max_rule_length}")
    print(f"Pruning algorithm: {algorithm}")

    # Prepare transaction database
    txn_db = TransactionDB.from_DataFrame(df, target=target_attribute)
    print(f"\nTransaction DB: {len(txn_db)} transactions, {len(txn_db.unique_items)} unique items")

    # Initialize CBA with top_rules for automatic threshold detection
    cba = CBA(support=init_support, confidence=init_confidence, maxlen=max_rule_length, algorithm=algorithm)

    # top_rules_args enables automatic threshold tuning
    top_rules_args = {
        "target_rule_count": target_rule_count,
        "init_support": init_support,
        "init_conf": init_confidence,
        "conf_step": 0.05,  # Decrease confidence by 5% each iteration
        "supp_step": 0.05,  # Increase support by 5% each iteration
        "minlen": 1,  # Minimum rule length
        "init_maxlen": min(3, max_rule_length),  # Start with shorter rules
        "total_timeout": 100.0,  # 100 second timeout
        "max_iterations": 30,  # Maximum iterations
    }

    print("\nFitting CBA with automatic threshold detection...")
    print("(This will iteratively adjust thresholds to reach target rule count)")

    cba.fit(txn_db, top_rules_args=top_rules_args)

    print("\n✓ CBA fitted successfully!")
    print(f"  Final rules count: {len(cba.clf.rules)}")

    # Calculate accuracy
    accuracy = cba.rule_model_accuracy(txn_db)
    print(f"  Training accuracy: {accuracy:.2%}")

    # Convert to our AssociationRule format
    rules = convert_pyarc_cars_to_rules(cba.clf.rules)

    return rules, accuracy


def mine_with_fim_apriori(
    df: pd.DataFrame,
    target_attribute: str,
    support: float = 0.01,
    confidence: float = 0.5,
    max_rule_length: int = 10,
) -> list[AssociationRule]:
    """
    Option 2: Mine rules directly using fim.apriori (what R arules uses internally).

    This gives you manual control over thresholds - no automatic adjustment.

    Args:
        df: Input dataframe
        target_attribute: Target class attribute
        support: Minimum support threshold (0.0-1.0)
        confidence: Minimum confidence threshold (0.0-1.0)
        max_rule_length: Maximum rule length

    Returns:
        List of association rules
    """
    print("\n" + "=" * 70)
    print("OPTION 2: Mining with fim.apriori (manual thresholds)")
    print("=" * 70)
    print(f"Target attribute: {target_attribute}")
    print(f"Support: {support}, Confidence: {confidence}")
    print(f"Max rule length: {max_rule_length}")

    # Prepare transaction database
    txn_db = TransactionDB.from_DataFrame(df, target=target_attribute)
    transactions = txn_db.string_representation

    print(f"\nTransaction DB: {len(transactions)} transactions")

    # Set up appearance constraints (consequent must be target attribute)
    # This matches the R arules appearance parameter
    appearance = {
        "rhs": [f"{target_attribute}:=:{val}" for val in df[target_attribute].unique()],
        "lhs": [],  # Any other attribute can be in antecedent
        "default": "lhs",
    }

    print("\nMining association rules with fim.apriori...")

    # Mine rules with fim.apriori
    # Note: fim uses percentages for support/confidence
    rules = fim.apriori(
        transactions,
        supp=support * 100,  # Convert to percentage
        conf=confidence * 100,  # Convert to percentage
        mode="o",  # Use original apriori algorithm
        target="r",  # Mine association rules (not just itemsets)
        report="sc",  # Report support and confidence
        appear=appearance,
        zmax=max_rule_length,  # Maximum rule length
    )

    print(f"✓ Mined {len(rules)} rules")

    # Convert to our AssociationRule format
    association_rules = convert_fim_rules_to_association_rules(rules, target_attribute)

    print(f"  {len(association_rules)} rules match target attribute constraint")

    return association_rules


def print_rules(rules: list[AssociationRule], max_display: int = 10):
    """Print rules in a readable format"""
    print(f"\nShowing {min(len(rules), max_display)} of {len(rules)} rules:")
    print("-" * 100)

    for i, rule in enumerate(rules[:max_display]):
        ante_str = " AND ".join(rule.antecedent) if rule.antecedent else "∅"
        print(f"\nRule {i + 1}:")
        print(f"  IF {ante_str}")
        print(f"  THEN {rule.consequent}")
        print(f"  Confidence: {rule.confidence:.3f}, Support: {rule.support:.3f}, Lift: {rule.lift:.3f}")


def apply_m1_m2_pruning(rules: list[AssociationRule], df: pd.DataFrame, target_attribute: str) -> list[AssociationRule]:
    """Apply M1 and M2 pruning to a set of rules"""
    from pyarc.algorithms import M1Algorithm, M2Algorithm

    print("\n" + "=" * 70)
    print("Applying M1/M2 Pruning")
    print("=" * 70)

    # Prepare transaction database
    txn_db = TransactionDB.from_DataFrame(df, target=target_attribute)

    # Convert to pyARC format
    raw_cars = []
    for r in rules:
        new_ante = []
        for item in r.antecedent:
            parts = item.split("=", 1)
            if len(parts) == 2:
                new_ante.append(f"{parts[0]}:=:{parts[1]}")

        ante = tuple(new_ante)
        cons = f"{target_attribute}:=:{r.consequent}"
        raw_cars.append((cons, ante, r.support * 100, r.confidence * 100))  # pyARC uses percentages

    pyarc_rules = createCARs(raw_cars)
    print(f"Starting with {len(pyarc_rules)} rules")

    # Apply M1
    m1_clf = M1Algorithm(pyarc_rules, txn_db).build()
    print(f"After M1: {len(m1_clf.rules)} rules")

    # Apply M2
    m2_clf = M2Algorithm(m1_clf.rules, txn_db).build()
    print(f"After M2: {len(m2_clf.rules)} rules")

    # Calculate accuracy
    accuracy = m2_clf.test_transactions(txn_db)
    print(f"Accuracy: {accuracy:.2%}")

    # Convert back
    pruned_rules = convert_pyarc_cars_to_association_rules(m2_clf.rules)

    return pruned_rules


def main():
    """Main demo function"""
    print("\n" + "=" * 70)
    print("pyARC Mining Demo - Pure Python Association Rule Mining")
    print("=" * 70)

    # Load PMML configuration
    with open("./cursor.xml") as f:
        parser = SimplePmmlParser(f.read())
    pmml = parser.parse()
    ts = pmml.association_model.task_setting

    print(f"\nPMML Version: {pmml.version}")
    print(f"Application: {pmml.header.application_name} {pmml.header.application_version}")

    # Get dataset ID
    dataset_ext = next(filter(lambda x: x.name.lower() == "dataset", pmml.header.extensions), None)
    if not dataset_ext:
        print("Error: Dataset extension not found in PMML header")
        sys.exit(1)

    dataset_id = int(dataset_ext.value)
    print(f"Dataset ID: {dataset_id}")

    # Load data
    print("\nLoading dataset from database...")
    df = load_dataset_from_db(dataset_id)
    print(f"✓ Loaded: {len(df)} rows, {len(df.columns)} columns")
    print(f"  Columns: {list(df.columns)}")

    # Extract mining parameters from PMML
    confidence_candidates = list(
        filter(lambda x: x.interest_measure.lower() in ["conf", "fui"], ts.interest_measure_settings)
    )
    support_candidates = list(filter(lambda x: x.interest_measure.lower() == "supp", ts.interest_measure_settings))
    rule_length_candidates = list(
        filter(lambda x: x.interest_measure.lower() == "rule_length", ts.interest_measure_settings)
    )

    confidence = confidence_candidates[0].threshold if confidence_candidates else 0.5
    support = support_candidates[0].threshold if support_candidates else 0.01
    max_rule_length = (
        int(rule_length_candidates[0].threshold)
        if rule_length_candidates and rule_length_candidates[0].threshold
        else 10
    )

    print("\nMining Parameters:")
    print(f"  Confidence: {confidence}")
    print(f"  Support: {support}")
    print(f"  Max Rule Length: {max_rule_length}")

    # Determine target attribute (from consequent)
    consequent_setting_id = ts.consequent_setting
    if not consequent_setting_id:
        print("Error: Consequent setting not found in PMML")
        sys.exit(1)

    consequent_dba = next(filter(lambda x: x.id == consequent_setting_id, ts.dba_settings))
    if not consequent_dba.ba_refs:
        print("Error: No BBA references in consequent DBA setting")
        sys.exit(1)

    first_bba_ref = consequent_dba.ba_refs[0]
    first_bba = next(filter(lambda x: x.id == first_bba_ref, ts.bba_settings))
    target_attribute = first_bba.name

    print(f"  Target Attribute: {target_attribute}")

    # Check if AUTO_CONF_SUPP is requested
    auto_conf_supp = any(im.interest_measure.lower() == "auto_conf_supp" for im in ts.interest_measure_settings)

    if auto_conf_supp:
        print("\n⚠ AUTO_CONF_SUPP detected - using automatic threshold detection")

    # ========================================================================
    # OPTION 1: Mine with CBA + top_rules (automatic threshold detection)
    # ========================================================================
    if auto_conf_supp or "--auto" in sys.argv:
        rules_option1, accuracy = mine_with_cba_top_rules(
            df=df,
            target_attribute=target_attribute,
            target_rule_count=1000,  # Try to find ~1000 rules
            init_support=support,
            init_confidence=confidence,
            max_rule_length=max_rule_length,
            algorithm="m2",  # Use M2 pruning
        )

        print_rules(rules_option1, max_display=10)

    # ========================================================================
    # OPTION 2: Mine with fim.apriori (manual control)
    # ========================================================================
    if not auto_conf_supp or "--manual" in sys.argv:
        rules_option2 = mine_with_fim_apriori(
            df=df,
            target_attribute=target_attribute,
            support=support,
            confidence=confidence,
            max_rule_length=max_rule_length,
        )

        print_rules(rules_option2, max_display=10)

        # Optionally apply M1/M2 pruning to fim rules
        if rules_option2 and ("--prune" in sys.argv or not auto_conf_supp):
            pruned_rules = apply_m1_m2_pruning(rules_option2, df, target_attribute)
            print_rules(pruned_rules, max_display=10)

    print("\n" + "=" * 70)
    print("Demo Complete!")
    print("=" * 70)
    print("\nCommand line options:")
    print("  --auto    : Force use of Option 1 (CBA + top_rules)")
    print("  --manual  : Force use of Option 2 (fim.apriori)")
    print("  --prune   : Apply M1/M2 pruning to Option 2 results")
    print("\nWithout options, behavior depends on AUTO_CONF_SUPP in PMML")


if __name__ == "__main__":
    main()
