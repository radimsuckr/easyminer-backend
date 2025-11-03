"""
Verification Script: Prove both mining pipelines work with available Python packages

This script tests:
1. Mode 1 (Standard): arules::apriori() equivalent using fim.arules()
2. Mode 2 (AUTO_CONF_SUPP): rCBA::build() equivalent using pyARC CBA + top_rules
"""

import fim
import pandas as pd
from pyarc import CBA, TransactionDB


def create_test_dataset():
    """Create a simple test dataset for classification"""
    data = {
        "Weather": [
            "Sunny",
            "Sunny",
            "Overcast",
            "Rain",
            "Rain",
            "Rain",
            "Overcast",
            "Sunny",
            "Sunny",
            "Rain",
            "Sunny",
            "Overcast",
            "Overcast",
            "Rain",
        ],
        "Temperature": [
            "Hot",
            "Hot",
            "Hot",
            "Mild",
            "Cool",
            "Cool",
            "Cool",
            "Mild",
            "Cool",
            "Mild",
            "Mild",
            "Mild",
            "Hot",
            "Mild",
        ],
        "Humidity": [
            "High",
            "High",
            "High",
            "High",
            "Normal",
            "Normal",
            "Normal",
            "High",
            "Normal",
            "Normal",
            "Normal",
            "High",
            "Normal",
            "High",
        ],
        "Wind": [
            "Weak",
            "Strong",
            "Weak",
            "Weak",
            "Weak",
            "Strong",
            "Strong",
            "Weak",
            "Weak",
            "Weak",
            "Strong",
            "Strong",
            "Weak",
            "Strong",
        ],
        "PlayTennis": ["No", "No", "Yes", "Yes", "Yes", "No", "Yes", "No", "Yes", "Yes", "Yes", "Yes", "Yes", "No"],
    }
    return pd.DataFrame(data)


def test_mode1_fim_arules():
    """
    Mode 1: Standard Mining with fixed thresholds
    Mimics: R's arules::apriori(txns, parameter=list(confidence=X, support=Y))
    """
    print("\n" + "=" * 70)
    print("MODE 1: Standard Mining (fim.arules)")
    print("=" * 70)

    df = create_test_dataset()
    target = "PlayTennis"

    # Prepare transactions in pyARC format
    txn_db = TransactionDB.from_DataFrame(df, target=target)
    transactions = txn_db.string_representation

    print(f"Dataset: {len(df)} rows, target='{target}'")
    print(f"Transactions: {len(transactions)}")

    # User-specified thresholds (from PMML)
    user_confidence = 70.0  # User says: "I want 70% confidence"
    user_support = 20.0  # User says: "I want 20% support"
    max_rule_length = 5

    print("\nUser-specified thresholds:")
    print(f"  Confidence: {user_confidence}%")
    print(f"  Support: {user_support}%")
    print(f"  Max rule length: {max_rule_length}")

    # Mine with fim.arules (equivalent to R's arules::apriori)
    print("\nMining with fim.arules()...")

    # Set up appearance constraints (consequent must be PlayTennis)
    # fim appearance: dict of {item: 'i'/'o'/'b'/'n'} where o=consequent
    appearance = {}
    for val in df[target].unique():
        appearance[f"{target}:=:{val}"] = "o"

    rules = fim.arules(
        transactions,
        supp=user_support,  # User-provided
        conf=user_confidence,  # User-provided
        mode="o",  # Original apriori
        report="sc",  # Report support & confidence
        appear=appearance,
        zmax=max_rule_length,
    )

    print(f"✓ Mined {len(rules)} rules")

    # Display sample rules
    if rules:
        print("\nSample rules (first 10):")
        for i, (cons, ante, supp, conf) in enumerate(rules[:10]):
            ante_str = " AND ".join(ante) if ante else "∅"
            print(f"  {i + 1}. IF {ante_str}")
            print(f"     THEN {cons}")
            print(f"     [conf={conf:.1f}%, supp={supp:.1f}%]")

    return len(rules) > 0


def test_mode2_pyarc_cba_with_top_rules():
    """
    Mode 2: AUTO_CONF_SUPP with automatic threshold detection
    Mimics: R's rCBA::build(txns, className="PlayTennis")
    """
    print("\n" + "=" * 70)
    print("MODE 2: AUTO_CONF_SUPP (pyARC CBA + top_rules)")
    print("=" * 70)

    df = create_test_dataset()
    target = "PlayTennis"

    print(f"Dataset: {len(df)} rows, target='{target}'")

    # NO user-specified thresholds!
    # System automatically finds optimal confidence/support
    target_rule_count = 10  # User says: "Give me ~10 good rules"
    max_rule_length = 5

    print("\nAutomatic threshold detection:")
    print(f"  Target rule count: {target_rule_count}")
    print(f"  Max rule length: {max_rule_length}")
    print("  System will find optimal confidence/support!")

    # Prepare transaction database
    txn_db = TransactionDB.from_DataFrame(df, target=target)

    print(f"\nTransaction DB: {len(txn_db)} transactions")

    # Initialize CBA with top_rules for automatic tuning
    print("\nInitializing CBA with automatic threshold detection...")
    cba = CBA(
        support=0.01,  # Just initial values
        confidence=0.5,  # top_rules will adjust these!
        maxlen=max_rule_length,
        algorithm="m2",  # Use M2 pruning
    )

    # top_rules_args enables automatic threshold tuning
    top_rules_args = {
        "target_rule_count": target_rule_count,
        "init_support": 1.0,  # Start low
        "init_conf": 50.0,  # Start at 50%
        "conf_step": 5.0,  # Adjust by 5% steps
        "supp_step": 5.0,
        "minlen": 1,
        "init_maxlen": 3,
        "total_timeout": 30.0,  # 30 sec timeout
        "max_iterations": 20,
    }

    print("Fitting CBA (this will automatically adjust thresholds)...")
    cba.fit(txn_db, top_rules_args=top_rules_args)

    print("\n✓ CBA fitted successfully!")
    print(f"  Rules found: {len(cba.clf.rules)}")
    print("  System automatically determined optimal thresholds!")

    # Calculate accuracy
    accuracy = cba.rule_model_accuracy(txn_db)
    print(f"  Training accuracy: {accuracy:.2%}")

    # Display sample rules
    if cba.clf.rules:
        print("\nSample rules (first 10):")
        for i, rule in enumerate(cba.clf.rules[:10]):
            ante_str = " AND ".join([f"{a[0]}={a[1]}" for a in rule.antecedent]) if rule.antecedent else "∅"
            print(f"  {i + 1}. IF {ante_str}")
            print(f"     THEN {rule.consequent.attribute}={rule.consequent.value}")
            print(f"     [conf={rule.confidence:.1f}%, supp={rule.support:.1f}%]")

    return len(cba.clf.rules) > 0


def main():
    """Run all verification tests"""
    print("\n" + "=" * 70)
    print("MINING PIPELINE VERIFICATION")
    print("Testing: Can we build both Scala/R mining modes in Python?")
    print("=" * 70)

    results = {}

    # Test Mode 1: Standard mining
    try:
        results["Mode 1 (fim.arules)"] = test_mode1_fim_arules()
    except Exception as e:
        print(f"\n✗ Mode 1 (fim.arules) FAILED: {e}")
        results["Mode 1 (fim.arules)"] = False

    # Test Mode 2: AUTO_CONF_SUPP
    try:
        results["Mode 2 (CBA + top_rules)"] = test_mode2_pyarc_cba_with_top_rules()
    except Exception as e:
        print(f"\n✗ Mode 2 (CBA + top_rules) FAILED: {e}")
        results["Mode 2 (CBA + top_rules)"] = False

    # Summary
    print("\n" + "=" * 70)
    print("VERIFICATION RESULTS")
    print("=" * 70)
    for test, passed in results.items():
        status = "✓ PASS" if passed else "✗ FAIL"
        print(f"{status} - {test}")

    all_passed = all(results.values())
    print("\n" + "=" * 70)
    if all_passed:
        print("🎉 SUCCESS: Both pipelines work with current Python setup!")
        print("=" * 70)
        print("\nWe can build:")
        print("  1. Standard Mining (Mode 1) using fim.arules()")
        print("     - Mimics R: arules::apriori()")
        print("     - User provides confidence/support")
        print("     - Works ✓")
        print()
        print("  2. AUTO_CONF_SUPP (Mode 2) using pyARC CBA + top_rules")
        print("     - Mimics R: rCBA::build()")
        print("     - Automatically finds thresholds")
        print("     - Works ✓")
        print()
    else:
        print("⚠ ISSUES FOUND - See errors above")
        print("=" * 70)

    return all_passed


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
