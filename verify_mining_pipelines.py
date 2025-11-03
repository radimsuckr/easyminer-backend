"""
Verification Script: Prove both mining pipelines work with available Python packages

This script tests:
1. Mode 1 (Standard): arules::apriori() equivalent using fim.arules()
2. Mode 2 (AUTO_CONF_SUPP): rCBA::build() equivalent using pyARC top_rules + optional CBA

CORRECTED: CBA pruning is optional and controlled by separate CBA interest measure.
"""

import fim
import pandas as pd
from pyarc import TransactionDB
from pyarc.algorithms import M1Algorithm, M2Algorithm, createCARs, top_rules


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

    # Set up appearance constraints (consequent must be target attribute)
    # fim appearance: dict of {item: 'i'/'o'/'b'/'n'}
    # 'o' = output (consequent only), 'i' = input (antecedent only)
    appearance = {}

    # Mark target values as output (consequent only)
    for val in df[target].unique():
        appearance[f"{target}:=:{val}"] = "o"

    # Mark ALL other items as input (antecedent only)
    for col in df.columns:
        if col != target:
            for val in df[col].unique():
                appearance[f"{col}:=:{val}"] = "i"

    rules = fim.arules(
        transactions,
        supp=user_support,  # User-provided
        conf=user_confidence,  # User-provided
        mode="o",  # Original apriori
        report="sc",  # Report support & confidence
        appear=appearance if appearance else None,
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


def test_mode2_pyarc_top_rules():
    """
    Mode 2: AUTO_CONF_SUPP with automatic threshold detection
    Mimics: R's rCBA::build(txns, className="PlayTennis")

    CORRECTED: CBA pruning is OPTIONAL and controlled by separate CBA interest measure.
    AUTO_CONF_SUPP = automatic threshold detection
    CBA = M1/M2 pruning (independent flag)
    """
    print("\n" + "=" * 70)
    print("MODE 2: AUTO_CONF_SUPP (top_rules + optional CBA)")
    print("=" * 70)

    df = create_test_dataset()
    target = "PlayTennis"

    print(f"Dataset: {len(df)} rows, target='{target}'")

    # NO user-specified thresholds!
    # System automatically finds optimal confidence/support
    target_rule_count = 10  # User says: "Give me ~10 good rules"
    max_rule_length = 5
    apply_cba_pruning = True  # Let's test WITHOUT pruning to see all rules

    print("\nAutomatic threshold detection:")
    print(f"  Target rule count: {target_rule_count}")
    print(f"  Max rule length: {max_rule_length}")
    print(f"  CBA pruning: {'YES' if apply_cba_pruning else 'NO (CBA interest measure not present)'}")

    # Prepare transaction database
    txn_db = TransactionDB.from_DataFrame(df, target=target)
    transactions = txn_db.string_representation

    print(f"\nTransaction DB: {len(txn_db)} transactions")

    # Set up appearance (consequent constraints)
    # CRITICAL: Must specify ALL items, not just target!
    appearance = {}

    # Mark target values as output (consequent only)
    for val in df[target].unique():
        appearance[f"{target}:=:{val}"] = "o"

    # Mark ALL other items as input (antecedent only)
    for col in df.columns:
        if col != target:
            for val in df[col].unique():
                appearance[f"{col}:=:{val}"] = "i"

    # Use top_rules for automatic threshold detection
    print("\nStep 1: Mining with top_rules (automatic threshold detection)...")

    raw_rules = top_rules(
        transactions,
        appearance=appearance,
        target_rule_count=target_rule_count,
        init_support=1.0,
        init_conf=50.0,
        conf_step=5.0,
        supp_step=5.0,
        minlen=1,
        init_maxlen=min(3, max_rule_length),
        total_timeout=30.0,
        max_iterations=20,
    )

    print(f"  → Mined {len(raw_rules)} rules with automatic thresholds")
    print(f"     (appearance constraint ensures all consequents are {target})")

    # Convert to CARs (no filtering needed - appearance already handled it!)
    cars = createCARs(raw_rules)
    print(f"  → Created {len(cars)} CARs")

    # Filter by max rule length
    cars = [car for car in cars if len(car.antecedent) <= max_rule_length]
    print(f"  → After length filter (≤{max_rule_length}): {len(cars)} rules")

    # Optionally apply CBA pruning (only if CBA interest measure present)
    if apply_cba_pruning and cars:
        print("\nStep 2: Applying CBA pruning (M1/M2)...")
        m1_clf = M1Algorithm(cars, txn_db).build()
        print(f"  → After M1: {len(m1_clf.rules)} rules")

        m2_clf = M2Algorithm(m1_clf.rules, txn_db).build()
        print(f"  → After M2: {len(m2_clf.rules)} rules")

        final_rules = m2_clf.rules
        accuracy = m2_clf.test_transactions(txn_db)
        print(f"  → Accuracy: {accuracy:.2%}")
    else:
        print("\nStep 2: Skipping CBA pruning (CBA interest measure not present)")
        final_rules = cars
        accuracy = None

    # Limit to target_rule_count (sort by confidence desc, support desc)
    print("\nStep 3: Limiting to target rule count...")
    final_rules = sorted(final_rules, key=lambda r: (-r.confidence, -r.support))
    final_rules = final_rules[:target_rule_count]
    print(f"  → Final output: {len(final_rules)} rules")

    print("\n✓ AUTO_CONF_SUPP completed successfully!")
    if accuracy:
        print(f"  Accuracy: {accuracy:.2%}")

    # Display sample rules
    if final_rules:
        print("\nSample rules (first 10):")
        for i, rule in enumerate(final_rules[:10]):
            ante_str = " AND ".join([f"{a[0]}={a[1]}" for a in rule.antecedent]) if rule.antecedent else "∅"
            print(f"  {i + 1}. IF {ante_str}")
            print(f"     THEN {rule.consequent.attribute}={rule.consequent.value}")
            print(f"     [conf={rule.confidence:.1f}%, supp={rule.support:.1f}%]")

    return len(final_rules) > 0


def main():
    """Run all verification tests"""
    print("\n" + "=" * 70)
    print("MINING PIPELINE VERIFICATION (CORRECTED)")
    print("Testing: Can we build both Scala/R mining modes in Python?")
    print("=" * 70)

    results = {}

    # Test Mode 1: Standard mining
    try:
        results["Mode 1 (fim.arules)"] = test_mode1_fim_arules()
    except Exception as e:
        print(f"\n✗ Mode 1 (fim.arules) FAILED: {e}")
        import traceback

        traceback.print_exc()
        results["Mode 1 (fim.arules)"] = False

    # Test Mode 2: AUTO_CONF_SUPP
    try:
        results["Mode 2 (top_rules + optional CBA)"] = test_mode2_pyarc_top_rules()
    except Exception as e:
        print(f"\n✗ Mode 2 (top_rules + optional CBA) FAILED: {e}")
        import traceback

        traceback.print_exc()
        results["Mode 2 (top_rules + optional CBA)"] = False

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
        print("  2. AUTO_CONF_SUPP (Mode 2) using pyARC top_rules")
        print("     - Mimics R: rCBA::build()")
        print("     - Automatically finds thresholds")
        print("     - Optional CBA pruning (M1/M2)")
        print("     - Works ✓")
        print()
        print("KEY FIX: CBA pruning is now optional!")
        print("  - AUTO_CONF_SUPP alone → auto thresholds, NO pruning")
        print("  - AUTO_CONF_SUPP + CBA → auto thresholds, WITH pruning")
    else:
        print("⚠ ISSUES FOUND - See errors above")
        print("=" * 70)

    return all_passed


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
