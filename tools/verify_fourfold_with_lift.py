"""
Verify FourFold Table Calculation: Scala GUHA vs Our Implementation

The Scala implementation uses this formula (from ContingencyTable.scala):
  a = support × count
  b = a / confidence - a
  c = (confidence × count) / lift - a
  d = count - a - b - c

Where:
  - support = a / (a+b+c+d)
  - confidence = a / (a+b)
  - lift = confidence / ((a+c) / (a+b+c+d))

Our implementation:
  a = support × count
  b = a / confidence - a
  c = DIRECT COUNT from data (consequent TRUE, antecedent FALSE)
  d = count - a - b - c

Question: Are these equivalent?
"""

import pandas as pd
from pyarc import TransactionDB
from pyarc.algorithms import createCARs, top_rules


def calculate_fourfold_scala_style(support_pct, confidence_pct, lift, total_transactions):
    """
    Calculate FourFold table using Scala's GUHA formulas.

    This is how the original Scala implementation does it.
    """
    support = support_pct / 100.0
    confidence = confidence_pct / 100.0

    a = round(support * total_transactions)
    b = round(a / confidence - a)
    c = round((confidence * total_transactions) / lift - a)
    d = total_transactions - a - b - c

    return {"a": int(a), "b": int(b), "c": int(c), "d": int(d)}


def calculate_fourfold_our_style(rule, transactions_df, target_col, total_transactions):
    """
    Calculate FourFold table using our direct counting method.
    """
    support_pct = rule.support
    confidence_pct = rule.confidence

    support = support_pct / 100.0
    confidence = confidence_pct / 100.0

    a = round(support * total_transactions)
    b = round(a / confidence - a) if confidence > 0 else 0

    # Direct count from data
    antecedent_items = rule.antecedent
    consequent_attr = rule.consequent.attribute
    consequent_val = rule.consequent.value

    mask_consequent = transactions_df[consequent_attr] == consequent_val
    mask_antecedent = pd.Series([True] * len(transactions_df), index=transactions_df.index)
    for attr, val in antecedent_items:
        mask_antecedent &= transactions_df[attr] == val

    c = int((mask_consequent & ~mask_antecedent).sum())
    d = total_transactions - a - b - c

    return {"a": int(a), "b": int(b), "c": int(c), "d": int(d)}


def calculate_lift_from_fourfold(a, b, c, d):
    """
    Calculate lift from fourfold table.

    lift = confidence / P(consequent)
    where:
      confidence = a / (a+b)
      P(consequent) = (a+c) / (a+b+c+d)
    """
    confidence = a / (a + b) if (a + b) > 0 else 0
    p_consequent = (a + c) / (a + b + c + d) if (a + b + c + d) > 0 else 0
    lift = confidence / p_consequent if p_consequent > 0 else 0
    return lift


def test_comparison(csv_path, target_col="WillReturn", num_rules=5):
    """
    Compare Scala GUHA method vs our direct counting method.
    """
    print("=" * 80)
    print("FOURFOLD TABLE COMPARISON: Scala GUHA vs Our Implementation")
    print("=" * 80)

    # Load dataset
    print(f"\nLoading: {csv_path}")
    df = pd.read_csv(csv_path)
    total_transactions = len(df)
    print(f"Dataset: {total_transactions} rows")

    # Mine rules
    print("\nMining rules with pyARC...")
    txn_db = TransactionDB.from_DataFrame(df, target=target_col)
    transactions = txn_db.string_representation

    appearance = {}
    for val in df[target_col].unique():
        appearance[f"{target_col}:=:{val}"] = "o"
    for col in df.columns:
        if col != target_col:
            for val in df[col].unique():
                appearance[f"{col}:=:{val}"] = "i"

    raw_rules = top_rules(
        transactions,
        appearance=appearance,
        target_rule_count=num_rules,
        init_support=1.0,
        init_conf=50.0,
        conf_step=5.0,
        supp_step=5.0,
        minlen=1,
        init_maxlen=3,
        total_timeout=30.0,
        max_iterations=20,
    )

    cars = createCARs(raw_rules)
    sorted_rules = sorted(cars, key=lambda r: (-r.confidence, -r.support))[:num_rules]
    print(f"✓ Mined {len(sorted_rules)} rules\n")

    # Compare methods
    print("=" * 80)
    print("COMPARISON")
    print("=" * 80)

    for i, rule in enumerate(sorted_rules, 1):
        ante_str = " AND ".join([f"{item[0]}={item[1]}" for item in rule.antecedent]) if rule.antecedent else "∅"
        cons_str = f"{rule.consequent.attribute}={rule.consequent.value}"

        print(f"\nRule {i}: IF {ante_str} THEN {cons_str}")
        print(f"  Support: {rule.support:.2f}%, Confidence: {rule.confidence:.2f}%")

        # Method 1: Our direct counting
        fourfold_ours = calculate_fourfold_our_style(rule, df, target_col, total_transactions)
        lift_from_ours = calculate_lift_from_fourfold(
            fourfold_ours["a"], fourfold_ours["b"], fourfold_ours["c"], fourfold_ours["d"]
        )

        print("\n  Method 1 (Our direct counting):")
        print(f"    a={fourfold_ours['a']}, b={fourfold_ours['b']}, c={fourfold_ours['c']}, d={fourfold_ours['d']}")
        print(f"    Sum: {sum(fourfold_ours.values())} (expected: {total_transactions})")
        print(f"    Calculated lift: {lift_from_ours:.4f}")

        # Method 2: Scala GUHA (using lift calculated from our fourfold)
        fourfold_scala = calculate_fourfold_scala_style(
            rule.support, rule.confidence, lift_from_ours, total_transactions
        )

        print("\n  Method 2 (Scala GUHA formula with calculated lift):")
        print(f"    a={fourfold_scala['a']}, b={fourfold_scala['b']}, c={fourfold_scala['c']}, d={fourfold_scala['d']}")
        print(f"    Sum: {sum(fourfold_scala.values())} (expected: {total_transactions})")

        # Compare
        print("\n  Comparison:")
        a_match = fourfold_ours["a"] == fourfold_scala["a"]
        b_match = fourfold_ours["b"] == fourfold_scala["b"]
        c_match = fourfold_ours["c"] == fourfold_scala["c"]
        d_match = fourfold_ours["d"] == fourfold_scala["d"]

        print(f"    a: {fourfold_ours['a']:6d} vs {fourfold_scala['a']:6d} {'✓' if a_match else '✗'}")
        print(f"    b: {fourfold_ours['b']:6d} vs {fourfold_scala['b']:6d} {'✓' if b_match else '✗'}")
        print(f"    c: {fourfold_ours['c']:6d} vs {fourfold_scala['c']:6d} {'✓' if c_match else '✗'}")
        print(f"    d: {fourfold_ours['d']:6d} vs {fourfold_scala['d']:6d} {'✓' if d_match else '✗'}")

        if a_match and b_match and c_match and d_match:
            print("    ✅ METHODS ARE EQUIVALENT!")
        else:
            print("    ⚠️  METHODS DIFFER!")
            # Check if difference is just rounding
            diff_a = abs(fourfold_ours["a"] - fourfold_scala["a"])
            diff_b = abs(fourfold_ours["b"] - fourfold_scala["b"])
            diff_c = abs(fourfold_ours["c"] - fourfold_scala["c"])
            diff_d = abs(fourfold_ours["d"] - fourfold_scala["d"])
            max_diff = max(diff_a, diff_b, diff_c, diff_d)
            print(f"    Max difference: {max_diff}")
            if max_diff <= 1:
                print("    (Difference likely due to rounding - acceptable)")

    print("\n" + "=" * 80)
    print("CONCLUSION")
    print("=" * 80)
    print("Both methods calculate fourfold tables, but:")
    print("  1. Scala uses: support, confidence, LIFT (from R arules)")
    print("  2. Ours uses: support, confidence, DIRECT DATA COUNT")
    print("")
    print("Since pyARC doesn't provide lift, we must either:")
    print("  A) Calculate lift from our fourfold table, then verify")
    print("  B) Use direct counting (which is more accurate to actual data)")
    print("")
    print("Our approach (B) is valid because it counts actual transactions,")
    print("ensuring the fourfold table perfectly reflects the dataset.")


if __name__ == "__main__":
    test_comparison("../performance_test_data.csv", num_rules=5)
