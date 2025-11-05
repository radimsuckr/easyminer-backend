"""
Test FourFold Table Calculation for pyARC Rules

This script tests whether we can accurately calculate FourFold contingency tables
(a, b, c, d) from pyARC mining results, which only provide support% and confidence%.

FourFold Table:
- a: Antecedent TRUE  & Consequent TRUE
- b: Antecedent TRUE  & Consequent FALSE
- c: Antecedent FALSE & Consequent TRUE
- d: Antecedent FALSE & Consequent FALSE

Total: a + b + c + d = N (total transactions)
"""

import pandas as pd
from pyarc import TransactionDB
from pyarc.algorithms import createCARs, top_rules


def calculate_fourfold_table(rule, transactions_df, target_col, total_transactions):
    """
    Calculate the FourFold table (a, b, c, d) for a given rule.

    Args:
        rule: pyARC CAR object with antecedent, consequent, support, confidence
        transactions_df: Original DataFrame with all transactions
        target_col: Name of the target/consequent column
        total_transactions: Total number of transactions (N)

    Returns:
        dict with keys: a, b, c, d
    """
    # Extract rule components
    antecedent_items = rule.antecedent  # List of (attribute, value) tuples
    consequent_attr = rule.consequent.attribute
    consequent_val = rule.consequent.value

    support_pct = rule.support  # Percentage
    confidence_pct = rule.confidence  # Percentage

    # Calculate a: transactions where both antecedent AND consequent are true
    # support% = a / N
    a = int(round((support_pct / 100.0) * total_transactions))

    # Calculate b: transactions where antecedent is TRUE but consequent is FALSE
    # confidence% = a / (a + b)
    # Therefore: a + b = a / (confidence / 100)
    if confidence_pct > 0:
        a_plus_b = a / (confidence_pct / 100.0)
        b = int(round(a_plus_b - a))
    else:
        b = 0

    # Calculate c: transactions where antecedent is FALSE but consequent is TRUE
    # We need to count transactions matching consequent but not all antecedent items
    mask_consequent = transactions_df[consequent_attr] == consequent_val

    # Build antecedent mask
    mask_antecedent = pd.Series([True] * len(transactions_df), index=transactions_df.index)
    for attr, val in antecedent_items:
        mask_antecedent &= transactions_df[attr] == val

    # c = count where consequent is true AND antecedent is false
    c = int((mask_consequent & ~mask_antecedent).sum())

    # Calculate d: transactions where both are FALSE
    d = total_transactions - a - b - c

    # Validation: all values should be non-negative
    if a < 0 or b < 0 or c < 0 or d < 0:
        print("⚠️  WARNING: Negative values detected!")
        print(f"   a={a}, b={b}, c={c}, d={d}")
        print(f"   Rule: {rule}")
        print(f"   Support={support_pct}%, Confidence={confidence_pct}%")

    return {
        "a": max(0, a),
        "b": max(0, b),
        "c": max(0, c),
        "d": max(0, d),
    }


def test_fourfold_with_pyarc(csv_path, target_col="WillReturn", target_rule_count=20, max_length=5):
    """
    Test FourFold table calculation with pyARC mining.
    """
    print("=" * 80)
    print("FOURFOLD TABLE CALCULATION TEST - pyARC")
    print("=" * 80)

    # Load dataset
    print(f"\n1. Loading dataset: {csv_path}")
    df = pd.read_csv(csv_path)
    total_transactions = len(df)
    print(f"   Dataset: {total_transactions} rows × {len(df.columns)} columns")

    # Prepare transaction database
    print(f"\n2. Preparing TransactionDB (target='{target_col}')")
    txn_db = TransactionDB.from_DataFrame(df, target=target_col)
    transactions = txn_db.string_representation
    print(f"   Transactions: {len(transactions)}")

    # Build appearance constraints
    print("\n3. Building appearance constraints")
    appearance = {}

    # Mark target values as output (consequent only)
    for val in df[target_col].unique():
        appearance[f"{target_col}:=:{val}"] = "o"
        print(f"   - {target_col}:=:{val} → output (consequent)")

    # Mark ALL other items as input (antecedent only)
    input_count = 0
    for col in df.columns:
        if col != target_col:
            for val in df[col].unique():
                appearance[f"{col}:=:{val}"] = "i"
                input_count += 1
    print(f"   - {input_count} items → input (antecedent)")

    # Mine rules with pyARC top_rules
    print("\n4. Mining rules with pyARC top_rules")
    print(f"   Parameters: target_rule_count={target_rule_count}, max_length={max_length}")

    raw_rules = top_rules(
        transactions,
        appearance=appearance,
        target_rule_count=target_rule_count,
        init_support=1.0,
        init_conf=50.0,
        conf_step=5.0,
        supp_step=5.0,
        minlen=1,
        init_maxlen=min(3, max_length),
        total_timeout=30.0,
        max_iterations=20,
    )

    # Convert to CARs
    cars = createCARs(raw_rules)
    print(f"   ✓ Mined {len(cars)} CARs")

    # Filter by max_length and sort
    filtered_rules = [r for r in cars if len(r.antecedent) <= max_length]
    sorted_rules = sorted(filtered_rules, key=lambda r: (-r.confidence, -r.support))[:target_rule_count]
    print(f"   ✓ After filtering: {len(sorted_rules)} rules")

    # Calculate FourFold tables for all rules
    print("\n5. Calculating FourFold tables")
    print("=" * 80)

    results = []
    for i, rule in enumerate(sorted_rules[:10], 1):  # Show first 10
        # Format rule for display
        ante_str = " AND ".join([f"{item[0]}={item[1]}" for item in rule.antecedent]) if rule.antecedent else "∅"
        cons_str = f"{rule.consequent.attribute}={rule.consequent.value}"

        # Calculate FourFold table
        fourfold = calculate_fourfold_table(rule, df, target_col, total_transactions)

        # Verify: a + b + c + d should equal total_transactions
        sum_check = fourfold["a"] + fourfold["b"] + fourfold["c"] + fourfold["d"]
        check_ok = sum_check == total_transactions

        # Store result
        result = {
            "rule_num": i,
            "antecedent": ante_str,
            "consequent": cons_str,
            "support": rule.support,
            "confidence": rule.confidence,
            "a": fourfold["a"],
            "b": fourfold["b"],
            "c": fourfold["c"],
            "d": fourfold["d"],
            "sum": sum_check,
            "sum_ok": check_ok,
        }
        results.append(result)

        # Display
        print(f"\nRule {i}:")
        print(f"  IF {ante_str}")
        print(f"  THEN {cons_str}")
        print(f"  Support: {rule.support:.2f}%, Confidence: {rule.confidence:.2f}%")
        print("  FourFold Table:")
        print(f"    a (both TRUE):        {fourfold['a']:6d}")
        print(f"    b (ante TRUE, cons FALSE): {fourfold['b']:6d}")
        print(f"    c (ante FALSE, cons TRUE): {fourfold['c']:6d}")
        print(f"    d (both FALSE):       {fourfold['d']:6d}")
        print("    -----------------------------------")
        print(f"    Sum (a+b+c+d):        {sum_check:6d} {'✓' if check_ok else '✗ MISMATCH!'}")
        print(f"    Expected (N):         {total_transactions:6d}")

        # Verify support and confidence calculations
        calculated_support = (fourfold["a"] / total_transactions) * 100 if total_transactions > 0 else 0
        calculated_confidence = (
            (fourfold["a"] / (fourfold["a"] + fourfold["b"])) * 100 if (fourfold["a"] + fourfold["b"]) > 0 else 0
        )

        print("  Verification:")
        print(f"    Original support:     {rule.support:.2f}%")
        print(
            f"    Calculated support:   {calculated_support:.2f}% {'✓' if abs(rule.support - calculated_support) < 0.5 else '✗'}"
        )
        print(f"    Original confidence:  {rule.confidence:.2f}%")
        print(
            f"    Calculated confidence: {calculated_confidence:.2f}% {'✓' if abs(rule.confidence - calculated_confidence) < 0.5 else '✗'}"
        )

    # Summary
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"Total rules tested: {len(results)}")
    print(f"Rules with valid sum: {sum(1 for r in results if r['sum_ok'])}/{len(results)}")

    all_ok = all(r["sum_ok"] for r in results)
    if all_ok:
        print("\n✅ SUCCESS! All FourFold tables calculated correctly!")
        print("   FourFold table calculation is working as expected.")
    else:
        print("\n⚠️  WARNING! Some FourFold tables have mismatches.")
        print("   Review the calculation logic.")

    return results


if __name__ == "__main__":
    # Use the smaller dataset (100k rows)
    import sys

    csv_path = "../performance_test_data.csv"

    if len(sys.argv) > 1:
        csv_path = sys.argv[1]

    results = test_fourfold_with_pyarc(csv_path=csv_path, target_col="WillReturn", target_rule_count=20, max_length=5)
