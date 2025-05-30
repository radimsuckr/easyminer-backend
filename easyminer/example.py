import pandas as pd
from efficient_apriori import apriori


def run_minimal_apriori_example():
    """
    A minimal example demonstrating association rule mining
    with efficient-apriori and static data.
    """
    print("--- Minimal Apriori Example with Static Data ---")

    # 1. Static Transaction Data
    data = {
        "TransactionID": [1, 1, 1, 2, 2, 3, 3, 3, 3, 4, 4, 5, 5, 5],
        "Item": [
            "Milk",
            "Bread",
            "Butter",  # T1
            "Milk",
            "Bread",  # T2
            "Bread",
            "Butter",
            "Eggs",
            "Cheese",  # T3
            "Milk",
            "Eggs",  # T4
            "Milk",
            "Bread",
            "Butter",  # T5
        ],
    }
    df = pd.DataFrame(data)
    print("\nInitial Data (Pandas DataFrame):")
    print(df)

    # 2. Prepare transactions for efficient-apriori
    transactions = df.groupby("TransactionID")["Item"].apply(lambda x: tuple(set(x))).tolist()

    print("\nTransactions (list of tuples for efficient-apriori):")
    for i, t in enumerate(transactions):
        print(f"  T{i + 1}: {t}")

    # 3. Run Apriori algorithm
    itemsets, rules = apriori(transactions, min_support=0.4, min_confidence=0.6)

    print("\n--- Frequent Itemsets ---")
    if not itemsets:
        print("No frequent itemsets found with the given support.")
    else:
        for k, v in itemsets.items():
            print(f"\nItemsets of length {k}:")
            for itemset, support_count in v.items():
                support_value = support_count / len(transactions)
                print(f"  {itemset}: support_count={support_count}, support_value={support_value:.2f}")

    print("\n--- Association Rules ---")
    if not rules:
        print("No association rules found with the given support and confidence.")
    else:
        rules_sorted = sorted(rules, key=lambda rule: rule.confidence, reverse=True)
        for rule in rules_sorted:
            print(f"Rule: {rule.lhs} -> {rule.rhs}")
            print(f"  Support: {rule.support:.2f} (Count: {int(rule.support * len(transactions))})")
            print(f"  Confidence: {rule.confidence:.2f}")
            print(f"  Lift: {rule.lift:.2f}")
            # conviction can sometimes be inf, handle that for printing
            conviction_str = f"{rule.conviction:.2f}" if rule.conviction != float("inf") else "inf"
            print(f"  Conviction: {conviction_str}")
            # Calculate coverage (LHS support)
            lhs_support_value = rule.count_lhs / len(transactions)
            print(f"  Coverage (LHS support): {lhs_support_value:.2f} (Count: {rule.count_lhs})")
            print("-" * 20)

    print("\n--- Example Finished ---")


if __name__ == "__main__":
    run_minimal_apriori_example()
