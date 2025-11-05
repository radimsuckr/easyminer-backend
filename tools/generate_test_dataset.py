"""
Generate a synthetic dataset for performance testing rule mining algorithms.
Creates a CSV file with ~2500 rows and 12 categorical attributes (~40-50 KB).
"""

import csv
import random
from pathlib import Path

random.seed(42)  # For reproducibility


def generate_customer_shopping_data(num_rows=100000):
    """
    Generate synthetic customer shopping behavior data with high variability.
    Includes correlations to ensure interesting rules can be mined.
    """

    # Define attribute values - MORE VARIED for complex mining
    age_groups = ["18-22", "23-27", "28-32", "33-37", "38-42", "43-47", "48-52", "53-57", "58-62", "63-67", "68+"]
    genders = ["Male", "Female", "NonBinary", "PreferNotToSay"]
    membership = ["None", "Bronze", "Silver", "Gold", "Platinum", "Diamond"]
    income_levels = ["VeryLow", "Low", "BelowMedium", "Medium", "AboveMedium", "High", "VeryHigh", "Wealthy"]
    cities = ["Urban", "Suburban", "Rural", "Metropolitan", "Town", "Village"]

    # Shopping patterns - MORE VARIED
    shopping_frequency = ["Daily", "TwiceWeek", "Weekly", "BiWeekly", "Monthly", "Quarterly", "Rarely"]
    preferred_category = [
        "Electronics",
        "Clothing",
        "Food",
        "Home",
        "Sports",
        "Books",
        "Beauty",
        "Garden",
        "Toys",
        "Automotive",
    ]
    payment_method = ["Cash", "Credit", "Debit", "Mobile", "Crypto", "Voucher", "PayLater"]
    discount_usage = ["Never", "Rarely", "Sometimes", "Often", "Always", "OnlyWithCoupon"]

    # Purchase outcomes
    avg_basket = ["Tiny", "Small", "Medium", "Large", "VeryLarge", "Huge"]
    satisfaction = ["VeryLow", "Low", "Medium", "High", "VeryHigh"]
    will_return = ["DefinitelyNo", "ProbablyNo", "Maybe", "ProbablyYes", "DefinitelyYes"]

    data: list[dict[str, str]] = []

    for _ in range(num_rows):
        # Base attributes
        age = random.choice(age_groups)
        gender = random.choice(genders)

        # Add correlations for interesting rules

        # Correlation: Age influences membership and income
        age_num = int(age.split("-")[0]) if "-" in age else 68
        if age_num < 30:
            income = random.choices(income_levels, weights=[0.25, 0.25, 0.2, 0.15, 0.08, 0.05, 0.015, 0.005])[0]
            member = random.choices(membership, weights=[0.5, 0.2, 0.15, 0.1, 0.04, 0.01])[0]
        elif age_num < 45:
            income = random.choices(income_levels, weights=[0.05, 0.1, 0.15, 0.25, 0.2, 0.15, 0.08, 0.02])[0]
            member = random.choices(membership, weights=[0.2, 0.15, 0.25, 0.2, 0.15, 0.05])[0]
        elif age_num < 60:
            income = random.choices(income_levels, weights=[0.03, 0.07, 0.1, 0.15, 0.25, 0.25, 0.12, 0.03])[0]
            member = random.choices(membership, weights=[0.15, 0.1, 0.2, 0.25, 0.2, 0.1])[0]
        else:
            income = random.choices(income_levels, weights=[0.1, 0.15, 0.2, 0.25, 0.15, 0.1, 0.04, 0.01])[0]
            member = random.choices(membership, weights=[0.25, 0.15, 0.25, 0.2, 0.1, 0.05])[0]

        city = random.choice(cities)

        # Correlation: Income and membership influence shopping frequency
        if income in ["High", "VeryHigh", "Wealthy"] or member in ["Gold", "Platinum", "Diamond"]:
            frequency = random.choices(shopping_frequency, weights=[0.25, 0.25, 0.2, 0.15, 0.1, 0.03, 0.02])[0]
        else:
            frequency = random.choices(shopping_frequency, weights=[0.02, 0.05, 0.1, 0.15, 0.3, 0.25, 0.13])[0]

        # Correlation: Age and gender influence preferred category
        if age_num < 30:
            if gender == "Male":
                category = random.choices(
                    preferred_category, weights=[0.3, 0.1, 0.15, 0.05, 0.2, 0.05, 0.02, 0.03, 0.05, 0.05]
                )[0]
            else:
                category = random.choices(
                    preferred_category, weights=[0.1, 0.25, 0.15, 0.1, 0.08, 0.07, 0.15, 0.05, 0.03, 0.02]
                )[0]
        else:
            category = random.choices(
                preferred_category, weights=[0.08, 0.15, 0.25, 0.18, 0.1, 0.08, 0.05, 0.06, 0.03, 0.02]
            )[0]

        # Correlation: Membership influences payment and discount usage
        if member in ["Gold", "Platinum", "Diamond"]:
            payment = random.choices(payment_method, weights=[0.02, 0.5, 0.2, 0.15, 0.08, 0.03, 0.02])[0]
            discount = random.choices(discount_usage, weights=[0.02, 0.05, 0.1, 0.3, 0.4, 0.13])[0]
        elif member in ["Silver", "Bronze"]:
            payment = random.choices(payment_method, weights=[0.08, 0.35, 0.3, 0.18, 0.05, 0.02, 0.02])[0]
            discount = random.choices(discount_usage, weights=[0.05, 0.15, 0.25, 0.3, 0.2, 0.05])[0]
        else:
            payment = random.choices(payment_method, weights=[0.2, 0.2, 0.25, 0.2, 0.03, 0.05, 0.07])[0]
            discount = random.choices(discount_usage, weights=[0.2, 0.25, 0.3, 0.15, 0.08, 0.02])[0]

        # Correlation: Frequency and category influence basket size
        if frequency in ["Daily", "TwiceWeek", "Weekly"] and category in ["Food", "Home"]:
            basket = random.choices(avg_basket, weights=[0.05, 0.1, 0.2, 0.3, 0.25, 0.1])[0]
        else:
            basket = random.choices(avg_basket, weights=[0.25, 0.3, 0.25, 0.12, 0.06, 0.02])[0]

        # Correlation: Basket size, discount, and membership influence satisfaction
        satisfaction_weights = [0.15, 0.25, 0.3, 0.2, 0.1]
        if basket in ["Large", "VeryLarge", "Huge"] and discount in ["Often", "Always", "OnlyWithCoupon"]:
            satisfaction_weights = [0.02, 0.05, 0.15, 0.4, 0.38]
        elif member in ["Gold", "Platinum", "Diamond"]:
            satisfaction_weights = [0.05, 0.1, 0.2, 0.35, 0.3]

        satis = random.choices(satisfaction, weights=satisfaction_weights)[0]

        # Correlation: Satisfaction strongly influences return intention
        if satis in ["VeryHigh", "High"]:
            return_intent = random.choices(will_return, weights=[0.02, 0.05, 0.1, 0.3, 0.53])[0]
        elif satis == "Medium":
            return_intent = random.choices(will_return, weights=[0.1, 0.15, 0.5, 0.2, 0.05])[0]
        else:
            return_intent = random.choices(will_return, weights=[0.4, 0.35, 0.2, 0.04, 0.01])[0]

        row = {
            "Age": age,
            "Gender": gender,
            "Membership": member,
            "Income": income,
            "City": city,
            "Frequency": frequency,
            "Category": category,
            "Payment": payment,
            "Discount": discount,
            "Basket": basket,
            "Satisfaction": satis,
            "WillReturn": return_intent,
        }

        data.append(row)

    return data


def save_to_csv(data: list[dict[str, str]], filename: str = "performance_test_data.csv"):
    """Save data to CSV file."""
    if not data:
        return

    filepath = Path(__file__).parent / filename

    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)

    # Print file stats
    size_kb = filepath.stat().st_size / 1024
    print(f"Generated dataset: {filename}")
    print(f"Rows: {len(data)}")
    print(f"Columns: {len(data[0])}")
    print(f"Size: {size_kb:.1f} KB")

    print("\nAttribute cardinalities:")
    if data:
        for col in data[0].keys():
            unique_vals = len(set(row[col] for row in data))
            print(f"  {col}: {unique_vals} values")

    print("\nSample correlations built into the data:")
    print("- Age → Income, Membership")
    print("- Income/Membership → Shopping Frequency")
    print("- Age/Gender → Preferred Category")
    print("- Membership → Payment Method, Discount Usage")
    print("- Frequency/Category → Basket Size")
    print("- Basket/Discount/Membership → Satisfaction")
    print("- Satisfaction → Will Return (Target)")


if __name__ == "__main__":
    data = generate_customer_shopping_data(num_rows=100000)
    save_to_csv(data)
