"""
Performance comparison between CleverMiner, fim.arules, and pyARC for association rule mining.

Note: mlxtend was evaluated but excluded from benchmarks due to:
- No native appearance constraint support (generates ALL rules then filters)
- Poor scaling on complex queries (5.1x slower than fim on permissive settings)
- Often finds 0 rules due to lack of directed mining
- No competitive advantage over fim/pyARC for production use

mlxtend test results (100k rows dataset):
- Very restrictive (supp=0.20, conf=0.80, len=3): 356ms, 0 rules (4x faster than fim)
- Moderate (supp=0.05, conf=0.60, len=4): 1418ms, 0 rules (same as fim)
- Permissive (supp=0.02, conf=0.50, len=5): 7742ms, 70 rules (5.1x slower than fim)
"""

import time
from pathlib import Path

import pandas as pd


def benchmark_cleverminer(df, target_col="WillReturn", min_support=0.1, min_confidence=0.6, max_length=5):
    """Benchmark CleverMiner mining."""
    try:
        from cleverminer.cleverminer import cleverminer

        start_time = time.perf_counter()

        # Build antecedent and consequent settings
        other_cols = [col for col in df.columns if col != target_col]

        # Mine rules with CleverMiner
        cm = cleverminer(
            df=df,
            proc="4ftMiner",
            quantifiers={"conf": min_confidence, "Base": int(len(df) * min_support)},
            ante={
                "attributes": [{"name": col, "type": "subset", "minlen": 1, "maxlen": 1} for col in other_cols],
                "minlen": 1,
                "maxlen": max_length - 1,  # Reserve 1 for consequent
                "type": "con",
            },
            succ={
                "attributes": [{"name": target_col, "type": "subset", "minlen": 1, "maxlen": 1}],
                "minlen": 1,
                "maxlen": 1,
                "type": "con",
            },
        )

        end_time = time.perf_counter()
        duration = end_time - start_time

        # Extract rules
        raw_rules = cm.result.get("rules", [])
        rule_count = len(raw_rules)

        # Format rules for display
        formatted_rules = []
        for rule in raw_rules[:20]:  # Top 20
            cedents = rule.get("cedents_struct", {})
            ante_items = []
            for key, values in cedents.get("ante", {}).items():
                for val in values:
                    ante_items.append(f"{key}={val}")

            cons_dict = cedents.get("succ", {})
            cons_items = []
            for key, values in cons_dict.items():
                for val in values:
                    cons_items.append(f"{key}={val}")

            params = rule.get("params", {})
            formatted_rules.append(
                {
                    "antecedent": " AND ".join(ante_items) if ante_items else "∅",
                    "consequent": " AND ".join(cons_items) if cons_items else "?",
                    "confidence": params.get("conf", 0),
                    "support": params.get("rel_base", 0),
                }
            )

        return {
            "method": "CleverMiner",
            "duration_ms": duration * 1000,
            "rule_count": rule_count,
            "rules": formatted_rules,
            "success": True,
        }
    except Exception as e:
        return {
            "method": "CleverMiner",
            "duration_ms": 0,
            "rule_count": 0,
            "rules": [],
            "success": False,
            "error": str(e),
        }


def benchmark_fim_arules(df, target_col="WillReturn", min_support=0.1, min_confidence=0.6, max_length=5):
    """Benchmark fim.arules mining."""
    try:
        import fim
        from pyarc import TransactionDB

        start_time = time.perf_counter()

        # Prepare transaction database
        txn_db = TransactionDB.from_DataFrame(df, target=target_col)
        transactions = txn_db.string_representation

        # Build appearance constraints
        appearance = {}

        # Mark target values as output (consequent only)
        for val in df[target_col].unique():
            appearance[f"{target_col}:=:{val}"] = "o"

        # Mark ALL other items as input (antecedent only)
        for col in df.columns:
            if col != target_col:
                for val in df[col].unique():
                    appearance[f"{col}:=:{val}"] = "i"

        # Mine rules with fim.arules
        rules = fim.arules(
            transactions,
            supp=min_support * 100,  # fim expects percentage
            conf=min_confidence * 100,
            mode="o",  # original apriori
            report="sc",  # report support & confidence
            appear=appearance if appearance else None,
            zmax=max_length,
        )

        end_time = time.perf_counter()
        duration = end_time - start_time

        # Format rules for display (sorted by confidence desc, support desc)
        sorted_rules = sorted(rules, key=lambda r: (-r[3], -r[2]))[:20]  # Top 20
        formatted_rules = []
        for cons, ante, supp, conf in sorted_rules:
            formatted_rules.append(
                {
                    "antecedent": " AND ".join(ante) if ante else "∅",
                    "consequent": cons,
                    "confidence": conf,
                    "support": supp,
                }
            )

        return {
            "method": "fim.arules",
            "duration_ms": duration * 1000,
            "rule_count": len(rules),
            "rules": formatted_rules,
            "success": True,
        }
    except Exception as e:
        return {
            "method": "fim.arules",
            "duration_ms": 0,
            "rule_count": 0,
            "rules": [],
            "success": False,
            "error": str(e),
        }


def benchmark_pyarc_top_rules(df, target_col="WillReturn", target_rule_count=50, max_length=5):
    """Benchmark pyARC top_rules mining (AUTO_CONF_SUPP mode)."""
    try:
        from pyarc import TransactionDB
        from pyarc.algorithms import createCARs, top_rules

        start_time = time.perf_counter()

        # Prepare transaction database
        txn_db = TransactionDB.from_DataFrame(df, target=target_col)
        transactions = txn_db.string_representation

        # Build appearance constraints
        appearance = {}

        # Mark target values as output (consequent only)
        for val in df[target_col].unique():
            appearance[f"{target_col}:=:{val}"] = "o"

        # Mark ALL other items as input (antecedent only)
        for col in df.columns:
            if col != target_col:
                for val in df[col].unique():
                    appearance[f"{col}:=:{val}"] = "i"

        # Mine rules with top_rules
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

        # Filter by max_length
        filtered_rules = [r for r in cars if len(r.antecedent) <= max_length]

        # Sort and limit
        sorted_rules = sorted(filtered_rules, key=lambda r: (-r.confidence, -r.support))[:target_rule_count]

        end_time = time.perf_counter()
        duration = end_time - start_time

        # Format rules for display (top 20)
        formatted_rules = []
        for rule in sorted_rules[:20]:
            ante_str = " AND ".join([f"{item[0]}={item[1]}" for item in rule.antecedent]) if rule.antecedent else "∅"
            cons_str = f"{rule.consequent.attribute}={rule.consequent.value}"
            formatted_rules.append(
                {"antecedent": ante_str, "consequent": cons_str, "confidence": rule.confidence, "support": rule.support}
            )

        return {
            "method": "pyARC top_rules",
            "duration_ms": duration * 1000,
            "rule_count": len(sorted_rules),
            "rules": formatted_rules,
            "success": True,
        }
    except Exception as e:
        return {
            "method": "pyARC top_rules",
            "duration_ms": 0,
            "rule_count": 0,
            "rules": [],
            "success": False,
            "error": str(e),
        }


def run_benchmark(csv_path="../performance_test_data.csv"):
    """Run complete benchmark comparison."""
    print(f"Loading dataset: {csv_path}")
    df = pd.read_csv(csv_path)
    print(f"Dataset: {len(df)} rows × {len(df.columns)} columns")
    print(f"File size: {Path(csv_path).stat().st_size / 1024:.1f} KB\n")

    # Test parameters
    test_configs = [
        {
            "name": "1️⃣ Very restrictive (few rules)",
            "min_support": 0.20,
            "min_confidence": 0.80,
            "target_rule_count": 10,
            "max_length": 3,
        },
        {
            "name": "2️⃣ Moderate (100-500 rules)",
            "min_support": 0.05,
            "min_confidence": 0.60,
            "target_rule_count": 50,
            "max_length": 4,
        },
        {
            "name": "3️⃣ Permissive (1k-2k rules)",
            "min_support": 0.02,
            "min_confidence": 0.50,
            "target_rule_count": 100,
            "max_length": 5,
        },
        {
            "name": "4️⃣ Very permissive (5k-10k rules)",
            "min_support": 0.01,
            "min_confidence": 0.40,
            "target_rule_count": 200,
            "max_length": 6,
        },
        {
            "name": "5️⃣ EXTREME: Minimal thresholds",
            "min_support": 0.005,
            "min_confidence": 0.30,
            "target_rule_count": 500,
            "max_length": 7,
        },
        {
            "name": "6️⃣ EXTREME: Long complex rules",
            "min_support": 0.008,
            "min_confidence": 0.35,
            "target_rule_count": 300,
            "max_length": 9,
        },
        {
            "name": "7️⃣ STRESS: Maximum complexity",
            "min_support": 0.003,
            "min_confidence": 0.25,
            "target_rule_count": 1000,
            "max_length": 10,
        },
    ]

    results = []

    for config in test_configs:
        print("=" * 70)
        print(f"Test: {config['name']}")
        print(f"  Support: {config['min_support']}, Confidence: {config['min_confidence']}")
        print(f"  Max Length: {config['max_length']}, Target Rules: {config['target_rule_count']}")
        print("-" * 70)

        # Benchmark CleverMiner
        print("Running CleverMiner...", end=" ", flush=True)
        cm_result = benchmark_cleverminer(
            df,
            min_support=config["min_support"],
            min_confidence=config["min_confidence"],
            max_length=config["max_length"],
        )
        if cm_result["success"]:
            print(f"✓ {cm_result['duration_ms']:.1f}ms, {cm_result['rule_count']} rules")
        else:
            print(f"✗ {cm_result.get('error', 'Unknown error')}")
        results.append({**config, **cm_result})

        # Benchmark fim.arules
        print("Running fim.arules...", end=" ", flush=True)
        fim_result = benchmark_fim_arules(
            df,
            min_support=config["min_support"],
            min_confidence=config["min_confidence"],
            max_length=config["max_length"],
        )
        if fim_result["success"]:
            print(f"✓ {fim_result['duration_ms']:.1f}ms, {fim_result['rule_count']} rules")
        else:
            print(f"✗ {fim_result.get('error', 'Unknown error')}")
        results.append({**config, **fim_result})

        # Benchmark pyARC top_rules
        print("Running pyARC top_rules...", end=" ", flush=True)
        pyarc_result = benchmark_pyarc_top_rules(
            df, target_rule_count=config["target_rule_count"], max_length=config["max_length"]
        )
        if pyarc_result["success"]:
            print(f"✓ {pyarc_result['duration_ms']:.1f}ms, {pyarc_result['rule_count']} rules")
        else:
            print(f"✗ {pyarc_result.get('error', 'Unknown error')}")
        results.append({**config, **pyarc_result})

        # Display top 20 rules from each method
        print("\n" + "-" * 70)
        print("TOP 20 RULES COMPARISON")
        print("-" * 70)

        for result in [cm_result, fim_result, pyarc_result]:
            if result["success"] and result.get("rules"):
                print(f"\n{result['method']} - Top {min(20, len(result['rules']))} Rules:")
                print("-" * 70)
                for i, rule in enumerate(result["rules"][:20], 1):
                    print(f"{i:2d}. IF {rule['antecedent']}")
                    print(f"    THEN {rule['consequent']}")
                    print(f"    [conf={rule['confidence']:.1f}%, supp={rule['support']:.1f}%]")
            elif result["success"] and result["rule_count"] == 0:
                print(f"\n{result['method']}: No rules found")
            elif not result["success"]:
                print(f"\n{result['method']}: Failed - {result.get('error', 'Unknown')}")

        print()

    # Summary
    print("=" * 70)
    print("SUMMARY")
    print("=" * 70)

    df_results = pd.DataFrame(results)

    # Average times by method
    avg_times = df_results[df_results["success"]].groupby("method")["duration_ms"].mean()
    print("\nAverage execution time:")
    for method, time_ms in avg_times.items():
        print(f"  {method:20s}: {time_ms:8.1f} ms")

    # Speedup comparisons
    print("\nSpeedup comparisons:")
    if "CleverMiner" in avg_times.index and "fim.arules" in avg_times.index:
        speedup = avg_times["CleverMiner"] / avg_times["fim.arules"]
        print(f"  fim.arules is {speedup:.2f}x {'faster' if speedup > 1 else 'slower'} than CleverMiner")

    if "CleverMiner" in avg_times.index and "pyARC top_rules" in avg_times.index:
        speedup = avg_times["CleverMiner"] / avg_times["pyARC top_rules"]
        print(f"  pyARC top_rules is {speedup:.2f}x {'faster' if speedup > 1 else 'slower'} than CleverMiner")


if __name__ == "__main__":
    run_benchmark()
