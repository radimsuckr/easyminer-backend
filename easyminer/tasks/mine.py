import logging

import fim
import pandas as pd
from pyarc import TransactionDB
from pyarc.algorithms import createCARs, top_rules
from sqlalchemy import select
from sqlalchemy.orm import joinedload

from easyminer.database import get_sync_db_session
from easyminer.models.preprocessing import Attribute, DatasetInstance
from easyminer.parsers.pmml.miner import PMML as PMMLInput
from easyminer.serializers.pmml.miner import create_pmml_result_from_pyarc
from easyminer.validators.miner import validate_mining_task
from easyminer.worker import app

logger = logging.getLogger(__name__)


class MinerService:
    def __init__(self, dataset_id: int, db_url: str | None = None):
        self._ds_id: int = dataset_id
        self._db_url: str | None = db_url
        self._df: pd.DataFrame

    def _load_data(self) -> None:
        with get_sync_db_session(self._db_url) as db:
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

    def _prepare_transaction_db(self, target_col: str) -> tuple[TransactionDB, list[str]]:
        """
        Prepare pyARC TransactionDB from loaded DataFrame.

        Args:
            target_col: Name of the target/consequent column

        Returns:
            Tuple of (TransactionDB, list of transactions as strings)
        """
        if not hasattr(self, "_df") or self._df is None:
            self._load_data()

        txn_db = TransactionDB.from_DataFrame(self._df, target=target_col)
        transactions = txn_db.string_representation

        logger.info(f"Prepared TransactionDB: {len(transactions)} transactions, target='{target_col}'")
        return txn_db, transactions

    def _build_appearance_constraints(self, antecedent_attrs: list[str], consequent_attrs: list[str]) -> dict[str, str]:
        """
        Build appearance constraints for fim.arules / pyARC mining.

        Maps items to their roles:
        - 'i' = input (antecedent only)
        - 'o' = output (consequent only)
        - 'b' = both (can appear in either)
        - 'n' = neither (exclude)

        Args:
            antecedent_attrs: List of attribute names for antecedent
            consequent_attrs: List of attribute names for consequent

        Returns:
            Dictionary mapping item strings to appearance codes
        """
        if not hasattr(self, "_df") or self._df is None:
            self._load_data()

        appearance = {}

        # Mark consequent items as output (consequent only)
        for attr in consequent_attrs:
            if attr in self._df.columns:
                for val in self._df[attr].unique():
                    appearance[f"{attr}:=:{val}"] = "o"

        # Mark antecedent items as input (antecedent only)
        for attr in antecedent_attrs:
            if attr in self._df.columns:
                for val in self._df[attr].unique():
                    appearance[f"{attr}:=:{val}"] = "i"

        logger.info(
            f"Built appearance constraints: {len([v for v in appearance.values() if v == 'o'])} output items, "
            f"{len([v for v in appearance.values() if v == 'i'])} input items"
        )

        return appearance

    def mine_mode1_standard(
        self,
        target_col: str,
        antecedent_attrs: list[str],
        consequent_attrs: list[str],
        min_support: float,
        min_confidence: float,
        max_rule_length: int | None = None,
    ) -> list:
        """
        Mode 1: Standard mining with fixed user-provided thresholds.

        Uses fim.arules() - equivalent to R's arules::apriori()

        Args:
            target_col: Name of the target column
            antecedent_attrs: Attributes for antecedent
            consequent_attrs: Attributes for consequent
            min_support: Minimum support threshold (as decimal, e.g., 0.1 for 10%)
            min_confidence: Minimum confidence threshold (as decimal, e.g., 0.6 for 60%)
            max_rule_length: Maximum rule length (optional)

        Returns:
            List of pyARC CAR objects
        """
        logger.info("Mode 1: Standard mining with fixed thresholds")
        logger.info(f"  Support: {min_support:.2%}, Confidence: {min_confidence:.2%}")
        if max_rule_length:
            logger.info(f"  Max rule length: {max_rule_length}")

        # Prepare transaction database
        _, transactions = self._prepare_transaction_db(target_col)

        # Build appearance constraints
        appearance = self._build_appearance_constraints(antecedent_attrs, consequent_attrs)

        # Mine with fim.arules
        logger.info("Mining with fim.arules()...")
        raw_rules = fim.arules(
            transactions,
            supp=min_support * 100,  # fim expects percentage
            conf=min_confidence * 100,
            mode="o",  # Original apriori
            report="sc",  # Report support & confidence
            appear=appearance if appearance else None,
            zmax=max_rule_length if max_rule_length else 100,
        )

        logger.info(f"  ? Mined {len(raw_rules)} raw rules")

        # Convert to CARs for consistent processing
        cars = createCARs(raw_rules)
        logger.info(f"  ? Created {len(cars)} CARs")

        # Apply max rule length filter (if needed)
        if max_rule_length:
            cars = [car for car in cars if len(car.antecedent) + 1 <= max_rule_length]  # +1 for consequent
            logger.info(f"  ? After length filter (?{max_rule_length}): {len(cars)} rules")

        return cars

    def mine_mode2_auto(
        self,
        target_col: str,
        antecedent_attrs: list[str],
        consequent_attrs: list[str],
        target_rule_count: int = 1000,
        max_rule_length: int | None = None,
    ) -> list:
        """
        Mode 2: AUTO_CONF_SUPP mining with automatic threshold detection.

        Uses pyARC top_rules() - equivalent to R's rCBA::build()

        Args:
            target_col: Name of the target column
            antecedent_attrs: Attributes for antecedent
            consequent_attrs: Attributes for consequent
            target_rule_count: Target number of rules to find
            max_rule_length: Maximum rule length (optional)

        Returns:
            List of pyARC CAR objects
        """
        logger.info("Mode 2: AUTO_CONF_SUPP mining with automatic thresholds")
        logger.info(f"  Target rule count: {target_rule_count}")
        if max_rule_length:
            logger.info(f"  Max rule length: {max_rule_length}")

        # Prepare transaction database
        _, transactions = self._prepare_transaction_db(target_col)

        # Build appearance constraints
        appearance = self._build_appearance_constraints(antecedent_attrs, consequent_attrs)

        # Mine with pyARC top_rules (automatic threshold detection)
        logger.info("Mining with pyARC top_rules() (automatic threshold detection)...")
        raw_rules = top_rules(
            transactions,
            appearance=appearance,
            target_rule_count=target_rule_count,
            init_support=1.0,
            init_conf=50.0,
            conf_step=5.0,
            supp_step=5.0,
            minlen=1,
            init_maxlen=min(3, max_rule_length) if max_rule_length else 3,
            total_timeout=30.0,
            max_iterations=20,
        )

        logger.info(f"  ? Mined {len(raw_rules)} raw rules")

        # Convert to CARs
        cars = createCARs(raw_rules)
        logger.info(f"  ? Created {len(cars)} CARs")

        # Apply max rule length filter
        if max_rule_length:
            cars = [car for car in cars if len(car.antecedent) + 1 <= max_rule_length]  # +1 for consequent
            logger.info(f"  ? After length filter (?{max_rule_length}): {len(cars)} rules")

        # Sort by confidence desc, support desc and limit to target count
        cars = sorted(cars, key=lambda r: (-r.confidence, -r.support))[:target_rule_count]
        logger.info(f"  ? Final output: {len(cars)} rules")

        return cars


@app.task(pydantic=True)
def mine(pmml: PMMLInput, db_url: str | None = None) -> str:
    # Validate the mining task first (Scala compatibility)
    validate_mining_task(pmml)

    ts = pmml.association_model.task_setting
    logger.info(f"PMML Version: {pmml.version}")
    logger.info(f"PMML Header: {pmml.header}")

    dataset_ext = next(filter(lambda x: x.name.lower() == "dataset", pmml.header.extensions), None)
    if not dataset_ext:
        raise ValueError("Dataset extension not found in PMML header")
    try:
        dataset_id = int(dataset_ext.value)
    except ValueError:
        raise ValueError(f"Invalid dataset ID in Dataset extension: {dataset_ext.value}")

    # Check if CBA (Classification Based on Associations) is requested
    # CBA is enabled via InterestMeasure "CBA" in TaskSetting (Scala-compatible)
    cba_requested = any(im.interest_measure.lower() == "cba" for im in ts.interest_measure_settings)

    # Check if AUTO_CONF_SUPP is requested (automatic confidence/support detection)
    auto_conf_supp = any(im.interest_measure.lower() == "auto_conf_supp" for im in ts.interest_measure_settings)

    if cba_requested:
        logger.info("CBA requested via InterestMeasure")

    if auto_conf_supp:
        logger.info("AUTO_CONF_SUPP mode detected - will use pyARC top_rules for automatic threshold detection")

    # Extract interest measures
    base_candidates = list(filter(lambda x: x.interest_measure.lower() == "base", ts.interest_measure_settings))
    if len(base_candidates) > 1:
        logger.warning("More than 1 Base candidates")

    # Support both CONF and FUI (Scala uses FUI for confidence)
    confidence_candidates = list(
        filter(
            lambda x: x.interest_measure.lower() in ["conf", "fui"],
            ts.interest_measure_settings,
        )
    )
    if len(confidence_candidates) > 1:
        logger.warning("More than 1 confidence candidates")

    # Support SUPP for support
    support_candidates = list(filter(lambda x: x.interest_measure.lower() == "supp", ts.interest_measure_settings))
    if len(support_candidates) > 1:
        logger.warning("More than 1 support candidates")

    aad_candidates = list(filter(lambda x: x.interest_measure.lower() == "aad", ts.interest_measure_settings))
    if len(aad_candidates) > 1:
        logger.warning("More than 1 aad candidates")

    # Extract RULE_LENGTH (max rule length)
    rule_length_candidates = list(
        filter(lambda x: x.interest_measure.lower() == "rule_length", ts.interest_measure_settings)
    )
    max_rule_length = None
    if rule_length_candidates:
        max_rule_length = int(rule_length_candidates[0].threshold) if rule_length_candidates[0].threshold else None
        logger.info(f"Max rule length constraint: {max_rule_length}")

    # Build quantifiers dictionary
    quantifiers = {}
    if base_candidates and base_candidates[0].threshold is not None:
        quantifiers["Base"] = base_candidates[0].threshold
    if confidence_candidates and confidence_candidates[0].threshold is not None:
        quantifiers["conf"] = confidence_candidates[0].threshold
    if support_candidates and support_candidates[0].threshold is not None:
        # CleverMiner uses "Base" for support in 4ft-Miner
        # Only add if not already set by base_candidates
        if "Base" not in quantifiers:
            quantifiers["Base"] = support_candidates[0].threshold
    if aad_candidates and aad_candidates[0].threshold is not None:
        quantifiers["aad"] = aad_candidates[0].threshold

    # For AUTO_CONF_SUPP mode, use default values if not specified
    if auto_conf_supp:
        if "conf" not in quantifiers:
            quantifiers["conf"] = 0.5  # Default confidence
            logger.info("AUTO_CONF_SUPP: Using default confidence 0.5")
        if "Base" not in quantifiers:
            quantifiers["Base"] = 0.01  # Default support
            logger.info("AUTO_CONF_SUPP: Using default support 0.01")

    antecedent_setting_id = ts.antecedent_setting
    if not antecedent_setting_id:
        raise ValueError("Antecedent setting not found")

    consequent_setting_id = ts.consequent_setting
    if not consequent_setting_id:
        raise ValueError("Consequent setting not found")

    antecedent = next(filter(lambda x: x.id == antecedent_setting_id, ts.dba_settings))
    consequent = next(filter(lambda x: x.id == consequent_setting_id, ts.dba_settings))

    svc = MinerService(dataset_id, db_url)

    # Extract attribute names from antecedent and consequent settings
    antecedent_attrs = [next(filter(lambda x: x.id == bba_ref, ts.bba_settings)).name for bba_ref in antecedent.ba_refs]
    consequent_attrs = [next(filter(lambda x: x.id == bba_ref, ts.bba_settings)).name for bba_ref in consequent.ba_refs]

    # Identify target column (should be the consequent attribute)
    if not consequent_attrs:
        raise ValueError("No consequent attributes found")
    target_col = consequent_attrs[0]
    logger.info(f"Target column: {target_col}")

    # Route to appropriate mining mode
    if auto_conf_supp:
        # Mode 2: AUTO_CONF_SUPP - automatic threshold detection
        logger.info("=== MODE 2: AUTO_CONF_SUPP Mining ===")

        # Extract target rule count from PMML (if specified)
        hypotheses_max = ts.lispm_miner_hypotheses_max
        target_rule_count = hypotheses_max if hypotheses_max else 1000
        logger.info(f"Target rule count: {target_rule_count}")

        # Mine with automatic thresholds
        mined_rules = svc.mine_mode2_auto(
            target_col=target_col,
            antecedent_attrs=antecedent_attrs,
            consequent_attrs=consequent_attrs,
            target_rule_count=target_rule_count,
            max_rule_length=max_rule_length,
        )
    else:
        # Mode 1: Standard mining with user-provided thresholds
        logger.info("=== MODE 1: Standard Mining with Fixed Thresholds ===")

        # Extract thresholds from PMML
        min_confidence = quantifiers.get("conf", 0.5)
        min_support = quantifiers.get("Base", 0.01)

        # Convert to decimal if needed (PMML may provide as percentage or decimal)
        if min_confidence > 1.0:
            min_confidence = min_confidence / 100.0
        if min_support > 1.0:
            min_support = min_support / 100.0

        logger.info(f"User thresholds: support={min_support:.2%}, confidence={min_confidence:.2%}")

        # Mine with fixed thresholds
        mined_rules = svc.mine_mode1_standard(
            target_col=target_col,
            antecedent_attrs=antecedent_attrs,
            consequent_attrs=consequent_attrs,
            min_support=min_support,
            min_confidence=min_confidence,
            max_rule_length=max_rule_length,
        )

    logger.info(f"Mining complete: {len(mined_rules)} rules found")

    # Apply CBA if requested
    cba_extensions = []
    if cba_requested:
        logger.info("Applying CBA M1/M2 pruning to mined rules")
        try:
            from pyarc.algorithms import M1Algorithm, M2Algorithm

            # Get TransactionDB for CBA
            txn_db, _ = svc._prepare_transaction_db(target_col)

            # Apply M1 pruning
            logger.info("  ? Applying M1 algorithm...")
            m1_clf = M1Algorithm(mined_rules, txn_db).build()
            logger.info(f"  ? After M1: {len(m1_clf.rules)} rules")

            # Apply M2 pruning
            logger.info("  ? Applying M2 algorithm...")
            m2_clf = M2Algorithm(m1_clf.rules, txn_db).build()
            logger.info(f"  ? After M2: {len(m2_clf.rules)} rules")

            # Test accuracy
            accuracy = m2_clf.test_transactions(txn_db)

            # Update mined_rules to only include pruned rules
            pruned_rules = m2_clf.rules
            original_rule_count = len(mined_rules)
            mined_rules = pruned_rules

            # Prepare extensions with CBA results
            cba_extensions = [
                {"name": "cba_applied", "value": "true"},
                {"name": "cba_accuracy", "value": f"{accuracy:.4f}"},
                {"name": "cba_original_rules_count", "value": str(original_rule_count)},
                {"name": "cba_m1_rules_count", "value": str(len(m1_clf.rules))},
                {"name": "cba_m2_rules_count", "value": str(len(m2_clf.rules))},
                {"name": "cba_target_attribute", "value": target_col},
                {"name": "mining_mode", "value": "mode2_auto" if auto_conf_supp else "mode1_standard"},
            ]

            logger.info(
                f"CBA pruning complete: accuracy={accuracy:.2%}, "
                f"rules={original_rule_count}?{len(m1_clf.rules)}?{len(m2_clf.rules)}"
            )

        except Exception as e:
            logger.error(f"Error applying CBA pruning: {e}", exc_info=True)
            cba_extensions = [
                {"name": "cba_applied", "value": "false"},
                {"name": "cba_error", "value": str(e)},
            ]

    # Prepare headers
    headers_data = cba_extensions if cba_extensions else []
    if not cba_extensions:
        # Add default mining mode header
        headers_data = [
            {"name": "mining_mode", "value": "mode2_auto" if auto_conf_supp else "mode1_standard"},
            {"name": "algorithm", "value": "pyarc-fim-4ft"},
        ]

    # Count total attributes for PMML
    total_attributes = sum(len(svc._df[col].unique()) for col in svc._df.columns)

    # Create PMML result from pyARC rules
    result = create_pmml_result_from_pyarc(
        rules=mined_rules,
        transactions_df=svc._df,
        total_transactions=len(svc._df),
        total_attributes=total_attributes,
        headers_data=headers_data,
    )

    xml = result.to_xml()
    return xml if isinstance(xml, str) else xml.decode("utf-8")
