import logging
from typing import Any

import fim
import pandas as pd
from pyarc import TransactionDB
from pyarc.algorithms import createCARs, top_rules
from sqlalchemy import select
from sqlalchemy.orm import joinedload

from easyminer.database import get_sync_db_session
from easyminer.models.preprocessing import Attribute, DatasetInstance
from easyminer.parsers.pmml.miner import PMML as PMMLInput
from easyminer.parsers.pmml.miner import (
    BBASetting,
    DBASetting,
    Extension,
    InterestMeasureThreshold,
    TaskSetting,
)
from easyminer.schemas.center import DatabaseConfig
from easyminer.serializers.pmml.miner import create_pmml_result_from_pyarc
from easyminer.validators.miner import validate_mining_task
from easyminer.worker import app

logger = logging.getLogger(__name__)


def _build_db_url_from_pmml_extensions(extensions: list[Extension]) -> str:
    """Assumes all required extensions are present and validated by MinerTaskValidator:
    - database-server: e.g., "mysql://localhost:3306" or "localhost:3306"
    - database-name: database name
    - database-user: username
    - database-password: password
    """
    ext_dict = {ext.name.lower(): ext.value for ext in extensions}

    db_server = ext_dict["database-server"]
    db_name = ext_dict["database-name"]
    db_user = ext_dict["database-user"]
    db_password = ext_dict["database-password"]

    # Parse server - handle formats like "mysql://localhost:3306" or "localhost:3306"
    server_str = db_server.split("://")[1] if "://" in db_server else db_server

    # Split host and port (validated by validator)
    server, port_str = server_str.rsplit(":", 1)
    port = int(port_str)

    db_config = DatabaseConfig(
        server=server,
        port=port,
        username=db_user,
        password=db_password,
        database=db_name,
    )

    return db_config.get_sync_url()


def _mask_password(db_url: str) -> str:
    """Mask password in db_url for safe logging.

    Handles URLs in format: protocol://username:password@host:port/database
    Note: Password may contain special characters including '@' and ':'
    """
    if "://" not in db_url:
        return db_url

    try:
        protocol, rest = db_url.split("://", 1)
        if "@" not in rest:
            return db_url

        # Split on the LAST '@' since password might contain '@'
        auth, host_db = rest.rsplit("@", 1)
        if ":" not in auth:
            return db_url

        user, _ = auth.split(":", 1)
        return f"{protocol}://{user}:***@{host_db}"
    except Exception:
        return "***"


def resolve_dba_to_attributes(dba: DBASetting, task_setting: TaskSetting) -> list[str]:
    """Recursively resolve a DBA (Derived Boolean Attribute) to leaf-level attribute names.

    DBAs form a hierarchical structure where:
    - Level 1 & 2 DBAs (Conjunction/Disjunction) reference other DBAs via BASettingRef
    - Level 3 DBAs (Literal) reference BBAs (Basic Boolean Attributes) via BASettingRef

    This function recursively traverses the DBA hierarchy to collect all leaf-level
    attribute names from BBAs.

    Args:
        dba: The DBA setting to resolve
        task_setting: Task setting containing all BBA and DBA definitions

    Returns:
        List of unique attribute names (from BBAs)

    Example:
        DBA(id=10, type=Conjunction) -> [DBA(id=20), DBA(id=21)]
          DBA(id=20, type=Conjunction) -> [DBA(id=30), DBA(id=31)]
            DBA(id=30, type=Literal) -> BBA(id=1, name="age")
            DBA(id=31, type=Literal) -> BBA(id=2, name="salary")
          DBA(id=21, type=Literal) -> BBA(id=3, name="status")

        Result: ["age", "salary", "status"]
    """
    attribute_names: set[str] = set()

    # Build lookup dictionaries for efficient access
    bba_by_id: dict[str, BBASetting] = {bba.id: bba for bba in task_setting.bba_settings}
    dba_by_id: dict[str, DBASetting] = {dba.id: dba for dba in task_setting.dba_settings}

    def _resolve_ba_ref(ba_ref: str) -> None:
        """Recursively resolve a BA reference (can be BBA or DBA)"""
        # First check if it's a DBA (Derived Boolean Attribute)
        if ba_ref in dba_by_id:
            referenced_dba = dba_by_id[ba_ref]
            # Recurse into the DBA's references
            for nested_ba_ref in referenced_dba.ba_refs:
                _resolve_ba_ref(nested_ba_ref)
        # Otherwise it must be a BBA (Basic Boolean Attribute)
        elif ba_ref in bba_by_id:
            referenced_bba = bba_by_id[ba_ref]
            attribute_names.add(referenced_bba.name)
        else:
            logger.warning(f"BASettingRef '{ba_ref}' not found in BBA or DBA settings")

    # Start resolution from the input DBA
    for ba_ref in dba.ba_refs:
        _resolve_ba_ref(ba_ref)

    return sorted(attribute_names)  # Sort for deterministic output


class MinerService:
    def __init__(self, dataset_id: int, db_url: str, required_attributes: list[str] | None = None):
        self._ds_id: int = dataset_id
        self._db_url: str = db_url
        self._required_attributes: list[str] | None = required_attributes
        self._df: pd.DataFrame | None = None

    def _load_data(self) -> None:
        """
        Load data from database into DataFrame.

        Only loads attributes specified in required_attributes (if set), otherwise loads all.
        This significantly improves performance and memory usage for large datasets.
        """
        with get_sync_db_session(self._db_url) as db:
            # Build query with optional attribute name filter
            query = select(Attribute).where(Attribute.dataset_id == self._ds_id)
            if self._required_attributes:
                query = query.where(Attribute.name.in_(self._required_attributes))
                logger.info(
                    f"Loading {len(self._required_attributes)} required attributes: {self._required_attributes}"
                )
            else:
                logger.info("Loading all attributes from dataset")

            attributes = db.scalars(query).all()
            self._df = pd.DataFrame(columns=tuple(f.name for f in attributes))

            for attribute in attributes:
                instances = db.scalars(
                    select(DatasetInstance)
                    .where(DatasetInstance.attribute_id == attribute.id)
                    .order_by(DatasetInstance.tx_id)
                    .options(joinedload(DatasetInstance.value))
                ).all()
                self._df[attribute.name] = [i.value.value for i in instances]

            logger.info(f"Loaded {len(self._df)} rows with {len(self._df.columns)} columns")

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
            + f"{len([v for v in appearance.values() if v == 'i'])} input items"
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
        min_lift: float | None = None,
    ) -> list[Any]:
        """Mode 1: Standard mining with fixed user-provided thresholds.

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

        _, transactions = self._prepare_transaction_db(target_col)

        appearance = self._build_appearance_constraints(antecedent_attrs, consequent_attrs)

        logger.info("Mining with fim.arules()...")

        arules_params = {
            "supp": min_support * 100,  # fim expects percentage
            "conf": min_confidence * 100,
            "mode": "o",  # Original apriori
            "report": "sc",  # Report support & confidence
            "appear": appearance if appearance else None,
            "zmax": max_rule_length if max_rule_length else 100,
        }

        if min_lift is not None:
            arules_params["eval"] = "l"  # Use lift as evaluation measure
            arules_params["thresh"] = min_lift * 100  # fim expects percentage
            logger.info(f"  ✓ Using LIFT filter: eval='l', thresh={min_lift:.2f}")

        raw_rules = fim.arules(transactions, **arules_params)

        logger.info(f"  ? Mined {len(raw_rules)} raw rules")

        cars = createCARs(raw_rules)
        logger.info(f"  ? Created {len(cars)} CARs")

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
    ) -> list[Any]:
        """Mode 2: AUTO_CONF_SUPP mining with automatic threshold detection.

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

    def get_dataframe(self) -> pd.DataFrame:
        if self._df is None:
            raise ValueError("Data not loaded yet. Call _load_data() first.")
        return self._df


@app.task(pydantic=True)
def mine(pmml: PMMLInput) -> str:
    """Mine association rules from a dataset using PMML task specification.

    This Celery task processes a PMML document that specifies mining parameters,
    including interest measures (confidence, support, etc.), cedent settings (antecedent
    and consequent), and extracts database connection info from PMML header extensions.

    The task supports several key features:
    - Interest measures: CONF, SUPP, LIFT, RULE_LENGTH, AAD, etc.
    - Threshold types: "% of all" (percentage) and "Abs" (absolute count)
    - Compare types: "Greater than or equal", "Less than or equal", "Equal"
    - Automatic confidence/support detection (AUTO_CONF_SUPP)
    - Classification Based on Associations (CBA) pruning
    - Hierarchical DBA (Derived Boolean Attribute) resolution
    - Optimized data loading (only loads required attributes)

    Interest Measures (R Backend Spec):
        All measures are enforced by fim.arules native parameters (no post-filtering):

        CONF (Confidence):
        - CompareType: ">=" (always)
        - ThresholdType: "% of all"
        - Implementation: fim.arules(conf=X)

        SUPP (Support):
        - CompareType: ">=" (always)
        - ThresholdType: "% of all"
        - Implementation: fim.arules(supp=X)

        LIFT (Lift Ratio):
        - CompareType: ">=" (always)
        - ThresholdType: "% of all"
        - Implementation: fim.arules(eval='l', thresh=X)
        - Optional measure

        RULE_LENGTH (Max Attributes):
        - CompareType: "<=" (always)
        - ThresholdType: "Abs"
        - Implementation: fim.arules(zmax=X)
        - Optional measure

        AUTO_CONF_SUPP (Auto Thresholds):
        - Special mode using pyARC.top_rules()
        - Automatically determines optimal thresholds
        - Replaces manual CONF+SUPP

    Database Connection:
        Connection info is extracted from PMML header extensions:
        - database-server, database-name, database-user, database-password
        These are validated by MinerTaskValidator before use.
    """
    _ = validate_mining_task(pmml)

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

    db_url = _build_db_url_from_pmml_extensions(pmml.header.extensions)
    logger.info(f"Built db_url from PMML extensions: {_mask_password(db_url)}")

    # Check if CBA (Classification Based on Associations) is requested
    cba_requested = any(im.interest_measure.lower() == "cba" for im in ts.interest_measure_settings)

    # Check if AUTO_CONF_SUPP is requested (automatic confidence/support detection)
    auto_conf_supp = any(im.interest_measure.lower() == "auto_conf_supp" for im in ts.interest_measure_settings)

    # Log all interest measures with their compare_type for transparency
    logger.info("Interest measures configuration:")
    for im in ts.interest_measure_settings:
        if im.interest_measure.lower() == "rule_length":
            im.threshold_type = "Abs"  # RULE_LENGTH should use absolute counts

        compare_type = im.compare_type or "Greater than or equal"  # Default
        threshold_type = im.threshold_type or "% of all"  # Default
        logger.info(
            f"  - {im.interest_measure}: threshold={im.threshold}, type={threshold_type}, compare={compare_type}"
        )

    if cba_requested:
        logger.info("CBA requested via InterestMeasure")

    if auto_conf_supp:
        logger.info("AUTO_CONF_SUPP mode detected - will use pyARC top_rules for automatic threshold detection")

    confidence_candidates = list(filter(lambda x: x.interest_measure.lower() == "conf", ts.interest_measure_settings))
    support_candidates = list(filter(lambda x: x.interest_measure.lower() == "supp", ts.interest_measure_settings))
    lift_candidates = list(filter(lambda x: x.interest_measure.lower() == "lift", ts.interest_measure_settings))
    rule_length_candidates = list(
        filter(lambda x: x.interest_measure.lower() == "rule_length", ts.interest_measure_settings)
    )

    max_rule_length = None
    if rule_length_candidates and rule_length_candidates[0].threshold is not None:
        max_rule_length = int(rule_length_candidates[0].threshold)
        logger.info(f"Max rule length constraint: {max_rule_length}")

    quantifiers = {}

    def normalize_threshold(im: InterestMeasureThreshold, default_type: str = "% of all") -> float:
        """Normalize threshold value to decimal (0.0-1.0) based on threshold_type"""
        if im.threshold is None:
            return 0.0

        threshold_type = im.threshold_type or default_type
        value = im.threshold

        if threshold_type == "% of all":
            # Already in decimal form (0.0-1.0)
            if value > 1.0:
                logger.warning(f"Threshold {value} marked as '% of all' but > 1.0, dividing by 100")
                return value / 100.0
            return value
        elif threshold_type == "Abs":
            # Absolute count - not applicable for confidence/support (would need total count)
            logger.warning(f"Absolute threshold_type for {im.interest_measure} not supported in percentage context")
            return value if value <= 1.0 else value / 100.0
        else:
            logger.warning(f"Unknown threshold_type '{threshold_type}', treating as '% of all'")
            return value if value <= 1.0 else value / 100.0

    if confidence_candidates and confidence_candidates[0].threshold is not None:
        quantifiers["conf"] = normalize_threshold(confidence_candidates[0])
        logger.debug(
            f"Confidence threshold: {quantifiers['conf']:.4f} (type: {confidence_candidates[0].threshold_type or '% of all'})"
        )

    if support_candidates and support_candidates[0].threshold is not None:
        if "Base" not in quantifiers:
            quantifiers["Base"] = normalize_threshold(support_candidates[0])
            logger.debug(
                f"Support threshold: {quantifiers['Base']:.4f} (type: {support_candidates[0].threshold_type or '% of all'})"
            )

    # Extract LIFT threshold (will be passed to fim.arules as eval='l' + thresh)
    min_lift = None
    if lift_candidates and lift_candidates[0].threshold is not None:
        min_lift = normalize_threshold(lift_candidates[0])
        logger.debug(f"LIFT threshold: {min_lift:.4f} (type: {lift_candidates[0].threshold_type or '% of all'})")

    # For AUTO_CONF_SUPP mode, use default values if not specified
    if auto_conf_supp:
        if "conf" not in quantifiers:
            quantifiers["conf"] = 0.5
            logger.info("AUTO_CONF_SUPP: Using default confidence 0.5")
        if "Base" not in quantifiers:
            quantifiers["Base"] = 0.01
            logger.info("AUTO_CONF_SUPP: Using default support 0.01")

    antecedent_setting_id = ts.antecedent_setting
    if not antecedent_setting_id:
        raise ValueError("Antecedent setting not found")

    consequent_setting_id = ts.consequent_setting
    if not consequent_setting_id:
        raise ValueError("Consequent setting not found")

    antecedent = next(filter(lambda x: x.id == antecedent_setting_id, ts.dba_settings))
    consequent = next(filter(lambda x: x.id == consequent_setting_id, ts.dba_settings))

    # Extract attribute names from antecedent and consequent settings
    # Use resolver to handle hierarchical DBA structures (DBA can reference other DBAs)
    antecedent_attrs = resolve_dba_to_attributes(antecedent, ts)
    consequent_attrs = resolve_dba_to_attributes(consequent, ts)

    logger.info(f"Resolved antecedent attributes: {antecedent_attrs}")
    logger.info(f"Resolved consequent attributes: {consequent_attrs}")

    # Combine all required attributes for data loading
    required_attrs = list(set(antecedent_attrs + consequent_attrs))
    logger.info(f"Loading {len(required_attrs)} unique attributes from dataset")

    svc = MinerService(dataset_id, db_url, required_attributes=required_attrs)

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

        # Extract thresholds from PMML (already normalized by normalize_threshold)
        min_confidence = quantifiers.get("conf", 0.5)
        min_support = quantifiers.get("Base", 0.01)

        logger.info(f"User thresholds: support={min_support:.2%}, confidence={min_confidence:.2%}")

        # Mine with fixed thresholds
        mined_rules = svc.mine_mode1_standard(
            target_col=target_col,
            antecedent_attrs=antecedent_attrs,
            consequent_attrs=consequent_attrs,
            min_support=min_support,
            min_confidence=min_confidence,
            max_rule_length=max_rule_length,
            min_lift=min_lift,
        )

    logger.info(f"Mining complete: {len(mined_rules)} rules found")

    cba_extensions = []
    if cba_requested:
        logger.info("Applying CBA M1/M2 pruning to mined rules")
        try:
            from pyarc.algorithms import M1Algorithm, M2Algorithm

            txn_db, _ = svc._prepare_transaction_db(target_col)

            logger.info("  ? Applying M1 algorithm...")
            m1_clf = M1Algorithm(mined_rules, txn_db).build()
            logger.info(f"  ? After M1: {len(m1_clf.rules)} rules")

            logger.info("  ? Applying M2 algorithm...")
            m2_clf = M2Algorithm(m1_clf.rules, txn_db).build()
            logger.info(f"  ? After M2: {len(m2_clf.rules)} rules")

            accuracy = m2_clf.test_transactions(txn_db)

            pruned_rules = m2_clf.rules
            original_rule_count = len(mined_rules)
            mined_rules = pruned_rules

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
                + f"rules={original_rule_count}?{len(m1_clf.rules)}?{len(m2_clf.rules)}"
            )

        except Exception as e:
            logger.error(f"Error applying CBA pruning: {e}", exc_info=True)
            cba_extensions = [
                {"name": "cba_applied", "value": "false"},
                {"name": "cba_error", "value": str(e)},
            ]

    headers_data = cba_extensions if cba_extensions else []
    if not cba_extensions:
        headers_data = [
            {"name": "mining_mode", "value": "mode2_auto" if auto_conf_supp else "mode1_standard"},
            {"name": "algorithm", "value": "pyarc-fim-4ft"},
        ]

    # Count total attributes for PMML
    svc_df = svc.get_dataframe()
    total_attributes = sum(len(svc_df[col].unique()) for col in svc_df.columns)
    result = create_pmml_result_from_pyarc(
        rules=mined_rules,
        transactions_df=svc_df,
        total_transactions=len(svc_df),
        total_attributes=total_attributes,
        headers_data=headers_data,
    )

    xml = result.to_xml()
    return xml if isinstance(xml, str) else xml.decode("utf-8")
