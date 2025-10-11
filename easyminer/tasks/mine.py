import logging

import pandas as pd
from cleverminer.cleverminer import cleverminer
from sqlalchemy import select
from sqlalchemy.orm import joinedload

from easyminer.database import get_sync_db_session
from easyminer.models.preprocessing import Attribute, DatasetInstance
from easyminer.parsers.pmml.miner import PMML as PMMLInput
from easyminer.parsers.pmml.miner import CoefficientType, DBASettingType
from easyminer.serializers.pmml.miner import create_pmml_result_from_cleverminer
from easyminer.tasks.cba_utils import apply_cba_classification
from easyminer.worker import app

logger = logging.getLogger(__name__)


class MinerService:
    def __init__(self, dataset_id: int):
        self._ds_id: int = dataset_id
        self._df: pd.DataFrame

    def _load_data(self) -> None:
        with get_sync_db_session() as db:
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

    def mine_4ft(
        self,
        quantifiers: dict[str, float],
        antecedents: dict[str, str | int],
        consequents: dict[str, str | int],
    ) -> cleverminer:
        self._load_data()
        return cleverminer(
            df=self._df,
            proc="4ftMiner",
            quantifiers=quantifiers,
            ante=antecedents,
            succ=consequents,
        )


@app.task(pydantic=True)
def mine(pmml: PMMLInput) -> str:
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

    if cba_requested:
        logger.info("CBA requested via InterestMeasure")

    base_candidates = list(filter(lambda x: x.interest_measure.lower() == "base", ts.interest_measure_settings))
    if len(base_candidates) > 1:
        logger.warning("More than 1 Base candidates")
    confidence_candidates = list(filter(lambda x: x.interest_measure.lower() == "conf", ts.interest_measure_settings))
    if len(confidence_candidates) > 1:
        logger.warning("More than 1 conf candidates")
    aad_candidates = list(filter(lambda x: x.interest_measure.lower() == "aad", ts.interest_measure_settings))
    if len(aad_candidates) > 1:
        logger.warning("More than 1 conf candidates")

    quantifiers = {}
    if base_candidates:
        quantifiers["Base"] = base_candidates[0].threshold
    if confidence_candidates:
        quantifiers["conf"] = confidence_candidates[0].threshold
    if aad_candidates:
        quantifiers["aad"] = aad_candidates[0].threshold

    antecedent_setting_id = ts.antecedent_setting
    if not antecedent_setting_id:
        raise ValueError("Antecedent setting not found")

    consequent_setting_id = ts.consequent_setting
    if not consequent_setting_id:
        raise ValueError("Consequent setting not found")

    antecedent = next(filter(lambda x: x.id == antecedent_setting_id, ts.dba_settings))
    consequent = next(filter(lambda x: x.id == consequent_setting_id, ts.dba_settings))

    antecedents = {
        "attributes": [
            {
                "name": next(filter(lambda x: x.id == bba_ref, ts.bba_settings)).name,
                "type": "seq"
                if next(filter(lambda x: x.id == bba_ref, ts.bba_settings)).coefficient.type == CoefficientType.sequence
                else "subset",
                "minlen": next(filter(lambda x: x.id == bba_ref, ts.bba_settings)).coefficient.minimal_length,
                "maxlen": next(filter(lambda x: x.id == bba_ref, ts.bba_settings)).coefficient.maximal_length,
            }
            for bba_ref in antecedent.ba_refs
        ],
        "minlen": antecedent.minimal_length,
        "maxlen": antecedent.maximal_length,
        "type": "con" if antecedent.type == DBASettingType.conjunction else "dis",
    }
    consequents = {
        "attributes": [
            {
                "name": next(filter(lambda x: x.id == bba_ref, ts.bba_settings)).name,
                "type": "seq"
                if next(filter(lambda x: x.id == bba_ref, ts.bba_settings)).coefficient.type == CoefficientType.sequence
                else "subset",
                "minlen": next(filter(lambda x: x.id == bba_ref, ts.bba_settings)).coefficient.minimal_length,
                "maxlen": next(filter(lambda x: x.id == bba_ref, ts.bba_settings)).coefficient.maximal_length,
            }
            for bba_ref in consequent.ba_refs
        ],
        "minlen": consequent.minimal_length,
        "maxlen": consequent.maximal_length,
        "type": "con" if consequent.type == DBASettingType.conjunction else "dis",
    }

    svc = MinerService(dataset_id)
    cm = svc.mine_4ft(quantifiers=quantifiers, antecedents=antecedents, consequents=consequents)

    # Apply CBA if requested
    cba_extensions = []
    if cba_requested:
        logger.info("Applying CBA classification to mined rules")
        try:
            # Identify target attribute from consequent
            if not consequents.get("attributes") or len(consequents["attributes"]) == 0:
                raise ValueError("No consequent attributes defined for CBA classification")

            target_attr = consequents["attributes"][0]["name"]
            logger.info(f"Target attribute for CBA: {target_attr}")

            # Apply CBA classification
            # NOTE: Confidence and support filtering already done by cleverminer
            cba_result = apply_cba_classification(
                cleverminer_result=cm.result,
                dataset_id=dataset_id,
                target_attribute=target_attr,
            )

            # Prepare extensions with CBA results
            cba_extensions = [
                {"name": "cba_applied", "value": "true"},
                {"name": "cba_accuracy", "value": f"{cba_result.accuracy:.4f}"},
                {"name": "cba_pruned_rules_count", "value": str(cba_result.pruned_rules_count)},
                {"name": "cba_filtered_rules_count", "value": str(cba_result.filtered_rules_count)},
                {"name": "cba_original_rules_count", "value": str(cba_result.original_rules_count)},
                {"name": "cba_default_class", "value": cba_result.default_class},
                {"name": "cba_target_attribute", "value": cba_result.target_attribute},
            ]

            logger.info(
                f"CBA classification complete: accuracy={cba_result.accuracy:.2%}, "
                f"rules={cba_result.original_rules_count}→{cba_result.filtered_rules_count}→{cba_result.pruned_rules_count}"
            )

            # Filter cleverminer result to only include pruned rules (Scala compatibility)
            if cba_result.pruned_rule_ids:
                pruned_rule_ids_set = set(cba_result.pruned_rule_ids)
                original_rule_count = len(cm.result["rules"])
                cm.result["rules"] = [rule for rule in cm.result["rules"] if rule.get("rule_id") in pruned_rule_ids_set]
                logger.info(
                    f"Filtered output to {len(cm.result['rules'])} pruned rules "
                    f"(removed {original_rule_count - len(cm.result['rules'])} rules for CBA compatibility)"
                )

        except Exception as e:
            logger.error(f"Error applying CBA classification: {e}", exc_info=True)
            cba_extensions = [
                {"name": "cba_applied", "value": "false"},
                {"name": "cba_error", "value": str(e)},
            ]

    # Create PMML result with CBA extensions if applicable
    result = create_pmml_result_from_cleverminer(cm.result, headers_data=cba_extensions if cba_extensions else None)
    xml = result.to_xml()
    return xml if isinstance(xml, str) else xml.decode("utf-8")
