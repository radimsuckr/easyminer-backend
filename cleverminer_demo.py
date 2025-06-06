import sys

import pandas as pd
from cleverminer.cleverminer import cleverminer
from sqlalchemy import select
from sqlalchemy.orm import joinedload

from easyminer.database import get_sync_db_session
from easyminer.models.preprocessing import Attribute, DatasetInstance
from easyminer.parsers.pmml.miner import CoefficientType, DBASettingType, SimplePmmlParser

DS_ID: int = 1


class MinerService:
    def __init__(self):
        self._df: pd.DataFrame

    def _load_data(self) -> None:
        with get_sync_db_session() as db:
            attributes = db.scalars(select(Attribute).where(Attribute.dataset_id == DS_ID)).all()
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
        self, quantifiers: dict[str, float], antecedents: dict[str, str | int], consequents: dict[str, str | int]
    ) -> cleverminer:
        self._load_data()
        return cleverminer(df=self._df, proc="4ftMiner", quantifiers=quantifiers, ante=antecedents, succ=consequents)


if __name__ == "__main__":
    with open("./cursor.xml") as f:
        parser = SimplePmmlParser(f.read())
    pmml = parser.parse()
    ts = pmml.association_model.task_setting
    print(f"PMML Version: {pmml.version}")
    print(f"PMML Header: {pmml.header}")
    # print(pmml)
    print("-" * 10)

    base_candidates = list(filter(lambda x: x.interest_measure.lower() == "base", ts.interest_measure_settings))
    if len(base_candidates) > 1:
        print("More than 1 Base candidates")
    confidence_candidates = list(filter(lambda x: x.interest_measure.lower() == "conf", ts.interest_measure_settings))
    if len(confidence_candidates) > 1:
        print("More than 1 conf candidates")
    aad_candidates = list(filter(lambda x: x.interest_measure.lower() == "aad", ts.interest_measure_settings))
    if len(aad_candidates) > 1:
        print("More than 1 conf candidates")

    quantifiers = {}
    if base_candidates:
        quantifiers["Base"] = base_candidates[0].threshold
    if confidence_candidates:
        quantifiers["conf"] = confidence_candidates[0].threshold
    if aad_candidates:
        quantifiers["aad"] = aad_candidates[0].threshold

    antecedent_setting_id = ts.antecedent_setting
    if not antecedent_setting_id:
        print("Antecedent setting not found")
        sys.exit(1)

    consequent_setting_id = ts.consequent_setting
    if not consequent_setting_id:
        print("Consequent setting not found")
        sys.exit(1)

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

    svc = MinerService()
    cm = svc.mine_4ft(quantifiers=quantifiers, antecedents=antecedents, consequents=consequents)
    cm.print_summary()
    cm.print_rulelist()
