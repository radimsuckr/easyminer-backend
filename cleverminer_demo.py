import sys
from decimal import Decimal

import pandas as pd
from cleverminer.cleverminer import cleverminer
from sqlalchemy import select

from easyminer.database import get_sync_db_session
from easyminer.models.data import DataSourceInstance, Field
from miner_parser import SimplePmmlParser

DS_ID: int = 2


class MinerService:
    def __init__(self):
        self._df: pd.DataFrame

    def _load_data(self) -> None:
        def _f(x: Decimal | None) -> str:
            if x is None or x < 9000:
                return "low"
            elif x < 10000:
                return "medium"
            else:
                return "high"

        with get_sync_db_session() as db:
            fields = db.scalars(select(Field).where(Field.data_source_id == DS_ID)).all()
            self._df = pd.DataFrame(columns=tuple(f.name for f in fields))
            for field in fields:
                values = (
                    db.scalars(
                        select(DataSourceInstance.value_numeric).where(DataSourceInstance.field_id == field.id)
                    ).all()
                    if field.name == "Salary"
                    else db.scalars(
                        select(DataSourceInstance.value_nominal).where(DataSourceInstance.field_id == field.id)
                    ).all()
                )
                if field.name == "Salary":
                    values = list(map(_f, values))
                self._df[field.name] = values

    def mine_4ft(self, quantifiers: dict[str, float]) -> cleverminer:
        self._load_data()

        return cleverminer(
            df=self._df,
            proc="4ftMiner",
            quantifiers=quantifiers,
            # quantifiers={"Base": 75, "conf": 0.95},
            # ante=clm_vars(["District"]),
            ante={
                "attributes": [{"name": "District", "type": "seq", "minlen": 1, "maxlen": 1}],
                "minlen": 1,
                "maxlen": 1,
                "type": "con",
            },
            succ={
                "attributes": [{"name": "Salary", "type": "seq", "minlen": 1, "maxlen": 1}],
                "minlen": 1,
                "maxlen": 1,
                "type": "con",
            },
            # succ={"attributes": [clm_subset("Salary")], "minlen": 1, "maxlen": 1, "type": "con"},
        )


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
    if not base_candidates:
        print("No base candidates!")
        sys.exit(1)
    if len(base_candidates) > 1:
        print("More than 1 Base candidates")
    confidence_candidates = list(
        filter(lambda x: x.interest_measure.lower() == "confidence", ts.interest_measure_settings)
    )
    if not confidence_candidates:
        print("No confidence candidates!")
        sys.exit(1)
    if len(confidence_candidates) > 1:
        print("More than 1 conf candidates")

    quantifiers = {"Base": base_candidates[0].threshold, "conf": confidence_candidates[0].threshold}

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

    # breakpoint()

    svc = MinerService()
    cm = svc.mine_4ft(quantifiers=quantifiers)
    cm.print_summary()
    cm.print_rulelist()
