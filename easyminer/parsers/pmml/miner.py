from enum import Enum
from typing import Literal

from pydantic_xml import BaseXmlModel, attr, element

NSMAP: dict[str, str] = {
    "": "http://www.dmg.org/PMML-4_2",  # Default PMML namespace
    "guha": "http://keg.vse.cz/ns/GUHA0.1rev1",
    "xsi": "http://www.w3.org/2001/XMLSchema-instance",
}

# Empty namespace for TaskSetting children inside guha:AssociationModel (xmlns="")
EMPTY_NSMAP = {"": ""}


# ── Shared enums ──────────────────────────────────────────────────────────────


class LiteralSign(Enum):
    positive = "Positive"
    negative = "Negative"


class CoefficientType(Enum):
    one_category = "One category"
    subset = "Subset"
    nominal = "Nominal"
    sequence = "Sequence"
    all = "All"


class DBASettingType(Enum):
    conjunction = "Conjunction"
    disjunction = "Disjunction"
    literal = "Literal"


# ── Shared Header models (PMML namespace, same in both formats) ───────────────


class Extension(BaseXmlModel, tag="Extension", nsmap=NSMAP):
    name: str = attr()
    value: str = attr()


class Application(BaseXmlModel, tag="Application", nsmap=NSMAP):
    name: str = attr()
    version: str = attr()


class Header(BaseXmlModel, tag="Header", search_mode="unordered", nsmap=NSMAP):
    copyright: str | None = attr(default=None)
    application: Application | None = element(default=None)
    annotation: str | None = element(default=None)
    timestamp: str | None = element(tag="Timestamp", default=None)
    extensions: list[Extension] = element(default_factory=list)

    @property
    def application_name(self) -> str | None:
        return self.application.name if self.application else None

    @property
    def application_version(self) -> str | None:
        return self.application.version if self.application else None


# ── Flat format models (all in PMML namespace) ───────────────────────────────
# Used for the simplified format: TaskSetting directly under PMML,
# BBASetting/DBASetting/InterestMeasureThreshold as flat children.


class FlatCoefficient(BaseXmlModel, tag="Coefficient", nsmap=NSMAP):
    type: CoefficientType = element("Type")
    minimal_length: int = element("MinimalLength", default=1)
    maximal_length: int = element("MaximalLength", default=1)
    category: str | None = element("Category", default=None)


class FlatBBASetting(BaseXmlModel, tag="BBASetting", nsmap=NSMAP):
    id: str = attr()
    text: str | None = element("Text", default=None)
    name: str | None = element("Name", default=None)
    field_ref: str = element("FieldRef")
    coefficient: FlatCoefficient = element("Coefficient")


class FlatDBASetting(BaseXmlModel, tag="DBASetting", nsmap=NSMAP):
    id: str = attr()
    type: DBASettingType = attr(default=DBASettingType.conjunction)
    ba_refs: list[str] = element("BASettingRef", default_factory=list)
    minimal_length: int = element("MinimalLength", default=1)
    maximal_length: int = element("MaximalLength", default=1)
    literal_sign: LiteralSign = element("LiteralSign", default=LiteralSign.positive)


class FlatInterestMeasureThreshold(BaseXmlModel, tag="InterestMeasureThreshold", nsmap=NSMAP):
    id: str | None = attr(default=None)
    interest_measure: str = element("InterestMeasure")
    threshold: float | None = element("Threshold", default=None)
    threshold_type: str = element("ThresholdType", default="% of all")
    compare_type: str = element("CompareType", default="Greater than or equal")


class FlatTaskSetting(BaseXmlModel, tag="TaskSetting", search_mode="unordered", nsmap=NSMAP):
    bba_settings: list[FlatBBASetting] = element("BBASetting", default_factory=list)
    dba_settings: list[FlatDBASetting] = element("DBASetting", default_factory=list)
    interest_measure_settings: list[FlatInterestMeasureThreshold] = element(
        "InterestMeasureThreshold", default_factory=list
    )
    antecedent_setting: str | None = element("AntecedentSetting", default=None)
    consequent_setting: str | None = element("ConsequentSetting", default=None)
    hypotheses_count_max: int | None = element("HypothesesCountMax", default=None)

    @property
    def lispm_miner_hypotheses_max(self) -> int | None:
        return self.hypotheses_count_max


class SimplifiedPMML(BaseXmlModel, tag="PMML", search_mode="unordered", nsmap=NSMAP):
    version: str = attr()
    schema_location: str | None = attr(alias="schemaLocation", ns="xsi", default=None)
    header: Header = element("Header")
    task_setting: FlatTaskSetting = element("TaskSetting")

    def get_task_setting(self) -> FlatTaskSetting:
        return self.task_setting


# ── Envelope format models (empty namespace, inside guha:AssociationModel) ────


class Coefficient(BaseXmlModel, tag="Coefficient", nsmap=EMPTY_NSMAP):
    type: CoefficientType = element("Type")
    minimal_length: int = element("MinimalLength", default=1)
    maximal_length: int = element("MaximalLength", default=1)
    category: str | None = element("Category", default=None)


class BBASetting(BaseXmlModel, tag="BBASetting", nsmap=EMPTY_NSMAP):
    id: str = attr()
    text: str | None = element("Text", default=None)
    name: str | None = element("Name", default=None)
    field_ref: str = element("FieldRef")
    coefficient: Coefficient = element("Coefficient")


class BBASettings(BaseXmlModel, tag="BBASettings", nsmap=EMPTY_NSMAP):
    bba_settings: list[BBASetting] = element("BBASetting", default_factory=list)


class DBASetting(BaseXmlModel, tag="DBASetting", nsmap=EMPTY_NSMAP):
    id: str = attr()
    type: DBASettingType = attr(default=DBASettingType.conjunction)
    ba_refs: list[str] = element("BASettingRef", default_factory=list)
    minimal_length: int = element("MinimalLength", default=1)
    maximal_length: int = element("MaximalLength", default=1)
    literal_sign: LiteralSign = element("LiteralSign", default=LiteralSign.positive)


class DBASettings(BaseXmlModel, tag="DBASettings", nsmap=EMPTY_NSMAP):
    dba_settings: list[DBASetting] = element("DBASetting", default_factory=list)


class InterestMeasureThreshold(BaseXmlModel, tag="InterestMeasureThreshold", nsmap=EMPTY_NSMAP):
    id: str | None = attr(default=None)
    interest_measure: str = element("InterestMeasure")
    threshold: float | None = element("Threshold", default=None)
    threshold_type: str = element("ThresholdType", default="% of all")
    compare_type: str = element("CompareType", default="Greater than or equal")


class InterestMeasureSetting(BaseXmlModel, tag="InterestMeasureSetting", nsmap=EMPTY_NSMAP):
    interest_measure_thresholds: list[InterestMeasureThreshold] = element(
        "InterestMeasureThreshold", default_factory=list
    )


class LispMinerExtension(BaseXmlModel, tag="Extension", nsmap=EMPTY_NSMAP):
    name: str = attr()
    hypotheses_count_max: int = element("HypothesesCountMax")


class TaskSetting(BaseXmlModel, tag="TaskSetting", search_mode="unordered", nsmap=EMPTY_NSMAP):
    extensions: list[LispMinerExtension] = element("Extension", default_factory=list)
    bba_settings_container: BBASettings | None = element("BBASettings", default=None)
    dba_settings_container: DBASettings | None = element("DBASettings", default=None)
    interest_measure_container: InterestMeasureSetting | None = element("InterestMeasureSetting", default=None)
    antecedent_setting: str | None = element("AntecedentSetting", default=None)
    consequent_setting: str | None = element("ConsequentSetting", default=None)
    hypotheses_count_max: int | None = element("HypothesesCountMax", default=None)

    @property
    def lispm_miner_hypotheses_max(self) -> int | None:
        if self.hypotheses_count_max is not None:
            return self.hypotheses_count_max
        for ext in self.extensions:
            if ext.name == "LISp-Miner":
                return ext.hypotheses_count_max
        return None

    @property
    def bba_settings(self) -> list[BBASetting]:
        return self.bba_settings_container.bba_settings if self.bba_settings_container else []

    @property
    def dba_settings(self) -> list[DBASetting]:
        return self.dba_settings_container.dba_settings if self.dba_settings_container else []

    @property
    def interest_measure_settings(self) -> list[InterestMeasureThreshold]:
        return self.interest_measure_container.interest_measure_thresholds if self.interest_measure_container else []


class AssociationModel(BaseXmlModel, tag="AssociationModel", ns="guha", nsmap=NSMAP):
    model_name: str = attr(alias="modelName")
    function_name: str = attr(alias="functionName")
    algorithm_name: Literal["4ft"] = attr(alias="algorithmName", default="4ft")
    task_setting: TaskSetting = element("TaskSetting", ns="")


class DataDictionary(BaseXmlModel, tag="DataDictionary", nsmap=NSMAP):
    number_of_fields: int | None = attr(name="numberOfFields", default=None)


class TransformationDictionary(BaseXmlModel, tag="TransformationDictionary", nsmap=NSMAP):
    pass


class PMML(BaseXmlModel, tag="PMML", search_mode="unordered", nsmap=NSMAP):
    version: str = attr()
    schema_location: str | None = attr(alias="schemaLocation", ns="xsi", default=None)
    header: Header = element("Header")
    data_dictionary: DataDictionary | None = element("DataDictionary", default=None)
    transformation_dictionary: TransformationDictionary | None = element("TransformationDictionary", default=None)
    association_model: AssociationModel | None = element("AssociationModel", ns="guha", default=None)

    def get_task_setting(self) -> TaskSetting:
        if self.association_model:
            return self.association_model.task_setting
        raise ValueError("No TaskSetting found in PMML")


# ── Parser ────────────────────────────────────────────────────────────────────


class SimplePmmlParser:
    """Parse PMML XML strings. Tries flat format first, falls back to envelope."""

    def __init__(self, xml_string: str):
        self.xml_string: str = xml_string

    def parse(self) -> SimplifiedPMML | PMML:
        xml_bytes = self.xml_string.encode("utf-8")
        try:
            return SimplifiedPMML.from_xml(xml_bytes)
        except Exception:
            return PMML.from_xml(xml_bytes)
