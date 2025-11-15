from enum import Enum
from typing import Literal

from pydantic_xml import BaseXmlModel, attr, element

NSMAP: dict[str, str] = {
    "": "http://www.dmg.org/PMML-4_0",  # Default PMML namespace
    "guha": "http://keg.vse.cz/ns/GUHA0.1rev1",
    "xsi": "http://www.w3.org/2001/XMLSchema-instance",
}

# Empty namespace for TaskSetting
EMPTY_NSMAP = {"": ""}


class LiteralSign(Enum):
    positive = "Positive"
    negative = "Negative"


class CoefficientType(Enum):
    one_category = "One category"
    subset = "Subset"
    nominal = "Nominal"
    sequence = "Sequence"


class DBASettingType(Enum):
    conjunction = "Conjunction"
    disjunction = "Disjunction"
    literal = "Literal"


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


class Coefficient(BaseXmlModel, tag="Coefficient", nsmap=EMPTY_NSMAP):
    type: CoefficientType = element("Type")
    minimal_length: int = element("MinimalLength", default=1)
    maximal_length: int = element("MaximalLength", default=1)
    category: str | None = element("Category", default=None)


class BBASetting(BaseXmlModel, tag="BBASetting", nsmap=EMPTY_NSMAP):
    id: str = attr()
    text: str = element("Text")
    name: str = element("Name")
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
    id: str = attr()
    interest_measure: str = element("InterestMeasure")
    threshold: float | None = element("Threshold", default=None)
    threshold_type: str | None = element("ThresholdType", default=None)
    compare_type: str | None = element("CompareType", default=None)


class InterestMeasureSetting(BaseXmlModel, tag="InterestMeasureSetting", nsmap=EMPTY_NSMAP):
    interest_measure_thresholds: list[InterestMeasureThreshold] = element(
        "InterestMeasureThreshold", default_factory=list
    )


class LispMinerExtension(BaseXmlModel, tag="Extension", nsmap=EMPTY_NSMAP):
    name: str = attr()
    hypotheses_count_max: int = element("HypothesesCountMax")


class TaskSetting(BaseXmlModel, tag="TaskSetting", nsmap=EMPTY_NSMAP):
    extensions: list[LispMinerExtension] = element("Extension", default_factory=list)
    bba_settings_container: BBASettings | None = element("BBASettings", default=None)
    dba_settings_container: DBASettings | None = element("DBASettings", default=None)
    antecedent_setting: str | None = element("AntecedentSetting", default=None)
    consequent_setting: str | None = element("ConsequentSetting", default=None)
    interest_measure_container: InterestMeasureSetting | None = element("InterestMeasureSetting", default=None)

    @property
    def lispm_miner_hypotheses_max(self) -> int | None:
        """Extract hypotheses max from LISp-Miner extension"""
        for ext in self.extensions:
            if ext.name == "LISp-Miner":
                return ext.hypotheses_count_max
        return None

    @property
    def bba_settings(self) -> list[BBASetting]:
        """Get BBA settings from container"""
        return self.bba_settings_container.bba_settings if self.bba_settings_container else []

    @property
    def dba_settings(self) -> list[DBASetting]:
        """Get DBA settings from container"""
        return self.dba_settings_container.dba_settings if self.dba_settings_container else []

    @property
    def interest_measure_settings(self) -> list[InterestMeasureThreshold]:
        """Get interest measure settings from container"""
        return self.interest_measure_container.interest_measure_thresholds if self.interest_measure_container else []


class AssociationModel(BaseXmlModel, tag="AssociationModel", ns="guha", nsmap=NSMAP):
    model_name: str = attr(alias="modelName")
    function_name: str = attr(alias="functionName")
    algorithm_name: Literal["4ft"] = attr(alias="algorithmName", default="4ft")
    task_setting: TaskSetting = element("TaskSetting", ns="")


class DataDictionary(BaseXmlModel, tag="DataDictionary", nsmap=NSMAP):
    """DataDictionary element - typically empty for mining requests"""

    number_of_fields: int | None = attr(name="numberOfFields", default=None)


class TransformationDictionary(BaseXmlModel, tag="TransformationDictionary", nsmap=NSMAP):
    """TransformationDictionary element - typically empty for mining requests"""

    pass


class PMML(BaseXmlModel, tag="PMML", search_mode="unordered", nsmap=NSMAP):
    version: str = attr()
    schema_location: str | None = attr(alias="schemaLocation", ns="xsi", default=None)
    header: Header = element("Header")
    data_dictionary: DataDictionary = element("DataDictionary")
    transformation_dictionary: TransformationDictionary = element("TransformationDictionary")
    association_model: AssociationModel = element("AssociationModel", ns="guha")


class SimplePmmlParser:
    """Simple wrapper for parsing PMML XML strings using pydantic-xml"""

    def __init__(self, xml_string: str):
        self.xml_string: str = xml_string

    def parse(self) -> PMML:
        """Parse the XML string into a PMML object"""
        return PMML.from_xml(bytes(self.xml_string, "utf-8"))
