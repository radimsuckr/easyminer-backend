from typing import Literal

from pydantic_xml import BaseXmlModel, attr, element


class Extension(BaseXmlModel, tag="Extension"):
    name: str = attr()
    value: str = attr()


class Application(BaseXmlModel, tag="Application"):
    name: str | None = attr(default=None)
    version: str | None = attr(default=None)


class Header(BaseXmlModel, tag="Header"):
    copyright: str | None = attr(default=None)
    application: Application | None = element("Application", default=None)
    annotation: str | None = element("Annotation", default=None)
    timestamp: str | None = element("Timestamp", default=None)
    extensions: list[Extension] = element("Extension", default_factory=list)

    @property
    def application_name(self) -> str | None:
        return self.application.name if self.application else None

    @property
    def application_version(self) -> str | None:
        return self.application.version if self.application else None


class Coefficient(BaseXmlModel, tag="Coefficient"):
    type: str = element("Type")
    minimal_length: int | None = element("MinimalLength", default=None)
    maximal_length: int | None = element("MaximalLength", default=None)
    category: str | None = element("Category", default=None)


class BBASetting(BaseXmlModel, tag="BBASetting"):
    id: str = attr()
    text: str = element("Text")
    name: str = element("Name")
    field_ref: str = element("FieldRef")
    coefficient: Coefficient = element("Coefficient")


class BBASettings(BaseXmlModel, tag="BBASettings"):
    bba_settings: list[BBASetting] = element("BBASetting", default_factory=list)


class DBASetting(BaseXmlModel, tag="DBASetting"):
    id: str = attr()
    type: str = attr()
    ba_refs: list[str] = element("BASettingRef", default_factory=list)
    minimal_length: int = element("MinimalLength")
    literal_sign: str | None = element("LiteralSign", default=None)


class DBASettings(BaseXmlModel, tag="DBASettings"):
    dba_settings: list[DBASetting] = element("DBASetting", default_factory=list)


class InterestMeasureThreshold(BaseXmlModel, tag="InterestMeasureThreshold"):
    id: str = attr()
    interest_measure: str = element("InterestMeasure")
    threshold: float = element("Threshold")
    threshold_type: str | None = element("ThresholdType", default=None)
    compare_type: str | None = element("CompareType", default=None)


class InterestMeasureSetting(BaseXmlModel, tag="InterestMeasureSetting"):
    interest_measure_thresholds: list[InterestMeasureThreshold] = element(
        "InterestMeasureThreshold", default_factory=list
    )


class HypothesesCountMax(BaseXmlModel, tag="HypothesesCountMax"):
    value: int = element(tag="", default=None)


class LispMinerExtension(BaseXmlModel, tag="Extension"):
    name: str = attr()
    hypotheses_count_max: int | None = element("HypothesesCountMax", default=None)


# TaskSetting needs to handle xmlns="" which resets namespace to empty
class TaskSetting(BaseXmlModel, tag="TaskSetting", ns=""):
    # All child elements are in empty namespace due to xmlns=""
    extensions: list[LispMinerExtension] = element("Extension", default_factory=list, ns="")
    bba_settings_container: BBASettings | None = element("BBASettings", default=None, ns="")
    dba_settings_container: DBASettings | None = element("DBASettings", default=None, ns="")
    antecedent_setting: str | None = element("AntecedentSetting", default=None, ns="")
    consequent_setting: str | None = element("ConsequentSetting", default=None, ns="")
    interest_measure_container: InterestMeasureSetting | None = element("InterestMeasureSetting", default=None, ns="")

    @property
    def lispm_miner_hypotheses_max(self) -> int | None:
        """Extract hypotheses max from LISp-Miner extension"""
        for ext in self.extensions:
            if ext.name == "LISp-Miner" and ext.hypotheses_count_max is not None:
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


class AssociationModel(BaseXmlModel, tag="AssociationModel"):
    model_name: str = attr(alias="modelName")
    function_name: str = attr(alias="functionName")
    algorithm_name: Literal["4ft"] = attr(alias="algorithmName", default="4ft")
    task_setting: TaskSetting = element("TaskSetting", ns="")  # TaskSetting is in empty namespace


class PMML(
    BaseXmlModel,
    tag="PMML",
    nsmap={
        "": "http://www.dmg.org/PMML-4_0",
        "guha": "http://keg.vse.cz/ns/GUHA0.1rev1",
        "pmml": "http://www.dmg.org/PMML-4_0",
        "xsi": "http://www.w3.org/2001/XMLSchema-instance",
    },
):
    version: str = attr()
    schema_location: str | None = attr(alias="schemaLocation", ns="xsi", default=None)
    header: Header = element("Header", ns="")  # Header is in default namespace
    association_model: AssociationModel = element("AssociationModel", ns="guha")


class SimplePmmlParser:
    """Simple wrapper for parsing PMML XML strings using pydantic-xml"""

    def __init__(self, xml_string: str):
        self.xml_string: str = xml_string

    def parse(self) -> PMML:
        """Parse the XML string into a PMML object"""
        return PMML.from_xml(bytes(self.xml_string, "utf-8"))


if __name__ == "__main__":
    # Test with the provided XML string
    xml_string = """<?xml version="1.0" encoding="UTF-8"?>
<PMML
	xmlns="http://www.dmg.org/PMML-4_0"
	xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
	xmlns:pmml="http://www.dmg.org/PMML-4_0"
	version="4.0"
	xsi:schemaLocation="http://www.dmg.org/PMML-4_0 http://easyminer.eu/schemas/PMML4.0+GUHA0.1.xsd"
>
	<Header copyright="Copyright (c) User-Generated">
		<Application name="CleverMinerConverter" version="1.0" />
		<Annotation>PMML generated from cleverminer function call</Annotation>
		<Timestamp>2023-10-27T10:00:00</Timestamp>
		<Extension name="dataset" value="UserDataFrame" />
	</Header>
	<guha:AssociationModel
		xmlns:guha="http://keg.vse.cz/ns/GUHA0.1rev1"
		modelName="CleverMiner_District_Salary_Rules"
		functionName="associationRules"
		algorithmName="4ft"
	>
		<TaskSetting xmlns="">
			<BBASettings>
				<BBASetting id="bba_district">
					<Text>District</Text>
					<Name>District</Name>
					<FieldRef>District</FieldRef>
					<Coefficient>
						<Type>One category</Type>
						<Category>ANY_DISTRICT_VALUE</Category>
					</Coefficient>
				</BBASetting>
				<BBASetting id="bba_salary">
					<Text>Salary</Text>
					<Name>Salary</Name>
					<FieldRef>Salary</FieldRef>
					<Coefficient>
						<Type>Subset</Type>
						<MinimalLength>1</MinimalLength>
						<MaximalLength>1</MaximalLength>
					</Coefficient>
				</BBASetting>
			</BBASettings>
			<DBASettings>
				<DBASetting id="dba_antecedent" type="Conjunction">
					<BASettingRef>bba_district</BASettingRef>
					<MinimalLength>1</MinimalLength>
				</DBASetting>
				<DBASetting id="dba_succedent" type="Conjunction">
					<BASettingRef>bba_salary</BASettingRef>
					<MinimalLength>1</MinimalLength>
					<LiteralSign>+</LiteralSign>
				</DBASetting>
			</DBASettings>
			<AntecedentSetting>dba_antecedent</AntecedentSetting>
			<ConsequentSetting>dba_succedent</ConsequentSetting>
			<InterestMeasureSetting>
				<InterestMeasureThreshold id="im_base">
					<InterestMeasure>BASE</InterestMeasure>
					<Threshold>75</Threshold>
					<ThresholdType>Min</ThresholdType>
					<CompareType>&gt;=</CompareType>
				</InterestMeasureThreshold>
				<InterestMeasureThreshold id="im_conf">
					<InterestMeasure>Confidence</InterestMeasure>
					<Threshold>0.95</Threshold>
					<ThresholdType></ThresholdType>
					<CompareType></CompareType>
				</InterestMeasureThreshold>
			</InterestMeasureSetting>
		</TaskSetting>
	</guha:AssociationModel>
</PMML>"""

    print("--- Parsing example XML String ---")
    try:
        parser = SimplePmmlParser(xml_string)
        parsed_pmml = parser.parse()

        print("\n--- Parsed PMML Object ---")
        print(parsed_pmml.model_dump_json(indent=2))
        print("--- End Parsed PMML Object ---\n")

        print("--- Quick Meaningful Results ---")
        print(f"PMML Version: {parsed_pmml.version}")
        print(f"Application: {parsed_pmml.header.application_name} v{parsed_pmml.header.application_version}")

        # Find dataset extension
        dataset_ext = next((ext.value for ext in parsed_pmml.header.extensions if ext.name == "dataset"), "N/A")
        print(f"Dataset Extension: {dataset_ext}")

        assoc_model = parsed_pmml.association_model
        print(f"Association Model: {assoc_model.model_name}, Algorithm: {assoc_model.algorithm_name}")

        task_settings = assoc_model.task_setting
        if task_settings.lispm_miner_hypotheses_max is not None:
            print(f"Max Hypotheses (LISp-Miner): {task_settings.lispm_miner_hypotheses_max}")

        print("\nBBA Settings:")
        for bba in task_settings.bba_settings:
            print(f"  ID: {bba.id}, Name: {bba.name}, FieldRef: {bba.field_ref}, Coeff Type: {bba.coefficient.type}")
            if bba.coefficient.type == "One category":
                print(f"    Category: {bba.coefficient.category}")
            elif bba.coefficient.type == "Subset":
                print(f"    MinLen: {bba.coefficient.minimal_length}, MaxLen: {bba.coefficient.maximal_length}")

        print("\nDBA Settings:")
        for dba in task_settings.dba_settings:
            print(f"  ID: {dba.id}, Type: {dba.type}, MinLength: {dba.minimal_length}, BA Refs: {dba.ba_refs}")
            if dba.literal_sign:
                print(f"    Literal Sign: {dba.literal_sign}")

        if task_settings.antecedent_setting:
            print(f"Antecedent Setting ID: {task_settings.antecedent_setting}")
        if task_settings.consequent_setting:
            print(f"Consequent Setting ID: {task_settings.consequent_setting}")

        print("\nInterest Measure Settings:")
        for im in task_settings.interest_measure_settings:
            print(
                f"  ID: {im.id}, Measure: {im.interest_measure}, Threshold: {im.threshold}, "
                f"ThresholdType: '{im.threshold_type}', CompareType: '{im.compare_type}'"
            )

    except Exception as e:
        print(f"An unexpected error occurred: {e.__class__.__name__}: {e}")
        import traceback

        traceback.print_exc()
