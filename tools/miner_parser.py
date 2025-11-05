from typing import Literal
from xml.etree.ElementTree import Element

import defusedxml.ElementTree as ET
from pydantic import BaseModel, Field


class Extension(BaseModel):
    name: str
    value: str


class Header(BaseModel):
    copyright: str | None = None
    application_name: str | None = Field(None, alias="Application_name")
    application_version: str | None = Field(None, alias="Application_version")
    annotation: str | None = None
    timestamp: str | None = None
    extensions: list[Extension] = Field(default_factory=list)


class Coefficient(BaseModel):
    type: str
    minimal_length: int | None = None
    maximal_length: int | None = None
    category: str | None = None


class BBASetting(BaseModel):
    id: str
    text: str
    name: str
    field_ref: str = Field(alias="FieldRef")
    coefficient: Coefficient


class DBASetting(BaseModel):
    id: str
    type: str
    ba_refs: list[str] = Field(default_factory=list, alias="BASettingRef")
    minimal_length: int
    literal_sign: str | None = None


class InterestMeasureThreshold(BaseModel):
    id: str
    interest_measure: str = Field(alias="InterestMeasure")
    threshold: float = Field(alias="Threshold")
    threshold_type: str | None = Field(None, alias="ThresholdType")
    compare_type: str | None = Field(None, alias="CompareType")


class TaskSetting(BaseModel):
    lispm_miner_hypotheses_max: int | None = Field(None, alias="HypothesesCountMax")
    bba_settings: list[BBASetting] = Field(default_factory=list)
    dba_settings: list[DBASetting] = Field(default_factory=list)
    antecedent_setting: str
    consequent_setting: str
    interest_measure_settings: list[InterestMeasureThreshold] = Field(default_factory=list)


class AssociationModel(BaseModel):
    model_name: str
    function_name: str
    algorithm_name: Literal["4ft"] = "4ft"
    task_setting: TaskSetting


class PMML(BaseModel):
    version: str
    header: Header
    association_model: AssociationModel


class SimplePmmlParser:
    def __init__(self, xml_string: str):
        self.root: Element = ET.fromstring(xml_string)
        self.ns: dict[str, str] = {
            "pmml": "http://www.dmg.org/PMML-4_0",
            "guha": "http://keg.vse.cz/ns/GUHA0.1rev1",
            # The empty key "" is used by ElementTree for the default namespace if one is active.
            # We will use this for root-level default namespaced elements.
            # For elements under xmlns="", we search without a namespace.
        }
        # Define the default namespace URI separately for clarity when using it.
        self.default_pmml_ns_uri: str = "http://www.dmg.org/PMML-4_0"

    def _get_text(self, element: Element | None) -> str | None:
        return element.text.strip() if element is not None and element.text else None

    def _get_int(self, element: Element | None) -> int | None:
        text = self._get_text(element)
        try:
            return int(text) if text else None
        except ValueError:
            return None

    def _get_float(self, element: Element | None) -> float | None:
        text = self._get_text(element)
        try:
            return float(text) if text else None
        except ValueError:
            return None

    def _parse_extensions(self, parent_element: Element) -> list[Extension]:
        extensions: list[Extension] = []
        # Extensions in Header are pmml:Extension
        for ext_node in parent_element.findall("pmml:Extension", self.ns):
            name = ext_node.get("name")
            value = ext_node.get("value")
            if name and value:
                extensions.append(Extension(name=name, value=value))
        return extensions

    def parse_header(self) -> Header:
        # Header and its children are in the default PMML namespace
        header_node = self.root.find(
            "pmml:Header", self.ns
        )  # More explicit than using {"": self.default_pmml_ns_uri} here
        if header_node is None:
            header_node = self.root.find(
                f"{{{self.default_pmml_ns_uri}}}Header"
            )  # Try with Clark notation if prefixed fails
            if header_node is None:
                raise ValueError("Header not found in PMML")

        app_node = header_node.find(f"{{{self.default_pmml_ns_uri}}}Application")
        timestamp_node = header_node.find(f"{{{self.default_pmml_ns_uri}}}Timestamp")
        annotation_node = header_node.find(f"{{{self.default_pmml_ns_uri}}}Annotation")

        return Header(
            copyright=header_node.get("copyright"),
            Application_name=app_node.get("name") if app_node is not None else None,
            Application_version=app_node.get("version") if app_node is not None else None,
            annotation=self._get_text(annotation_node),
            timestamp=self._get_text(timestamp_node),
            extensions=self._parse_extensions(header_node),  # _parse_extensions uses pmml: prefix
        )

    def parse_bba_settings(self, task_setting_node: Element) -> list[BBASetting]:
        bba_settings_list: list[BBASetting] = []
        # BBASettings and its children are in "no namespace" due to xmlns="" on TaskSetting
        bba_settings_node = task_setting_node.find("BBASettings")  # No namespace
        if bba_settings_node is None:
            return bba_settings_list

        for bba_node in bba_settings_node.findall("BBASetting"):  # No namespace
            id_val = bba_node.get("id")
            text_val = self._get_text(bba_node.find("Text"))
            name_val = self._get_text(bba_node.find("Name"))
            field_ref_val = self._get_text(bba_node.find("FieldRef"))

            coeff_node = bba_node.find("Coefficient")  # No namespace
            coeff = None
            if coeff_node is not None:
                coeff_type_node = coeff_node.find("Type")
                coeff_type = self._get_text(coeff_type_node) if coeff_type_node is not None else "Unknown"

                if coeff_type == "Subset":
                    coeff = Coefficient(
                        type=coeff_type,
                        minimal_length=self._get_int(coeff_node.find("MinimalLength")),
                        maximal_length=self._get_int(coeff_node.find("MaximalLength")),
                    )
                elif coeff_type == "One category":
                    coeff = Coefficient(type=coeff_type, category=self._get_text(coeff_node.find("Category")))
                else:
                    coeff = Coefficient(type=coeff_type)

            if id_val and text_val and name_val and field_ref_val and coeff:
                bba_settings_list.append(
                    BBASetting(
                        id=id_val,
                        text=text_val,
                        name=name_val,
                        FieldRef=field_ref_val,
                        coefficient=coeff,
                    )
                )
        return bba_settings_list

    def parse_dba_settings(self, task_setting_node: Element) -> list[DBASetting]:
        dba_settings_list: list[DBASetting] = []
        dba_settings_node = task_setting_node.find("DBASettings")  # No namespace
        if dba_settings_node is None:
            return dba_settings_list

        for dba_node in dba_settings_node.findall("DBASetting"):  # No namespace
            id_val = dba_node.get("id")
            type_val = dba_node.get("type")

            ba_refs = [
                self._get_text(ref_node) for ref_node in dba_node.findall("BASettingRef") if self._get_text(ref_node)
            ]  # No namespace

            min_len_node = dba_node.find("MinimalLength")  # No namespace
            min_len = self._get_int(min_len_node)

            literal_sign_node = dba_node.find("LiteralSign")  # No namespace
            literal_sign = self._get_text(literal_sign_node)

            if id_val and type_val and min_len is not None:
                dba_settings_list.append(
                    DBASetting(
                        id=id_val,
                        type=type_val,
                        BASettingRef=ba_refs,
                        minimal_length=min_len,
                        literal_sign=literal_sign,
                    )
                )
        return dba_settings_list

    def parse_interest_measures(self, task_setting_node: Element) -> list[InterestMeasureThreshold]:
        im_list: list[InterestMeasureThreshold] = []
        im_setting_node = task_setting_node.find("InterestMeasureSetting")  # No namespace
        if im_setting_node is None:
            return im_list

        for im_node in im_setting_node.findall("InterestMeasureThreshold"):  # No namespace
            id_val = im_node.get("id")
            measure_text = self._get_text(im_node.find("InterestMeasure"))
            threshold_val = self._get_float(im_node.find("Threshold"))
            threshold_type_text = self._get_text(im_node.find("ThresholdType"))
            compare_type_text = self._get_text(im_node.find("CompareType"))

            if id_val and measure_text:
                im_list.append(
                    InterestMeasureThreshold(
                        id=id_val,
                        InterestMeasure=measure_text,
                        Threshold=threshold_val,
                        ThresholdType=threshold_type_text,
                        CompareType=compare_type_text,
                    )
                )
        return im_list

    def parse_task_setting(self, model_node: Element) -> TaskSetting:
        # TaskSetting is a child of guha:AssociationModel and has xmlns="" -> no namespace
        task_setting_node = model_node.find("TaskSetting")
        if task_setting_node is None:
            raise ValueError("TaskSetting not found in AssociationModel")

        hypotheses_max = None
        # Extension LISp-Miner is under TaskSetting (no namespace)
        lispm_extension_node = task_setting_node.find("Extension[@name='LISp-Miner']")  # No namespace
        if lispm_extension_node is not None:
            hypotheses_max_node = lispm_extension_node.find("HypothesesCountMax")  # No namespace
            hypotheses_max = self._get_int(hypotheses_max_node)

        bba_settings = self.parse_bba_settings(task_setting_node)
        dba_settings = self.parse_dba_settings(task_setting_node)
        interest_measures = self.parse_interest_measures(task_setting_node)

        antecedent_node = task_setting_node.find("AntecedentSetting")  # No namespace
        consequent_node = task_setting_node.find("ConsequentSetting")  # No namespace

        return TaskSetting(
            HypothesesCountMax=hypotheses_max,
            bba_settings=bba_settings,
            dba_settings=dba_settings,
            antecedent_setting=self._get_text(antecedent_node),
            consequent_setting=self._get_text(consequent_node),
            interest_measure_settings=interest_measures,
        )

    def parse_association_model(self) -> AssociationModel:
        # AssociationModel is explicitly guha:AssociationModel
        model_node = self.root.find("guha:AssociationModel", self.ns)
        if model_node is None:
            raise ValueError("guha:AssociationModel not found in PMML")

        task_setting = self.parse_task_setting(model_node)

        return AssociationModel(
            model_name=model_node.get("modelName"),
            function_name=model_node.get("functionName"),
            algorithm_name=model_node.get("algorithmName"),
            task_setting=task_setting,
        )

    def parse(self) -> PMML:
        header = self.parse_header()
        association_model = self.parse_association_model()

        return PMML(version=self.root.get("version"), header=header, association_model=association_model)


if __name__ == "__main__":
    from easyminer.config import ROOT_DIR

    with open(ROOT_DIR / "miner_pmml.xml") as f:
        example_filled_xml_string = f.read()

    print("--- Parsing example filled XML String ---")
    try:
        parser = SimplePmmlParser(example_filled_xml_string)
        parsed_pmml = parser.parse()

        print("\n--- Parsed PMML Object (from example_filled_xml_string) ---")
        print(parsed_pmml.model_dump_json(indent=2))
        # ... (rest of the print statements from before) ...
        print("--- End Parsed PMML Object ---\n")

        print("--- Quick Meaningful Results (from example_filled_xml_string) ---")
        print(f"PMML Version: {parsed_pmml.version}")
        print(f"Application: {parsed_pmml.header.application_name} v{parsed_pmml.header.application_version}")
        print(
            f"Dataset Extension:{next((ext.value for ext in parsed_pmml.header.extensions if ext.name == 'dataset'), 'N/A')}"
        )

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
                f"  ID: {im.id}, Measure: {im.interest_measure}, Threshold: {im.threshold}, ThresholdType: '{im.threshold_type}', CompareType: '{im.compare_type}'"
            )

    except ET.ParseError as e:
        # Correctly accessing ParseError attributes
        print(f"XML Parsing Error: {e.msg} (line {e.lineno}, column {e.offset})")
    except ValueError as e:
        print(f"Data Parsing Error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e.__class__.__name__}: {e}")
