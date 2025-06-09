from typing import Literal

from pydantic_xml import BaseXmlModel, attr, element


class Extension(BaseXmlModel, tag="Extension"):
    name: str = attr()
    value: str = attr()


class Header(BaseXmlModel, tag="Header"):
    extensions: list[Extension] = element(tag="Extension", default_factory=list)


class BBA(BaseXmlModel, tag="BBA"):
    id: str = attr()
    literal: bool = attr(default=False)

    text: str = element(tag="Text")
    field_ref: str = element(tag="FieldRef")
    cat_ref: str = element(tag="CatRef")


class DBA(BaseXmlModel, tag="DBA"):
    id: str = attr()
    connective: str = attr(default="Conjunction")
    literal: bool = attr(default=True)

    text: str = element(tag="Text")
    ba_refs: list[str] = element(tag="BARef", default_factory=list)


class FourFtTable(BaseXmlModel, tag="FourFtTable"):
    a: int = attr()
    b: int = attr()
    c: int = attr()
    d: int = attr()


class AssociationRule(BaseXmlModel, tag="AssociationRule"):
    id: str = attr()
    antecedent: str | None = attr(default=None)
    consequent: str = attr()

    text: str = element(tag="Text")
    four_ft_table: FourFtTable = element(tag="FourFtTable")


class AssociationRules(BaseXmlModel, tag="AssociationRules"):
    bbas: list[BBA] = element(tag="BBA", default_factory=list)
    dbas: list[DBA] = element(tag="DBA", default_factory=list)
    arules: list[AssociationRule] = element(tag="AssociationRule", default_factory=list)


class AssociationModel(
    BaseXmlModel,
    tag="AssociationModel",
    ns="guha",
    nsmap={"guha": "http://keg.vse.cz/ns/GUHA0.1rev1", "": "http://www.dmg.org/PMML-4_0"},
):
    model_name: str = attr(name="modelName", default="c402d7406a440a39029c9296fe105eba")
    function_name: str = attr(name="functionName", default="associationRules")
    algorithm_name: str = attr(name="algorithmName", default="4ft")
    number_of_transactions: int = attr(name="numberOfTransactions", default=0)
    number_of_categories: int = attr(name="numberOfCategories", default=0)
    number_of_rules: int = attr(name="numberOfRules")

    association_rules: AssociationRules = element(tag="AssociationRules")


class PMML(
    BaseXmlModel,
    tag="PMML",
    nsmap={
        "": "http://www.dmg.org/PMML-4_0",
        "xsi": "http://www.w3.org/2001/XMLSchema-instance",
        "pmml": "http://www.dmg.org/PMML-4_0",
        "guha": "http://keg.vse.cz/ns/GUHA0.1rev1",
    },
):
    version: str = attr(default="4.0")
    xsi_schema_location: str = attr(
        name="schemaLocation",
        ns="xsi",
        default="http://www.dmg.org/PMML-4_0 http://sewebar.vse.cz/schemas/PMML4.0+GUHA0.1.xsd",
    )

    header: Header | None = element(tag="Header", default=None)
    association_model: AssociationModel = element(tag="AssociationModel", ns="guha")


# Usage example:
def create_pmml_result(
    number_of_rules: int,
    bbas_data: list[dict[Literal["id", "text", "name", "value"], str]],
    dbas_data: list[dict[Literal["id", "text", "barefs"], str | list[str]]],
    arules_data: list[dict[Literal["id", "id_antecedent", "id_consequent", "a", "b", "c", "d", "text"], str | int]],
    headers_data: list[dict[str, str]] | None = None,
) -> PMML:
    """
    Create PMML result from data dictionaries

    Args:
        number_of_rules: Total number of association rules
        bbas_data: List of BBA dictionaries with keys: id, text, name, value
        dbas_data: List of DBA dictionaries with keys: id, text, barefs
        arules_data: List of rule dictionaries with keys: id, id_antecedent, id_consequent, a, b, c, d, text
        headers_data: Optional list of header extension dictionaries with keys: name, value
    """

    # Create BBAs
    bbas = [BBA(id=bba["id"], text=bba["text"], field_ref=bba["name"], cat_ref=bba["value"]) for bba in bbas_data]

    # Create DBAs
    dbas = [DBA(id=dba["id"], text=dba["text"], ba_refs=dba["barefs"]) for dba in dbas_data]

    # Create Association Rules
    arules = [
        AssociationRule(
            id=rule["id"],
            antecedent=rule.get("id_antecedent"),
            consequent=rule["id_consequent"],
            text=rule["text"],
            four_ft_table=FourFtTable(a=rule["a"], b=rule["b"], c=rule["c"], d=rule["d"]),
        )
        for rule in arules_data
    ]

    # Create association rules container
    association_rules = AssociationRules(bbas=bbas, dbas=dbas, arules=arules)

    # Create association model
    association_model = AssociationModel(number_of_rules=number_of_rules, association_rules=association_rules)

    # Create header if extensions provided
    header = None
    if headers_data:
        extensions = [Extension(name=h["name"], value=h["value"]) for h in headers_data]
        header = Header(extensions=extensions)

    # Create PMML root
    return PMML(header=header, association_model=association_model)


def create_pmml_result_from_cleverminer(cleverminer_output: dict, headers_data: list[dict] | None = None) -> PMML:
    """
    Create PMML result from cleverminer algorithm output

    Args:
        cleverminer_output: Direct output dictionary from cleverminer
        headers_data: Optional list of header extension dictionaries with keys: name, value
    """

    # Extract basic information
    number_of_rules = len(cleverminer_output["rules"])

    # Collect all unique attribute-value combinations for BBAs
    attribute_value_combinations: set[tuple[str, str]] = set()

    for rule in cleverminer_output["rules"]:
        # Process antecedent
        for attr_name, values in rule["cedents_struct"]["ante"].items():
            for value in values:
                attribute_value_combinations.add((attr_name, value))

        # Process consequent
        for attr_name, values in rule["cedents_struct"]["succ"].items():
            for value in values:
                attribute_value_combinations.add((attr_name, value))

    # Create BBAs with sequential IDs
    bbas = []
    bba_lookup = {}  # (attr_name, value) -> bba_id

    for i, (attr_name, value) in enumerate(sorted(attribute_value_combinations), 1):
        bba_id = str(i)
        bba_lookup[(attr_name, value)] = bba_id

        bbas.append(BBA(id=bba_id, text=f"{attr_name}({value})", field_ref=attr_name, cat_ref=value))

    # Create DBAs and Association Rules
    dbas = []
    arules = []
    dba_counter = len(bbas) + 1

    for rule in cleverminer_output["rules"]:
        rule_id = str(rule["rule_id"])

        # Create DBA for antecedent (if not empty)
        antecedent_dba_id = None
        if rule["cedents_struct"]["ante"]:
            antecedent_dba_id = str(dba_counter)
            dba_counter += 1

            # Collect BA references for antecedent
            ante_ba_refs = []
            for attr_name, values in rule["cedents_struct"]["ante"].items():
                for value in values:
                    ante_ba_refs.append(bba_lookup[(attr_name, value)])

            dbas.append(DBA(id=antecedent_dba_id, text=rule["cedents_str"]["ante"], ba_refs=ante_ba_refs))

        # Create DBA for consequent
        consequent_dba_id = str(dba_counter)
        dba_counter += 1

        # Collect BA references for consequent
        succ_ba_refs = []
        for attr_name, values in rule["cedents_struct"]["succ"].items():
            for value in values:
                succ_ba_refs.append(bba_lookup[(attr_name, value)])

        dbas.append(DBA(id=consequent_dba_id, text=rule["cedents_str"]["succ"], ba_refs=succ_ba_refs))

        # Extract fourfold table data
        fourfold = rule["params"]["fourfold"]

        # Create Association Rule
        arules.append(
            AssociationRule(
                id=rule_id,
                antecedent=antecedent_dba_id,
                consequent=consequent_dba_id,
                text=f"{rule['cedents_str']['ante']} => {rule['cedents_str']['succ']}",
                four_ft_table=FourFtTable(
                    a=fourfold[0],  # True antecedent, True consequent
                    b=fourfold[1],  # True antecedent, False consequent
                    c=fourfold[2],  # False antecedent, True consequent
                    d=fourfold[3],  # False antecedent, False consequent
                ),
            )
        )

    # Create association rules container
    association_rules = AssociationRules(bbas=bbas, dbas=dbas, arules=arules)

    # Create association model with cleverminer task info
    association_model = AssociationModel(
        algorithm_name="4ft",  # cleverminer uses 4ft natively
        number_of_transactions=cleverminer_output["taskinfo"]["rowcount"],
        number_of_categories=sum(len(categories) for categories in cleverminer_output["datalabels"]["catnames"]),
        number_of_rules=number_of_rules,
        association_rules=association_rules,
    )

    # Create header if extensions provided
    header = None
    if headers_data:
        extensions = [Extension(name=h["name"], value=h["value"]) for h in headers_data]
        header = Header(extensions=extensions)

    # Add cleverminer metadata to headers if no custom headers provided
    if not headers_data:
        extensions = [
            Extension(name="task_type", value=cleverminer_output["taskinfo"]["task_type"]),
            Extension(
                name="total_verifications", value=str(cleverminer_output["summary_statistics"]["total_verifications"])
            ),
            Extension(name="processing_time", value=str(cleverminer_output["summary_statistics"]["time_processing"])),
            Extension(name="algorithm", value="cleverminer-4ft"),
        ]
        header = Header(extensions=extensions)

    # Create PMML root
    return PMML(header=header, association_model=association_model)


# Helper function to extract rule quality metrics
def extract_rule_metrics(cleverminer_output: dict) -> list[dict]:
    """
    Extract rule quality metrics from cleverminer output for analysis
    """
    metrics = []
    for rule in cleverminer_output["rules"]:
        metrics.append(
            {
                "rule_id": rule["rule_id"],
                "base": rule["params"]["base"],
                "confidence": rule["params"]["conf"],
                "relative_base": rule["params"]["rel_base"],
                "aad": rule["params"]["aad"],
                "antecedent": rule["cedents_str"]["ante"],
                "consequent": rule["cedents_str"]["succ"],
                "fourfold_table": rule["params"]["fourfold"],
            }
        )
    return metrics


# Usage example with your cleverminer output:
if __name__ == "__main__":
    # Your cleverminer_output dict goes here...
    import json

    with open("cm_result.json") as f:
        cleverminer_output = json.load(f)

    # Create PMML from cleverminer output
    pmml = create_pmml_result_from_cleverminer(cleverminer_output)

    # Serialize to XML
    xml_output = pmml.to_xml(xml_declaration=True, encoding="UTF-8", pretty_print=True)

    with open("output.pmml", "w") as f:
        f.write(str(xml_output, "utf-8"))

    # Extract metrics for analysis
    metrics = extract_rule_metrics(cleverminer_output)
    print(f"\nExtracted {len(metrics)} rules with metrics")


# Example usage:
# if __name__ == "__main__":
#     # Sample data
#     bbas_data = [
#         {"id": "1", "text": "age(young)", "name": "age", "value": "young"},
#         {"id": "2", "text": "income(high)", "name": "income", "value": "high"},
#     ]
#
#     dbas_data = [
#         {"id": "10", "text": "age(young)", "barefs": ["1"]},
#         {"id": "20", "text": "income(high)", "barefs": ["2"]},
#     ]
#
#     arules_data = [
#         {
#             "id": "r1",
#             "id_antecedent": "10",
#             "id_consequent": "20",
#             "text": "age(young) => income(high)",
#             "a": 150,
#             "b": 50,
#             "c": 100,
#             "d": 700,
#         }
#     ]
#
#     headers_data = [{"name": "dataset", "value": "customer_data"}, {"name": "algorithm", "value": "4ft-miner"}]
#
#     # Create PMML
#     pmml = create_pmml_result(
#         number_of_rules=1, bbas_data=bbas_data, dbas_data=dbas_data, arules_data=arules_data, headers_data=headers_data
#     )
#
#     # Serialize to XML
#     xml_output = pmml.to_xml(xml_declaration=True, encoding="UTF-8", pretty_print=True)
#
#     print(xml_output)
