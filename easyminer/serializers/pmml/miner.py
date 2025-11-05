from typing import Literal

import pandas as pd
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
    nsmap={
        "guha": "http://keg.vse.cz/ns/GUHA0.1rev1",
        "": "http://www.dmg.org/PMML-4_0",
    },
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
    arules_data: list[
        dict[
            Literal["id", "id_antecedent", "id_consequent", "a", "b", "c", "d", "text"],
            str | int,
        ]
    ],
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


# ============================================================================
# Current Production Functions - pyARC/fim based
# ============================================================================


def calculate_fourfold_table(rule, transactions_df: pd.DataFrame, total_transactions: int) -> dict[str, int]:
    # Extract rule components
    antecedent_items = rule.antecedent
    consequent_attr = rule.consequent.attribute
    consequent_val = rule.consequent.value

    support = rule.support / 100.0  # Convert to decimal
    confidence = rule.confidence / 100.0

    # Cell a: both antecedent and consequent are TRUE
    # Formula: a = support ? N
    a = int(round(support * total_transactions))

    # Cell b: antecedent TRUE, consequent FALSE
    # Formula: b = (a / confidence) - a
    # Derived from: confidence = a / (a+b)
    if confidence > 0:
        a_plus_b = a / confidence
        b = int(round(a_plus_b - a))
    else:
        b = 0

    # Cell c: antecedent FALSE, consequent TRUE
    # Direct count from dataset (GUHA definition)
    mask_consequent = transactions_df[consequent_attr] == consequent_val
    mask_antecedent = pd.Series([True] * len(transactions_df), index=transactions_df.index)

    for attr, val in antecedent_items:
        mask_antecedent &= transactions_df[attr] == val

    c = int((mask_consequent & ~mask_antecedent).sum())

    # Cell d: both antecedent and consequent are FALSE
    # Formula: d = N - a - b - c
    d = total_transactions - a - b - c

    # Validation
    if a < 0 or b < 0 or c < 0 or d < 0:
        raise ValueError(f"Invalid contingency table: negative values detected. a={a}, b={b}, c={c}, d={d}")

    total = a + b + c + d
    if total != total_transactions:
        raise ValueError(f"Invalid contingency table: sum mismatch. a+b+c+d={total}, expected={total_transactions}")

    return {"a": a, "b": b, "c": c, "d": d}


def create_pmml_result_from_pyarc(
    rules: list,
    transactions_df: pd.DataFrame,
    total_transactions: int,
    total_attributes: int,
    headers_data: list[dict] | None = None,
) -> PMML:
    """
    Create PMML result from pyARC mining results (fim.arules or pyARC CARs).

    This function handles both:
    - fim.arules output: list of (consequent, antecedent, support, confidence) tuples
    - pyARC CARs: list of CAR objects with .antecedent, .consequent, .support, .confidence

    Args:
        rules: List of rules (fim tuples or pyARC CAR objects)
        transactions_df: Original DataFrame with all transactions (needed for 4ft calculation)
        total_transactions: Total number of transactions
        total_attributes: Total number of attribute values (for numberOfCategories)
        headers_data: Optional list of header extension dictionaries with keys: name, value

    Returns:
        PMML object ready for XML serialization
    """
    number_of_rules = len(rules)

    # Detect rule format (fim tuples vs pyARC CARs)
    if not rules:
        bbas = []
        dbas = []
        arules = []
    elif isinstance(rules[0], tuple):
        # fim.arules format: (consequent, antecedent, support, confidence)
        raise NotImplementedError(
            "fim.arules tuple format not yet implemented. Please convert to pyARC CARs using createCARs() first."
        )
    else:
        # pyARC CAR format
        # Collect all unique attribute-value combinations for BBAs
        attribute_value_combinations: set[tuple[str, str]] = set()

        for rule in rules:
            # Process antecedent
            for attr, value in rule.antecedent:
                attribute_value_combinations.add((attr, value))

            # Process consequent
            attribute_value_combinations.add((rule.consequent.attribute, rule.consequent.value))

        # Create BBAs with sequential IDs
        bbas = []
        bba_lookup = {}  # (attr_name, value) -> bba_id

        for i, (attr_name, value) in enumerate(sorted(attribute_value_combinations), 1):
            bba_id = str(i)
            bba_lookup[(attr_name, value)] = bba_id

            bbas.append(
                BBA(
                    id=bba_id,
                    text=f"{attr_name}({value})",
                    field_ref=attr_name,
                    cat_ref=value,
                )
            )

        # Create DBAs and Association Rules
        dbas = []
        arules = []
        dba_counter = len(bbas) + 1

        for rule_idx, rule in enumerate(rules, 1):
            rule_id = str(rule_idx)

            # Create DBA for antecedent (if not empty)
            antecedent_dba_id = None
            if rule.antecedent:
                antecedent_dba_id = str(dba_counter)
                dba_counter += 1

                # Collect BA references for antecedent
                ante_ba_refs = []
                ante_texts = []
                for attr, value in rule.antecedent:
                    ante_ba_refs.append(bba_lookup[(attr, value)])
                    ante_texts.append(f"{attr}({value})")

                dbas.append(
                    DBA(
                        id=antecedent_dba_id,
                        text=" AND ".join(ante_texts),
                        ba_refs=ante_ba_refs,
                    )
                )

            # Create DBA for consequent
            consequent_dba_id = str(dba_counter)
            dba_counter += 1

            cons_attr = rule.consequent.attribute
            cons_value = rule.consequent.value

            dbas.append(
                DBA(
                    id=consequent_dba_id,
                    text=f"{cons_attr}({cons_value})",
                    ba_refs=[bba_lookup[(cons_attr, cons_value)]],
                )
            )

            fourfold = calculate_fourfold_table(
                rule=rule, transactions_df=transactions_df, total_transactions=total_transactions
            )

            if rule.antecedent:
                ante_text = " AND ".join([f"{attr}({value})" for attr, value in rule.antecedent])
                rule_text = f"{ante_text} => {cons_attr}({cons_value})"
            else:
                rule_text = f"=> {cons_attr}({cons_value})"

            arules.append(
                AssociationRule(
                    id=rule_id,
                    antecedent=antecedent_dba_id,
                    consequent=consequent_dba_id,
                    text=rule_text,
                    four_ft_table=FourFtTable(
                        a=fourfold["a"],
                        b=fourfold["b"],
                        c=fourfold["c"],
                        d=fourfold["d"],
                    ),
                )
            )

    association_rules = AssociationRules(bbas=bbas, dbas=dbas, arules=arules)

    association_model = AssociationModel(
        algorithm_name="4ft",  # Standard GUHA 4ft
        number_of_transactions=total_transactions,
        number_of_categories=total_attributes,
        number_of_rules=number_of_rules,
        association_rules=association_rules,
    )

    header = None
    if headers_data:
        extensions = [Extension(name=h["name"], value=h["value"]) for h in headers_data]
        header = Header(extensions=extensions)
    else:
        extensions = [
            Extension(name="algorithm", value="pyarc-fim"),
            Extension(name="implementation", value="easyminer-python"),
        ]
        header = Header(extensions=extensions)

    return PMML(header=header, association_model=association_model)
