import pytest

from easyminer.parsers.pmml.miner import (
    PMML,
    CoefficientType,
    DBASettingType,
    LiteralSign,
    SimplePmmlParser,
)


def test_simple_pmml_parser_basic():
    """Complete PMML with required namespaces"""
    xml_content = """<?xml version="1.0" encoding="UTF-8"?>
    <PMML xmlns="http://www.dmg.org/PMML-4_0"
          version="4.0"
          xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
          xmlns:pmml="http://www.dmg.org/PMML-4_0"
          xsi:schemaLocation="http://www.dmg.org/PMML-4_0 http://easyminer.eu/schemas/PMML4.0+GUHA0.1.xsd">
      <Header copyright="Copyright (c) KIZI UEP, 2025">
        <Extension name="author" value="testuser"/>
        <Extension name="subsystem" value="R"/>
        <Extension name="module" value="Apriori-R"/>
        <Extension name="format" value="4ftMiner.Task"/>
        <Extension name="dataset" value="TestDataset"/>
        <Extension name="database-type" value="limited"/>
        <Extension name="database-server" value="http://localhost:3306"/>
        <Extension name="database-name" value="testdb"/>
        <Extension name="database-user" value="user"/>
        <Extension name="database-password" value="pass"/>
        <Application name="EasyMiner" version="1.0"/>
        <Timestamp>2025-01-15 12:00:00 GMT +01:00</Timestamp>
      </Header>

      <DataDictionary/>
      <TransformationDictionary/>

      <guha:AssociationModel xmlns:guha="http://keg.vse.cz/ns/GUHA0.1rev1"
                             xmlns=""
                             modelName="123"
                             functionName="associationRules"
                             algorithmName="4ft">
        <TaskSetting>
          <Extension name="LISp-Miner">
            <HypothesesCountMax>1000</HypothesesCountMax>
          </Extension>

          <BBASettings>
            <BBASetting id="1">
              <Text>Age</Text>
              <Name>Age</Name>
              <FieldRef>Age</FieldRef>
              <Coefficient>
                <Type>One category</Type>
                <Category>young</Category>
              </Coefficient>
            </BBASetting>
          </BBASettings>

          <DBASettings>
            <DBASetting id="10" type="Conjunction">
              <BASettingRef>20</BASettingRef>
              <MinimalLength>1</MinimalLength>
            </DBASetting>
            <DBASetting id="20" type="Conjunction">
              <BASettingRef>30</BASettingRef>
              <MinimalLength>0</MinimalLength>
            </DBASetting>
            <DBASetting id="30" type="Literal">
              <BASettingRef>1</BASettingRef>
              <LiteralSign>Positive</LiteralSign>
            </DBASetting>
          </DBASettings>

          <ConsequentSetting>10</ConsequentSetting>

          <InterestMeasureSetting>
            <InterestMeasureThreshold id="100">
              <InterestMeasure>CONF</InterestMeasure>
              <Threshold>0.5</Threshold>
            </InterestMeasureThreshold>
          </InterestMeasureSetting>
        </TaskSetting>
      </guha:AssociationModel>
    </PMML>"""

    parser = SimplePmmlParser(xml_content)
    pmml = parser.parse()

    assert isinstance(pmml, PMML)
    assert pmml.version == "4.0"
    assert pmml.header.copyright == "Copyright (c) KIZI UEP, 2025"
    assert pmml.association_model.model_name == "123"


def test_header_parsing_all_extensions():
    """Header with all R backend extensions"""
    xml_content = """<?xml version="1.0" encoding="UTF-8"?>
    <PMML xmlns="http://www.dmg.org/PMML-4_0" version="4.0">
      <Header copyright="Test Copyright">
        <Extension name="author" value="john_doe"/>
        <Extension name="subsystem" value="R"/>
        <Extension name="module" value="Apriori-R"/>
        <Extension name="format" value="4ftMiner.Task"/>
        <Extension name="dataset" value="MyDataset"/>
        <Extension name="database-type" value="limited"/>
        <Extension name="database-server" value="mysql://localhost:3306"/>
        <Extension name="database-name" value="mydb"/>
        <Extension name="database-user" value="dbuser"/>
        <Extension name="database-password" value="secret"/>
        <Application name="EasyMiner" version="2.0"/>
        <Timestamp>2025-11-15 10:30:00 GMT +02:00</Timestamp>
      </Header>

      <DataDictionary/>
      <TransformationDictionary/>

      <guha:AssociationModel xmlns:guha="http://keg.vse.cz/ns/GUHA0.1rev1" xmlns=""
                             modelName="1" functionName="associationRules" algorithmName="4ft">
        <TaskSetting>
          <Extension name="LISp-Miner">
            <HypothesesCountMax>100</HypothesesCountMax>
          </Extension>
          <ConsequentSetting>1</ConsequentSetting>
        </TaskSetting>
      </guha:AssociationModel>
    </PMML>"""

    parser = SimplePmmlParser(xml_content)
    pmml = parser.parse()

    assert pmml.header.copyright == "Test Copyright"
    assert pmml.header.application_name == "EasyMiner"
    assert pmml.header.application_version == "2.0"
    assert pmml.header.timestamp == "2025-11-15 10:30:00 GMT +02:00"

    # Check all extensions
    extensions_dict = {ext.name: ext.value for ext in pmml.header.extensions}
    assert extensions_dict["author"] == "john_doe"
    assert extensions_dict["subsystem"] == "R"
    assert extensions_dict["module"] == "Apriori-R"
    assert extensions_dict["format"] == "4ftMiner.Task"
    assert extensions_dict["dataset"] == "MyDataset"
    assert extensions_dict["database-type"] == "limited"
    assert extensions_dict["database-server"] == "mysql://localhost:3306"
    assert extensions_dict["database-name"] == "mydb"
    assert extensions_dict["database-user"] == "dbuser"
    assert extensions_dict["database-password"] == "secret"


def test_bba_setting_one_category():
    """BBA with One category coefficient"""
    xml_content = """<?xml version="1.0" encoding="UTF-8"?>
    <PMML xmlns="http://www.dmg.org/PMML-4_0" version="4.0">
      <Header><Application name="Test" version="1.0"/></Header>
      <DataDictionary/>
      <TransformationDictionary/>
      <guha:AssociationModel xmlns:guha="http://keg.vse.cz/ns/GUHA0.1rev1" xmlns=""
                             modelName="1" functionName="associationRules" algorithmName="4ft">
        <TaskSetting>
          <Extension name="LISp-Miner">
            <HypothesesCountMax>100</HypothesesCountMax>
          </Extension>
          <BBASettings>
            <BBASetting id="1">
              <Text>Age attribute</Text>
              <Name>Age</Name>
              <FieldRef>age-preprocessed</FieldRef>
              <Coefficient>
                <Type>One category</Type>
                <Category>young</Category>
              </Coefficient>
            </BBASetting>
          </BBASettings>
          <ConsequentSetting>1</ConsequentSetting>
        </TaskSetting>
      </guha:AssociationModel>
    </PMML>"""

    parser = SimplePmmlParser(xml_content)
    pmml = parser.parse()

    bba_settings = pmml.association_model.task_setting.bba_settings
    assert len(bba_settings) == 1

    bba = bba_settings[0]
    assert bba.id == "1"
    assert bba.text == "Age attribute"
    assert bba.name == "Age"
    assert bba.field_ref == "age-preprocessed"
    assert bba.coefficient.type == CoefficientType.one_category
    assert bba.coefficient.category == "young"


def test_bba_setting_subset():
    """BBA with Subset coefficient"""
    xml_content = """<?xml version="1.0" encoding="UTF-8"?>
    <PMML xmlns="http://www.dmg.org/PMML-4_0" version="4.0">
      <Header><Application name="Test" version="1.0"/></Header>
      <DataDictionary/>
      <TransformationDictionary/>
      <guha:AssociationModel xmlns:guha="http://keg.vse.cz/ns/GUHA0.1rev1" xmlns=""
                             modelName="1" functionName="associationRules" algorithmName="4ft">
        <TaskSetting>
          <Extension name="LISp-Miner">
            <HypothesesCountMax>100</HypothesesCountMax>
          </Extension>
          <BBASettings>
            <BBASetting id="2">
              <Text>Salary</Text>
              <Name>Salary</Name>
              <FieldRef>salary-intervals</FieldRef>
              <Coefficient>
                <Type>Subset</Type>
                <MinimalLength>1</MinimalLength>
                <MaximalLength>3</MaximalLength>
              </Coefficient>
            </BBASetting>
          </BBASettings>
          <ConsequentSetting>1</ConsequentSetting>
        </TaskSetting>
      </guha:AssociationModel>
    </PMML>"""

    parser = SimplePmmlParser(xml_content)
    pmml = parser.parse()

    bba_settings = pmml.association_model.task_setting.bba_settings
    bba = bba_settings[0]

    assert bba.id == "2"
    assert bba.coefficient.type == CoefficientType.subset
    assert bba.coefficient.minimal_length == 1
    assert bba.coefficient.maximal_length == 3
    assert bba.coefficient.category is None  # No category for Subset type


def test_dba_settings_three_level_structure():
    """DBA 3-level hierarchy (Conjunction/Conjunction/Literal)"""
    xml_content = """<?xml version="1.0" encoding="UTF-8"?>
    <PMML xmlns="http://www.dmg.org/PMML-4_0" version="4.0">
      <Header><Application name="Test" version="1.0"/></Header>
      <DataDictionary/>
      <TransformationDictionary/>
      <guha:AssociationModel xmlns:guha="http://keg.vse.cz/ns/GUHA0.1rev1" xmlns=""
                             modelName="1" functionName="associationRules" algorithmName="4ft">
        <TaskSetting>
          <Extension name="LISp-Miner">
            <HypothesesCountMax>100</HypothesesCountMax>
          </Extension>

          <BBASettings>
            <BBASetting id="1">
              <Text>Age</Text>
              <Name>Age</Name>
              <FieldRef>age</FieldRef>
              <Coefficient>
                <Type>One category</Type>
                <Category>young</Category>
              </Coefficient>
            </BBASetting>
            <BBASetting id="2">
              <Text>Salary</Text>
              <Name>Salary</Name>
              <FieldRef>salary</FieldRef>
              <Coefficient>
                <Type>One category</Type>
                <Category>low</Category>
              </Coefficient>
            </BBASetting>
          </BBASettings>

          <DBASettings>
            <!-- Level 1: Top cedent (must be Conjunction) -->
            <DBASetting id="10" type="Conjunction">
              <BASettingRef>20</BASettingRef>
              <MinimalLength>1</MinimalLength>
            </DBASetting>

            <!-- Level 2: Partial cedent (can be Conjunction or Disjunction) -->
            <DBASetting id="20" type="Conjunction">
              <BASettingRef>30</BASettingRef>
              <BASettingRef>31</BASettingRef>
              <MinimalLength>2</MinimalLength>
            </DBASetting>

            <!-- Level 3: Literals -->
            <DBASetting id="30" type="Literal">
              <BASettingRef>1</BASettingRef>
              <LiteralSign>Positive</LiteralSign>
            </DBASetting>
            <DBASetting id="31" type="Literal">
              <BASettingRef>2</BASettingRef>
              <LiteralSign>Positive</LiteralSign>
            </DBASetting>
          </DBASettings>

          <AntecedentSetting>10</AntecedentSetting>
          <ConsequentSetting>10</ConsequentSetting>
        </TaskSetting>
      </guha:AssociationModel>
    </PMML>"""

    parser = SimplePmmlParser(xml_content)
    pmml = parser.parse()

    dba_settings = pmml.association_model.task_setting.dba_settings
    assert len(dba_settings) == 4

    # Level 1 (top cedent)
    level1 = next(d for d in dba_settings if d.id == "10")
    assert level1.type == DBASettingType.conjunction
    assert level1.ba_refs == ["20"]
    assert level1.minimal_length == 1

    # Level 2 (partial cedent)
    level2 = next(d for d in dba_settings if d.id == "20")
    assert level2.type == DBASettingType.conjunction
    assert level2.ba_refs == ["30", "31"]
    assert level2.minimal_length == 2

    # Level 3 (literals)
    literal1 = next(d for d in dba_settings if d.id == "30")
    assert literal1.ba_refs == ["1"]
    assert literal1.literal_sign == LiteralSign.positive

    literal2 = next(d for d in dba_settings if d.id == "31")
    assert literal2.ba_refs == ["2"]
    assert literal2.literal_sign == LiteralSign.positive


def test_dba_settings_with_disjunction():
    """DBA with Disjunction at Level 2"""
    xml_content = """<?xml version="1.0" encoding="UTF-8"?>
    <PMML xmlns="http://www.dmg.org/PMML-4_0" version="4.0">
      <Header><Application name="Test" version="1.0"/></Header>
      <DataDictionary/>
      <TransformationDictionary/>
      <guha:AssociationModel xmlns:guha="http://keg.vse.cz/ns/GUHA0.1rev1" xmlns=""
                             modelName="1" functionName="associationRules" algorithmName="4ft">
        <TaskSetting>
          <Extension name="LISp-Miner">
            <HypothesesCountMax>100</HypothesesCountMax>
          </Extension>

          <BBASettings>
            <BBASetting id="1">
              <Text>Age=young</Text>
              <Name>Age</Name>
              <FieldRef>age</FieldRef>
              <Coefficient>
                <Type>One category</Type>
                <Category>young</Category>
              </Coefficient>
            </BBASetting>
            <BBASetting id="2">
              <Text>Age=old</Text>
              <Name>Age</Name>
              <FieldRef>age</FieldRef>
              <Coefficient>
                <Type>One category</Type>
                <Category>old</Category>
              </Coefficient>
            </BBASetting>
          </BBASettings>

          <DBASettings>
            <!-- Level 1 -->
            <DBASetting id="10" type="Conjunction">
              <BASettingRef>20</BASettingRef>
              <MinimalLength>1</MinimalLength>
            </DBASetting>

            <!-- Level 2: Disjunction -->
            <DBASetting id="20" type="Disjunction">
              <BASettingRef>30</BASettingRef>
              <BASettingRef>31</BASettingRef>
              <MinimalLength>1</MinimalLength>
            </DBASetting>

            <!-- Level 3: Literals -->
            <DBASetting id="30" type="Literal">
              <BASettingRef>1</BASettingRef>
              <LiteralSign>Positive</LiteralSign>
            </DBASetting>
            <DBASetting id="31" type="Literal">
              <BASettingRef>2</BASettingRef>
              <LiteralSign>Positive</LiteralSign>
            </DBASetting>
          </DBASettings>

          <AntecedentSetting>10</AntecedentSetting>
          <ConsequentSetting>10</ConsequentSetting>
        </TaskSetting>
      </guha:AssociationModel>
    </PMML>"""

    parser = SimplePmmlParser(xml_content)
    pmml = parser.parse()

    dba_settings = pmml.association_model.task_setting.dba_settings

    # Check Disjunction at level 2
    disjunction = next(d for d in dba_settings if d.id == "20")
    assert disjunction.type == DBASettingType.disjunction
    assert disjunction.ba_refs == ["30", "31"]
    assert disjunction.minimal_length == 1  # At least one must be satisfied


def test_dba_settings_negative_literal():
    """DBA with Negative literal sign"""
    xml_content = """<?xml version="1.0" encoding="UTF-8"?>
    <PMML xmlns="http://www.dmg.org/PMML-4_0" version="4.0">
      <Header><Application name="Test" version="1.0"/></Header>
      <DataDictionary/>
      <TransformationDictionary/>
      <guha:AssociationModel xmlns:guha="http://keg.vse.cz/ns/GUHA0.1rev1" xmlns=""
                             modelName="1" functionName="associationRules" algorithmName="4ft">
        <TaskSetting>
          <Extension name="LISp-Miner">
            <HypothesesCountMax>100</HypothesesCountMax>
          </Extension>

          <BBASettings>
            <BBASetting id="1">
              <Text>Risk</Text>
              <Name>Risk</Name>
              <FieldRef>risk</FieldRef>
              <Coefficient>
                <Type>One category</Type>
                <Category>high</Category>
              </Coefficient>
            </BBASetting>
          </BBASettings>

          <DBASettings>
            <DBASetting id="10" type="Conjunction">
              <BASettingRef>20</BASettingRef>
              <MinimalLength>1</MinimalLength>
            </DBASetting>
            <DBASetting id="20" type="Conjunction">
              <BASettingRef>30</BASettingRef>
              <MinimalLength>0</MinimalLength>
            </DBASetting>
            <!-- Negative literal: NOT(Risk=high) -->
            <DBASetting id="30" type="Literal">
              <BASettingRef>1</BASettingRef>
              <LiteralSign>Negative</LiteralSign>
            </DBASetting>
          </DBASettings>

          <ConsequentSetting>10</ConsequentSetting>
        </TaskSetting>
      </guha:AssociationModel>
    </PMML>"""

    parser = SimplePmmlParser(xml_content)
    pmml = parser.parse()

    dba_settings = pmml.association_model.task_setting.dba_settings
    literal = next(d for d in dba_settings if d.id == "30")

    assert literal.literal_sign == LiteralSign.negative


def test_antecedent_and_consequent_settings():
    """Antecedent and Consequent references"""
    xml_content = """<?xml version="1.0" encoding="UTF-8"?>
    <PMML xmlns="http://www.dmg.org/PMML-4_0" version="4.0">
      <Header><Application name="Test" version="1.0"/></Header>
      <DataDictionary/>
      <TransformationDictionary/>
      <guha:AssociationModel xmlns:guha="http://keg.vse.cz/ns/GUHA0.1rev1" xmlns=""
                             modelName="1" functionName="associationRules" algorithmName="4ft">
        <TaskSetting>
          <Extension name="LISp-Miner">
            <HypothesesCountMax>100</HypothesesCountMax>
          </Extension>

          <BBASettings>
            <BBASetting id="1">
              <Text>Test</Text>
              <Name>Test</Name>
              <FieldRef>test</FieldRef>
              <Coefficient>
                <Type>One category</Type>
                <Category>value</Category>
              </Coefficient>
            </BBASetting>
          </BBASettings>

          <DBASettings>
            <DBASetting id="10" type="Conjunction">
              <BASettingRef>20</BASettingRef>
              <MinimalLength>1</MinimalLength>
            </DBASetting>
            <DBASetting id="20" type="Conjunction">
              <BASettingRef>30</BASettingRef>
              <MinimalLength>0</MinimalLength>
            </DBASetting>
            <DBASetting id="30" type="Literal">
              <BASettingRef>1</BASettingRef>
              <LiteralSign>Positive</LiteralSign>
            </DBASetting>

            <DBASetting id="11" type="Conjunction">
              <BASettingRef>21</BASettingRef>
              <MinimalLength>1</MinimalLength>
            </DBASetting>
            <DBASetting id="21" type="Conjunction">
              <BASettingRef>31</BASettingRef>
              <MinimalLength>0</MinimalLength>
            </DBASetting>
            <DBASetting id="31" type="Literal">
              <BASettingRef>1</BASettingRef>
              <LiteralSign>Positive</LiteralSign>
            </DBASetting>
          </DBASettings>

          <AntecedentSetting>10</AntecedentSetting>
          <ConsequentSetting>11</ConsequentSetting>
        </TaskSetting>
      </guha:AssociationModel>
    </PMML>"""

    parser = SimplePmmlParser(xml_content)
    pmml = parser.parse()

    task_setting = pmml.association_model.task_setting
    assert task_setting.antecedent_setting == "10"
    assert task_setting.consequent_setting == "11"


def test_interest_measure_conf_and_supp():
    """CONF and SUPP with ThresholdType and CompareType"""
    xml_content = """<?xml version="1.0" encoding="UTF-8"?>
    <PMML xmlns="http://www.dmg.org/PMML-4_0" version="4.0">
      <Header><Application name="Test" version="1.0"/></Header>
      <DataDictionary/>
      <TransformationDictionary/>
      <guha:AssociationModel xmlns:guha="http://keg.vse.cz/ns/GUHA0.1rev1" xmlns=""
                             modelName="1" functionName="associationRules" algorithmName="4ft">
        <TaskSetting>
          <Extension name="LISp-Miner">
            <HypothesesCountMax>100</HypothesesCountMax>
          </Extension>

          <ConsequentSetting>1</ConsequentSetting>

          <InterestMeasureSetting>
            <InterestMeasureThreshold id="100">
              <InterestMeasure>CONF</InterestMeasure>
              <Threshold>0.5</Threshold>
              <ThresholdType>% of all</ThresholdType>
              <CompareType>Greater than or equal</CompareType>
            </InterestMeasureThreshold>

            <InterestMeasureThreshold id="101">
              <InterestMeasure>SUPP</InterestMeasure>
              <Threshold>0.01</Threshold>
              <ThresholdType>% of all</ThresholdType>
              <CompareType>Greater than or equal</CompareType>
            </InterestMeasureThreshold>
          </InterestMeasureSetting>
        </TaskSetting>
      </guha:AssociationModel>
    </PMML>"""

    parser = SimplePmmlParser(xml_content)
    pmml = parser.parse()

    im_settings = pmml.association_model.task_setting.interest_measure_settings
    assert len(im_settings) == 2

    conf = next(im for im in im_settings if im.interest_measure == "CONF")
    assert conf.id == "100"
    assert conf.threshold == 0.5
    assert conf.threshold_type == "% of all"
    assert conf.compare_type == "Greater than or equal"

    supp = next(im for im in im_settings if im.interest_measure == "SUPP")
    assert supp.id == "101"
    assert supp.threshold == 0.01
    assert supp.threshold_type == "% of all"
    assert supp.compare_type == "Greater than or equal"


def test_interest_measure_rule_length():
    """RULE_LENGTH with Abs ThresholdType"""
    xml_content = """<?xml version="1.0" encoding="UTF-8"?>
    <PMML xmlns="http://www.dmg.org/PMML-4_0" version="4.0">
      <Header><Application name="Test" version="1.0"/></Header>
      <DataDictionary/>
      <TransformationDictionary/>
      <guha:AssociationModel xmlns:guha="http://keg.vse.cz/ns/GUHA0.1rev1" xmlns=""
                             modelName="1" functionName="associationRules" algorithmName="4ft">
        <TaskSetting>
          <Extension name="LISp-Miner">
            <HypothesesCountMax>100</HypothesesCountMax>
          </Extension>

          <ConsequentSetting>1</ConsequentSetting>

          <InterestMeasureSetting>
            <InterestMeasureThreshold id="100">
              <InterestMeasure>CONF</InterestMeasure>
              <Threshold>0.5</Threshold>
              <ThresholdType>% of all</ThresholdType>
              <CompareType>Greater than or equal</CompareType>
            </InterestMeasureThreshold>
            <InterestMeasureThreshold id="101">
              <InterestMeasure>SUPP</InterestMeasure>
              <Threshold>0.01</Threshold>
              <ThresholdType>% of all</ThresholdType>
              <CompareType>Greater than or equal</CompareType>
            </InterestMeasureThreshold>
            <InterestMeasureThreshold id="102">
              <InterestMeasure>RULE_LENGTH</InterestMeasure>
              <Threshold>5.0</Threshold>
              <ThresholdType>Abs</ThresholdType>
              <CompareType>Less than or equal</CompareType>
            </InterestMeasureThreshold>
          </InterestMeasureSetting>
        </TaskSetting>
      </guha:AssociationModel>
    </PMML>"""

    parser = SimplePmmlParser(xml_content)
    pmml = parser.parse()

    im_settings = pmml.association_model.task_setting.interest_measure_settings
    rule_length = next(im for im in im_settings if im.interest_measure == "RULE_LENGTH")

    assert rule_length.id == "102"
    assert rule_length.threshold == 5.0
    assert rule_length.threshold_type == "Abs"
    assert rule_length.compare_type == "Less than or equal"


def test_interest_measure_additional_measures():
    """Optional measures (LIFT, BASE, AAD)"""
    xml_content = """<?xml version="1.0" encoding="UTF-8"?>
    <PMML xmlns="http://www.dmg.org/PMML-4_0" version="4.0">
      <Header><Application name="Test" version="1.0"/></Header>
      <DataDictionary/>
      <TransformationDictionary/>
      <guha:AssociationModel xmlns:guha="http://keg.vse.cz/ns/GUHA0.1rev1" xmlns=""
                             modelName="1" functionName="associationRules" algorithmName="4ft">
        <TaskSetting>
          <Extension name="LISp-Miner">
            <HypothesesCountMax>100</HypothesesCountMax>
          </Extension>

          <ConsequentSetting>1</ConsequentSetting>

          <InterestMeasureSetting>
            <InterestMeasureThreshold id="100">
              <InterestMeasure>CONF</InterestMeasure>
              <Threshold>0.5</Threshold>
              <ThresholdType>% of all</ThresholdType>
              <CompareType>Greater than or equal</CompareType>
            </InterestMeasureThreshold>
            <InterestMeasureThreshold id="101">
              <InterestMeasure>SUPP</InterestMeasure>
              <Threshold>0.01</Threshold>
              <ThresholdType>% of all</ThresholdType>
              <CompareType>Greater than or equal</CompareType>
            </InterestMeasureThreshold>
            <InterestMeasureThreshold id="102">
              <InterestMeasure>LIFT</InterestMeasure>
              <Threshold>1.2</Threshold>
              <ThresholdType>% of all</ThresholdType>
              <CompareType>Greater than or equal</CompareType>
            </InterestMeasureThreshold>
            <InterestMeasureThreshold id="103">
              <InterestMeasure>BASE</InterestMeasure>
              <Threshold>0.05</Threshold>
              <ThresholdType>% of all</ThresholdType>
              <CompareType>Greater than or equal</CompareType>
            </InterestMeasureThreshold>
            <InterestMeasureThreshold id="104">
              <InterestMeasure>AAD</InterestMeasure>
              <Threshold>0.1</Threshold>
              <ThresholdType>% of all</ThresholdType>
              <CompareType>Greater than or equal</CompareType>
            </InterestMeasureThreshold>
          </InterestMeasureSetting>
        </TaskSetting>
      </guha:AssociationModel>
    </PMML>"""

    parser = SimplePmmlParser(xml_content)
    pmml = parser.parse()

    im_settings = pmml.association_model.task_setting.interest_measure_settings
    assert len(im_settings) == 5

    # Check each measure exists with correct types
    for im in im_settings:
        if im.interest_measure in ["CONF", "SUPP", "LIFT", "BASE", "AAD"]:
            assert im.threshold_type == "% of all"
            assert im.compare_type == "Greater than or equal"

    # Check specific values
    measures = {im.interest_measure: im.threshold for im in im_settings}
    assert measures["CONF"] == 0.5
    assert measures["SUPP"] == 0.01
    assert measures["LIFT"] == 1.2
    assert measures["BASE"] == 0.05
    assert measures["AAD"] == 0.1


def test_lispm_miner_hypotheses_count_max():
    """LISp-Miner extension with HypothesesCountMax"""
    xml_content = """<?xml version="1.0" encoding="UTF-8"?>
    <PMML xmlns="http://www.dmg.org/PMML-4_0" version="4.0">
      <Header><Application name="Test" version="1.0"/></Header>
      <DataDictionary/>
      <TransformationDictionary/>
      <guha:AssociationModel xmlns:guha="http://keg.vse.cz/ns/GUHA0.1rev1" xmlns=""
                             modelName="1" functionName="associationRules" algorithmName="4ft">
        <TaskSetting>
          <Extension name="LISp-Miner">
            <HypothesesCountMax>10000</HypothesesCountMax>
          </Extension>

          <ConsequentSetting>1</ConsequentSetting>
        </TaskSetting>
      </guha:AssociationModel>
    </PMML>"""

    parser = SimplePmmlParser(xml_content)
    pmml = parser.parse()

    task_setting = pmml.association_model.task_setting
    assert task_setting.lispm_miner_hypotheses_max == 10000


def test_association_model_attributes():
    """AssociationModel attributes (modelName, functionName, algorithmName)"""
    xml_content = """<?xml version="1.0" encoding="UTF-8"?>
    <PMML xmlns="http://www.dmg.org/PMML-4_0" version="4.0">
      <Header><Application name="Test" version="1.0"/></Header>
      <DataDictionary/>
      <TransformationDictionary/>
      <guha:AssociationModel xmlns:guha="http://keg.vse.cz/ns/GUHA0.1rev1" xmlns=""
                             modelName="task_12345"
                             functionName="associationRules"
                             algorithmName="4ft">
        <TaskSetting>
          <Extension name="LISp-Miner">
            <HypothesesCountMax>100</HypothesesCountMax>
          </Extension>
          <ConsequentSetting>1</ConsequentSetting>
        </TaskSetting>
      </guha:AssociationModel>
    </PMML>"""

    parser = SimplePmmlParser(xml_content)
    pmml = parser.parse()

    model = pmml.association_model
    assert model.model_name == "task_12345"
    assert model.function_name == "associationRules"
    assert model.algorithm_name == "4ft"


def test_interest_measure_threshold_type_case_sensitivity():
    """ThresholdType case sensitivity ("% of all" vs "Abs")"""
    xml_content = """<?xml version="1.0" encoding="UTF-8"?>
    <PMML xmlns="http://www.dmg.org/PMML-4_0" version="4.0">
      <Header><Application name="Test" version="1.0"/></Header>
      <DataDictionary/>
      <TransformationDictionary/>
      <guha:AssociationModel xmlns:guha="http://keg.vse.cz/ns/GUHA0.1rev1" xmlns=""
                             modelName="1" functionName="associationRules" algorithmName="4ft">
        <TaskSetting>
          <Extension name="LISp-Miner">
            <HypothesesCountMax>100</HypothesesCountMax>
          </Extension>
          <ConsequentSetting>1</ConsequentSetting>

          <InterestMeasureSetting>
            <InterestMeasureThreshold id="100">
              <InterestMeasure>CONF</InterestMeasure>
              <Threshold>0.5</Threshold>
              <ThresholdType>% of all</ThresholdType>
              <CompareType>Greater than or equal</CompareType>
            </InterestMeasureThreshold>
            <InterestMeasureThreshold id="101">
              <InterestMeasure>RULE_LENGTH</InterestMeasure>
              <Threshold>5.0</Threshold>
              <ThresholdType>Abs</ThresholdType>
              <CompareType>Less than or equal</CompareType>
            </InterestMeasureThreshold>
          </InterestMeasureSetting>
        </TaskSetting>
      </guha:AssociationModel>
    </PMML>"""

    parser = SimplePmmlParser(xml_content)
    pmml = parser.parse()

    im_settings = pmml.association_model.task_setting.interest_measure_settings

    # Verify exact case sensitivity
    conf = next(im for im in im_settings if im.interest_measure == "CONF")
    assert conf.threshold_type == "% of all"  # Must be lowercase "of" and "all"

    rule_length = next(im for im in im_settings if im.interest_measure == "RULE_LENGTH")
    assert rule_length.threshold_type == "Abs"  # Must be capital A, lowercase bs


def test_interest_measure_compare_type_variations():
    """CompareType variations (Greater/Less/Equal)"""
    xml_content = """<?xml version="1.0" encoding="UTF-8"?>
    <PMML xmlns="http://www.dmg.org/PMML-4_0" version="4.0">
      <Header><Application name="Test" version="1.0"/></Header>
      <DataDictionary/>
      <TransformationDictionary/>
      <guha:AssociationModel xmlns:guha="http://keg.vse.cz/ns/GUHA0.1rev1" xmlns=""
                             modelName="1" functionName="associationRules" algorithmName="4ft">
        <TaskSetting>
          <Extension name="LISp-Miner">
            <HypothesesCountMax>100</HypothesesCountMax>
          </Extension>
          <ConsequentSetting>1</ConsequentSetting>

          <InterestMeasureSetting>
            <InterestMeasureThreshold id="100">
              <InterestMeasure>CONF</InterestMeasure>
              <Threshold>0.5</Threshold>
              <ThresholdType>% of all</ThresholdType>
              <CompareType>Greater than or equal</CompareType>
            </InterestMeasureThreshold>
            <InterestMeasureThreshold id="101">
              <InterestMeasure>RULE_LENGTH</InterestMeasure>
              <Threshold>5.0</Threshold>
              <ThresholdType>Abs</ThresholdType>
              <CompareType>Less than or equal</CompareType>
            </InterestMeasureThreshold>
            <InterestMeasureThreshold id="102">
              <InterestMeasure>AUTO_CONF_SUPP</InterestMeasure>
              <Threshold>0.8</Threshold>
              <ThresholdType>% of all</ThresholdType>
              <CompareType>Equal</CompareType>
            </InterestMeasureThreshold>
          </InterestMeasureSetting>
        </TaskSetting>
      </guha:AssociationModel>
    </PMML>"""

    parser = SimplePmmlParser(xml_content)
    pmml = parser.parse()

    im_settings = pmml.association_model.task_setting.interest_measure_settings

    conf = next(im for im in im_settings if im.interest_measure == "CONF")
    assert conf.compare_type == "Greater than or equal"

    rule_length = next(im for im in im_settings if im.interest_measure == "RULE_LENGTH")
    assert rule_length.compare_type == "Less than or equal"

    auto_conf_supp = next(im for im in im_settings if im.interest_measure == "AUTO_CONF_SUPP")
    assert auto_conf_supp.compare_type == "Equal"


def test_data_dictionary_and_transformation_dictionary_empty():
    """Empty DataDictionary and TransformationDictionary"""
    xml_content = """<?xml version="1.0" encoding="UTF-8"?>
    <PMML xmlns="http://www.dmg.org/PMML-4_0" version="4.0">
      <Header><Application name="Test" version="1.0"/></Header>
      <DataDictionary/>
      <TransformationDictionary/>
      <guha:AssociationModel xmlns:guha="http://keg.vse.cz/ns/GUHA0.1rev1" xmlns=""
                             modelName="1" functionName="associationRules" algorithmName="4ft">
        <TaskSetting>
          <Extension name="LISp-Miner">
            <HypothesesCountMax>100</HypothesesCountMax>
          </Extension>
          <ConsequentSetting>1</ConsequentSetting>
        </TaskSetting>
      </guha:AssociationModel>
    </PMML>"""

    parser = SimplePmmlParser(xml_content)
    pmml = parser.parse()

    # Verify elements are parsed but empty
    assert pmml.data_dictionary is not None
    assert pmml.transformation_dictionary is not None
    assert pmml.data_dictionary.number_of_fields is None  # Empty, no fields


def test_data_dictionary_with_number_of_fields():
    """DataDictionary with numberOfFields attribute"""
    xml_content = """<?xml version="1.0" encoding="UTF-8"?>
    <PMML xmlns="http://www.dmg.org/PMML-4_0" version="4.0">
      <Header><Application name="Test" version="1.0"/></Header>
      <DataDictionary numberOfFields="3"/>
      <TransformationDictionary/>
      <guha:AssociationModel xmlns:guha="http://keg.vse.cz/ns/GUHA0.1rev1" xmlns=""
                             modelName="1" functionName="associationRules" algorithmName="4ft">
        <TaskSetting>
          <Extension name="LISp-Miner">
            <HypothesesCountMax>100</HypothesesCountMax>
          </Extension>
          <ConsequentSetting>1</ConsequentSetting>
        </TaskSetting>
      </guha:AssociationModel>
    </PMML>"""

    parser = SimplePmmlParser(xml_content)
    pmml = parser.parse()

    assert pmml.data_dictionary is not None
    assert pmml.data_dictionary.number_of_fields == 3


def test_complete_mining_task_example():
    """Complete mining task (Age=young → LoanStatus=bad)"""
    xml_content = """<?xml version="1.0" encoding="UTF-8"?>
    <PMML xmlns="http://www.dmg.org/PMML-4_0" version="4.0">
      <Header copyright="Test">
        <Extension name="author" value="testuser"/>
        <Extension name="subsystem" value="R"/>
        <Extension name="module" value="Apriori-R"/>
        <Extension name="format" value="4ftMiner.Task"/>
        <Extension name="dataset" value="LoanData"/>
        <Extension name="database-type" value="limited"/>
        <Extension name="database-server" value="http://localhost:3306"/>
        <Extension name="database-name" value="loans"/>
        <Extension name="database-user" value="user"/>
        <Extension name="database-password" value="pass"/>
        <Application name="EasyMiner" version="1.0"/>
        <Timestamp>2025-11-15 12:00:00 GMT +01:00</Timestamp>
      </Header>

      <DataDictionary/>
      <TransformationDictionary/>

      <guha:AssociationModel xmlns:guha="http://keg.vse.cz/ns/GUHA0.1rev1" xmlns=""
                             modelName="loan_task_1"
                             functionName="associationRules"
                             algorithmName="4ft">
        <TaskSetting>
          <Extension name="LISp-Miner">
            <HypothesesCountMax>1000</HypothesesCountMax>
          </Extension>

          <BBASettings>
            <BBASetting id="1">
              <Text>Age</Text>
              <Name>Age</Name>
              <FieldRef>Age</FieldRef>
              <Coefficient>
                <Type>One category</Type>
                <Category>young</Category>
              </Coefficient>
            </BBASetting>

            <BBASetting id="2">
              <Text>LoanStatus</Text>
              <Name>LoanStatus</Name>
              <FieldRef>LoanStatus</FieldRef>
              <Coefficient>
                <Type>One category</Type>
                <Category>bad</Category>
              </Coefficient>
            </BBASetting>
          </BBASettings>

          <DBASettings>
            <!-- Antecedent: Age=young -->
            <DBASetting id="10" type="Conjunction">
              <BASettingRef>20</BASettingRef>
              <MinimalLength>1</MinimalLength>
            </DBASetting>
            <DBASetting id="20" type="Conjunction">
              <BASettingRef>30</BASettingRef>
              <MinimalLength>0</MinimalLength>
            </DBASetting>
            <DBASetting id="30" type="Literal">
              <BASettingRef>1</BASettingRef>
              <LiteralSign>Positive</LiteralSign>
            </DBASetting>

            <!-- Consequent: LoanStatus=bad -->
            <DBASetting id="11" type="Conjunction">
              <BASettingRef>21</BASettingRef>
              <MinimalLength>1</MinimalLength>
            </DBASetting>
            <DBASetting id="21" type="Conjunction">
              <BASettingRef>31</BASettingRef>
              <MinimalLength>0</MinimalLength>
            </DBASetting>
            <DBASetting id="31" type="Literal">
              <BASettingRef>2</BASettingRef>
              <LiteralSign>Positive</LiteralSign>
            </DBASetting>
          </DBASettings>

          <AntecedentSetting>10</AntecedentSetting>
          <ConsequentSetting>11</ConsequentSetting>

          <InterestMeasureSetting>
            <InterestMeasureThreshold id="100">
              <InterestMeasure>CONF</InterestMeasure>
              <Threshold>0.5</Threshold>
              <ThresholdType>% of all</ThresholdType>
              <CompareType>Greater than or equal</CompareType>
            </InterestMeasureThreshold>
            <InterestMeasureThreshold id="101">
              <InterestMeasure>SUPP</InterestMeasure>
              <Threshold>0.01</Threshold>
              <ThresholdType>% of all</ThresholdType>
              <CompareType>Greater than or equal</CompareType>
            </InterestMeasureThreshold>
            <InterestMeasureThreshold id="102">
              <InterestMeasure>RULE_LENGTH</InterestMeasure>
              <Threshold>5.0</Threshold>
              <ThresholdType>Abs</ThresholdType>
              <CompareType>Less than or equal</CompareType>
            </InterestMeasureThreshold>
          </InterestMeasureSetting>
        </TaskSetting>
      </guha:AssociationModel>
    </PMML>"""

    parser = SimplePmmlParser(xml_content)
    pmml = parser.parse()

    # Verify complete structure
    assert pmml.version == "4.0"
    assert pmml.header.copyright == "Test"

    # Verify DataDictionary and TransformationDictionary
    assert pmml.data_dictionary is not None
    assert pmml.transformation_dictionary is not None

    model = pmml.association_model
    assert model.model_name == "loan_task_1"
    assert model.function_name == "associationRules"
    assert model.algorithm_name == "4ft"

    task = model.task_setting
    assert task.lispm_miner_hypotheses_max == 1000
    assert len(task.bba_settings) == 2
    assert len(task.dba_settings) == 6
    assert task.antecedent_setting == "10"
    assert task.consequent_setting == "11"
    assert len(task.interest_measure_settings) == 3

    # Verify all interest measures have complete fields
    for im in task.interest_measure_settings:
        assert im.threshold_type is not None
        assert im.compare_type is not None

        if im.interest_measure in ["CONF", "SUPP"]:
            assert im.threshold_type == "% of all"
            assert im.compare_type == "Greater than or equal"
        elif im.interest_measure == "RULE_LENGTH":
            assert im.threshold_type == "Abs"
            assert im.compare_type == "Less than or equal"


def test_data_dictionary_and_transformation_dictionary_missing():
    """Missing DataDictionary/TransformationDictionary raises error"""
    xml_content = """<?xml version="1.0" encoding="UTF-8"?>
    <PMML xmlns="http://www.dmg.org/PMML-4_0" version="4.0">
      <Header><Application name="Test" version="1.0"/></Header>
      <guha:AssociationModel xmlns:guha="http://keg.vse.cz/ns/GUHA0.1rev1" xmlns=""
                             modelName="1" functionName="associationRules" algorithmName="4ft">
        <TaskSetting>
          <Extension name="LISp-Miner">
            <HypothesesCountMax>100</HypothesesCountMax>
          </Extension>
          <ConsequentSetting>1</ConsequentSetting>
        </TaskSetting>
      </guha:AssociationModel>
    </PMML>"""

    parser = SimplePmmlParser(xml_content)

    # Missing required elements should raise ValidationError
    with pytest.raises(Exception) as exc_info:
        _ = parser.parse()

    # Verify it's a validation error about missing elements
    error_msg = str(exc_info.value).lower()
    assert "datadictionary" in error_msg or "transformationdictionary" in error_msg or "missing" in error_msg
