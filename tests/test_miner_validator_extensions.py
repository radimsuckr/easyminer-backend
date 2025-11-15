import pytest

from easyminer.parsers.pmml.miner import (
    PMML,
    AssociationModel,
    DataDictionary,
    Extension,
    Header,
    TaskSetting,
    TransformationDictionary,
)
from easyminer.validators.miner import MinerTaskValidationError, MinerTaskValidator


def create_minimal_pmml(extensions: list[Extension]) -> PMML:
    return PMML(
        version="4.0",
        header=Header(extensions=extensions),
        data_dictionary=DataDictionary(),
        transformation_dictionary=TransformationDictionary(),
        association_model=AssociationModel(
            modelName="TestModel",
            functionName="associationRules",
            algorithmName="4ft",
            task_setting=TaskSetting(
                extensions=[],
                bba_settings_container=None,
                dba_settings_container=None,
                interest_measure_container=None,
            ),
        ),
    )


def test_validator_accepts_valid_database_extensions():
    """Validator accepts all required database extensions"""
    extensions = [
        Extension(name="dataset", value="123"),
        Extension(name="database-server", value="localhost:3306"),
        Extension(name="database-name", value="mydb"),
        Extension(name="database-user", value="user"),
        Extension(name="database-password", value="pass"),
    ]

    pmml = create_minimal_pmml(extensions)
    validator = MinerTaskValidator(pmml)

    # Should not raise
    validator._validate_header_extensions()


def test_validator_rejects_missing_database_extensions():
    """Validator rejects missing database extensions"""
    extensions = [
        Extension(name="dataset", value="123"),
        Extension(name="database-server", value="localhost:3306"),
        # Missing database-name, database-user, database-password
    ]

    pmml = create_minimal_pmml(extensions)
    validator = MinerTaskValidator(pmml)

    with pytest.raises(MinerTaskValidationError, match="Missing required database extensions"):
        validator._validate_header_extensions()


def test_validator_rejects_missing_dataset_extension():
    """Validator rejects missing dataset extension"""
    extensions = [
        Extension(name="database-server", value="localhost:3306"),
        Extension(name="database-name", value="mydb"),
        Extension(name="database-user", value="user"),
        Extension(name="database-password", value="pass"),
    ]

    pmml = create_minimal_pmml(extensions)
    validator = MinerTaskValidator(pmml)

    with pytest.raises(MinerTaskValidationError, match="Dataset extension not found"):
        validator._validate_header_extensions()


def test_validator_rejects_invalid_dataset_value():
    """Validator rejects non-integer dataset value"""
    extensions = [
        Extension(name="dataset", value="not-a-number"),
        Extension(name="database-server", value="localhost:3306"),
        Extension(name="database-name", value="mydb"),
        Extension(name="database-user", value="user"),
        Extension(name="database-password", value="pass"),
    ]

    pmml = create_minimal_pmml(extensions)
    validator = MinerTaskValidator(pmml)

    with pytest.raises(MinerTaskValidationError, match="Dataset extension must contain a valid integer"):
        validator._validate_header_extensions()


def test_validator_rejects_server_without_port():
    """Validator rejects database-server without port"""
    extensions = [
        Extension(name="dataset", value="123"),
        Extension(name="database-server", value="localhost"),  # Missing port
        Extension(name="database-name", value="mydb"),
        Extension(name="database-user", value="user"),
        Extension(name="database-password", value="pass"),
    ]

    pmml = create_minimal_pmml(extensions)
    validator = MinerTaskValidator(pmml)

    with pytest.raises(MinerTaskValidationError, match="database-server must contain port"):
        validator._validate_header_extensions()


def test_validator_rejects_invalid_port():
    """Validator rejects non-numeric port"""
    extensions = [
        Extension(name="dataset", value="123"),
        Extension(name="database-server", value="localhost:abc"),
        Extension(name="database-name", value="mydb"),
        Extension(name="database-user", value="user"),
        Extension(name="database-password", value="pass"),
    ]

    pmml = create_minimal_pmml(extensions)
    validator = MinerTaskValidator(pmml)

    with pytest.raises(MinerTaskValidationError, match="database-server must contain valid numeric port"):
        validator._validate_header_extensions()


def test_validator_accepts_server_with_protocol():
    """Validator accepts database-server with mysql:// protocol"""
    extensions = [
        Extension(name="dataset", value="123"),
        Extension(name="database-server", value="mysql://db.example.com:3306"),
        Extension(name="database-name", value="mydb"),
        Extension(name="database-user", value="user"),
        Extension(name="database-password", value="pass"),
    ]

    pmml = create_minimal_pmml(extensions)
    validator = MinerTaskValidator(pmml)

    # Should not raise
    validator._validate_header_extensions()


def test_validator_case_insensitive_extension_names():
    """Validator handles case-insensitive extension names"""
    extensions = [
        Extension(name="Dataset", value="123"),  # Capital D
        Extension(name="Database-Server", value="localhost:3306"),  # Capital letters
        Extension(name="Database-Name", value="mydb"),
        Extension(name="Database-User", value="user"),
        Extension(name="Database-Password", value="pass"),
    ]

    pmml = create_minimal_pmml(extensions)
    validator = MinerTaskValidator(pmml)

    # Should not raise
    validator._validate_header_extensions()
