"""
Validator for mining tasks - ensures compatibility with Scala implementation.

Based on: cz.vse.easyminer.miner.impl.MinerTaskValidatorImpl
"""

from easyminer.parsers.pmml.miner import PMML, InterestMeasureThreshold, TaskSetting


class MinerTaskValidationError(Exception):
    pass


class MinerTaskValidator:
    """Validates mining task parameters according to Scala implementation rules"""

    def __init__(self, pmml: PMML):
        self.pmml: PMML = pmml
        self.task_setting: TaskSetting = pmml.association_model.task_setting
        self.interest_measures: dict[str, InterestMeasureThreshold] = {
            im.interest_measure.upper(): im for im in self.task_setting.interest_measure_settings
        }

    def _has_measure(self, measure_name: str) -> bool:
        """Check if a specific interest measure is present"""
        return measure_name.upper() in self.interest_measures

    def _get_measure_value(self, measure_name: str) -> float | None:
        """Get the threshold value for a specific interest measure"""
        measure = self.interest_measures.get(measure_name.upper())
        return measure.threshold if measure else None

    def _count_consequent_attributes(self) -> int:
        """Count the number of unique attributes in the consequent"""
        consequent_setting_id = self.task_setting.consequent_setting
        if not consequent_setting_id:
            return 0

        # Find the consequent DBA setting
        consequent = next((dba for dba in self.task_setting.dba_settings if dba.id == consequent_setting_id), None)
        if not consequent:
            return 0

        # Count unique field references from BBA settings
        unique_fields: set[str] = set()
        for ba_ref in consequent.ba_refs:
            bba = next((bba for bba in self.task_setting.bba_settings if bba.id == ba_ref), None)
            if bba:
                unique_fields.add(bba.field_ref)

        return len(unique_fields)

    def _validate_header_extensions(self) -> None:
        """Validate that required PMML header extensions are present and valid."""
        if not self.pmml.header or not self.pmml.header.extensions:
            raise MinerTaskValidationError("PMML header with extensions is required")

        ext_dict = {ext.name.lower(): ext.value for ext in self.pmml.header.extensions}

        # Validate Dataset extension
        if "dataset" not in ext_dict or not ext_dict["dataset"]:
            raise MinerTaskValidationError("Dataset extension not found in PMML header")

        try:
            _ = int(ext_dict["dataset"])
        except ValueError:
            raise MinerTaskValidationError(
                f"Dataset extension must contain a valid integer, got: {ext_dict['dataset']}"
            )

        # Validate database connection extensions
        required_db_extensions = ["database-server", "database-name", "database-user", "database-password"]
        missing = [name for name in required_db_extensions if name not in ext_dict or not ext_dict[name]]

        if missing:
            raise MinerTaskValidationError(f"Missing required database extensions in PMML header: {missing}")

        # Validate database-server format (must contain port)
        db_server = ext_dict["database-server"]
        # Handle both "mysql://host:port" and "host:port" formats
        server_str = db_server.split("://")[1] if "://" in db_server else db_server

        if ":" not in server_str:
            raise MinerTaskValidationError(
                f"database-server must contain port (format: host:port or mysql://host:port), got: {db_server}"
            )

        # Validate port is numeric
        try:
            port_str = server_str.rsplit(":", 1)[1]
            _ = int(port_str)
        except (ValueError, IndexError):
            raise MinerTaskValidationError(f"database-server must contain valid numeric port, got: {db_server}")

    def _validate_interest_measure_config(self) -> None:
        """Validate that interest measures have correct compare_type and threshold_type per R backend spec."""
        for im in self.task_setting.interest_measure_settings:
            measure_name = im.interest_measure.upper()
            compare_type = im.compare_type
            threshold_type = im.threshold_type

            # Validate per R backend spec
            if measure_name == "CONF":
                if compare_type != "Greater than or equal":
                    raise MinerTaskValidationError(
                        f"CONF must use CompareType='Greater than or equal', got: '{compare_type}'"
                    )
                if threshold_type != "% of all":
                    raise MinerTaskValidationError(f"CONF must use ThresholdType='% of all', got: '{threshold_type}'")

            elif measure_name == "SUPP":
                if compare_type != "Greater than or equal":
                    raise MinerTaskValidationError(
                        f"SUPP must use CompareType='Greater than or equal', got: '{compare_type}'"
                    )
                if threshold_type != "% of all":
                    raise MinerTaskValidationError(f"SUPP must use ThresholdType='% of all', got: '{threshold_type}'")

            elif measure_name == "LIFT":
                if compare_type != "Greater than or equal":
                    raise MinerTaskValidationError(
                        f"LIFT must use CompareType='Greater than or equal', got: '{compare_type}'"
                    )
                if threshold_type != "% of all":
                    raise MinerTaskValidationError(f"LIFT must use ThresholdType='% of all', got: '{threshold_type}'")

            elif measure_name == "RULE_LENGTH":
                if compare_type != "Less than or equal":
                    raise MinerTaskValidationError(
                        f"RULE_LENGTH must use CompareType='Less than or equal', got: '{compare_type}'"
                    )
                if threshold_type != "Abs":
                    raise MinerTaskValidationError(f"RULE_LENGTH must use ThresholdType='Abs', got: '{threshold_type}'")

            elif measure_name == "AUTO_CONF_SUPP":
                if compare_type != "Equal":
                    raise MinerTaskValidationError(
                        f"AUTO_CONF_SUPP must use CompareType='Equal', got: '{compare_type}'"
                    )
                if threshold_type != "Abs":
                    raise MinerTaskValidationError(
                        f"AUTO_CONF_SUPP must use ThresholdType='Abs', got: '{threshold_type}'"
                    )

    def validate(self) -> bool:
        """
        Validate the mining task according to Scala implementation rules.

        Raises:
            MinerTaskValidationError: If validation fails
        """
        self._validate_header_extensions()
        self._validate_interest_measure_config()

        # Check if AUTO_CONF_SUPP is enabled
        auto_conf_supp = self._has_measure("AUTO_CONF_SUPP")
        cba_enabled = self._has_measure("CBA")

        # Validate required measures based on AUTO_CONF_SUPP setting
        if not auto_conf_supp:
            # Standard validation - all measures required
            required_measures = {
                "CONF": "Confidence is required.",
                "SUPP": "Support is required.",
                "RULE_LENGTH": "Max rule length is required.",
            }

            for measure, error_msg in required_measures.items():
                if not self._has_measure(measure) and measure != "RULE_LENGTH":
                    # Allow FUI as alias for CONF
                    if measure == "CONF" and not self._has_measure("FUI"):
                        raise MinerTaskValidationError(error_msg)
                elif measure == "RULE_LENGTH":
                    if not self._has_measure(measure):
                        raise MinerTaskValidationError(error_msg)

        # Validate measure value ranges
        if self._has_measure("CONF"):
            conf_value = self._get_measure_value("CONF")
            if conf_value is not None and (conf_value > 1 or conf_value < 0.001):
                raise MinerTaskValidationError("Confidence must be greater than 0.001 and less than 1.")
        elif self._has_measure("FUI"):
            fui_value = self._get_measure_value("FUI")
            if fui_value is not None and (fui_value > 1 or fui_value < 0.001):
                raise MinerTaskValidationError("Confidence must be greater than 0.001 and less than 1.")

        if self._has_measure("SUPP"):
            supp_value = self._get_measure_value("SUPP")
            if supp_value is not None and (supp_value > 1 or supp_value < 0.001):
                raise MinerTaskValidationError("Support must be greater than 0.001 and less than 1.")

        if self._has_measure("RULE_LENGTH"):
            rule_length_value = self._get_measure_value("RULE_LENGTH")
            if rule_length_value is not None and rule_length_value <= 0:
                raise MinerTaskValidationError("Max rule length must be greater than 0.")

        # Note: MinRuleLength defaults to 1 in the Scala implementation
        # We don't require it explicitly, but if provided, validate it
        min_rule_length = 1  # Default value
        max_rule_length = self._get_measure_value("RULE_LENGTH")

        if max_rule_length is not None and max_rule_length < min_rule_length:
            raise MinerTaskValidationError("Max rule length must equal to or be greater than min rule length.")

        # Validate CBA and AUTO_CONF_SUPP constraints
        if cba_enabled or auto_conf_supp:
            consequent_attrs = self._count_consequent_attributes()

            if auto_conf_supp:
                if consequent_attrs != 1:
                    raise MinerTaskValidationError(
                        "You may use only one attribute as the consequent if the AUTO_CONF_SUPP parameter is turned on."
                    )
            elif cba_enabled:
                if consequent_attrs != 1:
                    raise MinerTaskValidationError(
                        "You may use only one attribute as the consequent if the CBA pruning is turned on."
                    )

        # Validate that consequent is present (required for limited datasets)
        if not self.task_setting.consequent_setting:
            # In Scala, this check is only for unlimited datasets, but we'll be conservative
            # and require consequent for all datasets
            pass  # We'll allow this for now, can be stricter later

        return True


def validate_mining_task(pmml: PMML) -> bool:
    """
    Convenience function to validate a mining task.

    Args:
        pmml: The PMML input to validate

    Raises:
        MinerTaskValidationError: If validation fails
    """
    validator = MinerTaskValidator(pmml)
    return validator.validate()
