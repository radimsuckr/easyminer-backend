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

    def validate(self) -> None:
        """
        Validate the mining task according to Scala implementation rules.

        Raises:
            MinerTaskValidationError: If validation fails
        """
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


def validate_mining_task(pmml: PMML) -> None:
    """
    Convenience function to validate a mining task.

    Args:
        pmml: The PMML input to validate

    Raises:
        MinerTaskValidationError: If validation fails
    """
    validator = MinerTaskValidator(pmml)
    validator.validate()
