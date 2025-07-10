# src/infrastructure/factories/validator_factory.py
"""
Factory for creating validator instances.
"""

import logging
from typing import Dict, Any

from ...domain.validators.filemaker.filemaker_data_validator import (
    FilemakerDataValidator, FilemakerDataValidationConfig
)
from ...domain.validators.filemaker.marketing_description_validator import (
    FilemakerMarketingDescriptionValidator, FilemakerMarketingDescriptionValidationConfig
)
from ...domain.validators.iseries.iseries_data_validator import (
    IseriesDataValidator, IseriesDataValidationConfig
)
from ...domain.validators.iseries.measurement_validator import (
    IseriesMeasurementValidator, IseriesMeasurementValidationConfig
)
from ...domain.validators.business.vehicle_application_validator import (
    VehicleApplicationBusinessValidator, VehicleApplicationValidationConfig
)
from ...domain.validators.upc_validator import UpcCodeValidator, UpcValidationConfig
from ...domain.validators.measurement_validator import MeasurementValidator, MeasurementValidationConfig

logger = logging.getLogger(__name__)


class ValidatorFactory:
    """Factory for creating validator instances."""

    @staticmethod
    def create_filemaker_data_validator(config: Dict[str, Any]) -> FilemakerDataValidator:
        """Create Filemaker data validator."""
        fm_config = FilemakerDataValidationConfig(**config)
        return FilemakerDataValidator(fm_config)

    @staticmethod
    def create_filemaker_marketing_description_validator(
            config: Dict[str, Any]) -> FilemakerMarketingDescriptionValidator:
        """Create Filemaker marketing description validator."""
        fm_marketing_config = FilemakerMarketingDescriptionValidationConfig(**config)
        return FilemakerMarketingDescriptionValidator(fm_marketing_config)

    @staticmethod
    def create_iseries_data_validator(config: Dict[str, Any]) -> IseriesDataValidator:
        """Create Iseries data validator."""
        iseries_config = IseriesDataValidationConfig(**config)
        return IseriesDataValidator(iseries_config)

    @staticmethod
    def create_iseries_measurement_validator(config: Dict[str, Any]) -> IseriesMeasurementValidator:
        """Create Iseries measurement validator."""
        measurement_config = IseriesMeasurementValidationConfig(**config)
        return IseriesMeasurementValidator(measurement_config)

    @staticmethod
    def create_vehicle_application_validator(config: Dict[str, Any]) -> VehicleApplicationBusinessValidator:
        """Create vehicle application business validator."""
        app_config = VehicleApplicationValidationConfig(**config)
        return VehicleApplicationBusinessValidator(app_config)

    @staticmethod
    def create_upc_validator(config: Dict[str, Any]) -> UpcCodeValidator:
        """Create UPC code validator."""
        upc_config = UpcValidationConfig(**config)
        return UpcCodeValidator(upc_config)

    @staticmethod
    def create_measurement_validator(config: Dict[str, Any]) -> MeasurementValidator:
        """Create measurement validator."""
        measurement_config = MeasurementValidationConfig(**config)
        return MeasurementValidator(measurement_config)