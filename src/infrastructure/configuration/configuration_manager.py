# src/infrastructure/configuration/configuration_manager.py
"""
Enhanced configuration manager with environment variable resolution and validation.
"""

import os
import re
import yaml
import json
import logging
from pathlib import Path
from typing import Dict, Any, Union, Optional, List
from dataclasses import dataclass, field
from dotenv import load_dotenv

from ...domain.interfaces import ConfigurationProvider

logger = logging.getLogger(__name__)


@dataclass
class ConfigurationValidationRule:
    """Configuration validation rule."""
    key_path: str
    required: bool = False
    data_type: type = str
    allowed_values: Optional[List[Any]] = None
    min_value: Optional[Union[int, float]] = None
    max_value: Optional[Union[int, float]] = None
    validation_function: Optional[callable] = None


class EnhancedConfigurationManager(ConfigurationProvider):
    """Enhanced configuration manager with validation and environment resolution."""

    def __init__(self, config_file_path: Union[str, Path] = "config.yaml"):
        self.config_file_path = Path(config_file_path)
        self._config_data: Dict[str, Any] = {}
        self._validation_rules: List[ConfigurationValidationRule] = []
        self._environment_loaded = False

        # Load environment variables
        self._load_environment()

        # Load configuration
        self.reload_configuration()

        # Initialize default validation rules
        self._initialize_default_validation_rules()

    def _load_environment(self) -> None:
        """Load environment variables from .env file."""
        if not self._environment_loaded:
            load_dotenv()
            self._environment_loaded = True
            logger.debug("Environment variables loaded")

    def reload_configuration(self) -> None:
        """Reload configuration from file."""
        try:
            if not self.config_file_path.exists():
                logger.warning(f"Configuration file not found: {self.config_file_path}")
                self._config_data = self._get_default_configuration()
                return

            # Read and process configuration file
            with open(self.config_file_path, 'r', encoding='utf-8') as f:
                raw_content = f.read()

            # Resolve environment variables
            resolved_content = self._resolve_environment_variables(raw_content)

            # Parse configuration
            if self.config_file_path.suffix.lower() == '.yaml':
                self._config_data = yaml.safe_load(resolved_content)
            elif self.config_file_path.suffix.lower() == '.json':
                self._config_data = json.loads(resolved_content)
            else:
                raise ValueError(f"Unsupported configuration file format: {self.config_file_path.suffix}")

            logger.info(f"Configuration loaded from: {self.config_file_path}")

        except Exception as e:
            logger.error(f"Failed to load configuration: {e}")
            self._config_data = self._get_default_configuration()

    def get_value(self, key_path: str, default: Any = None) -> Any:
        """Get configuration value using dot notation."""
        return self._get_nested_value(self._config_data, key_path, default)

    def get_section(self, section_name: str) -> Dict[str, Any]:
        """Get entire configuration section."""
        return self.get_value(section_name, {})

    def has_key(self, key_path: str) -> bool:
        """Check if configuration key exists."""
        try:
            self._get_nested_value(self._config_data, key_path)
            return True
        except (KeyError, TypeError):
            return False

    def set_value(self, key_path: str, value: Any) -> None:
        """Set configuration value using dot notation."""
        self._set_nested_value(self._config_data, key_path, value)

    def add_validation_rule(self, rule: ConfigurationValidationRule) -> None:
        """Add configuration validation rule."""
        self._validation_rules.append(rule)

    def validate_configuration(self) -> List[str]:
        """Validate configuration against defined rules."""
        errors = []

        for rule in self._validation_rules:
            try:
                value = self.get_value(rule.key_path)

                # Check if required
                if rule.required and value is None:
                    errors.append(f"Required configuration missing: {rule.key_path}")
                    continue

                if value is None:
                    continue  # Skip validation for optional missing values

                # Type validation
                if rule.data_type and not isinstance(value, rule.data_type):
                    try:
                        # Try to convert
                        converted_value = rule.data_type(value)
                        self.set_value(rule.key_path, converted_value)
                        value = converted_value
                    except (ValueError, TypeError):
                        errors.append(
                            f"Invalid type for {rule.key_path}: expected {rule.data_type.__name__}, got {type(value).__name__}")
                        continue

                # Allowed values validation
                if rule.allowed_values and value not in rule.allowed_values:
                    errors.append(f"Invalid value for {rule.key_path}: {value}. Allowed: {rule.allowed_values}")

                # Range validation
                if rule.min_value is not None and value < rule.min_value:
                    errors.append(f"Value too low for {rule.key_path}: {value} < {rule.min_value}")

                if rule.max_value is not None and value > rule.max_value:
                    errors.append(f"Value too high for {rule.key_path}: {value} > {rule.max_value}")

                # Custom validation function
                if rule.validation_function:
                    custom_error = rule.validation_function(value)
                    if custom_error:
                        errors.append(f"Custom validation failed for {rule.key_path}: {custom_error}")

            except Exception as e:
                errors.append(f"Validation error for {rule.key_path}: {e}")

        return errors

    def _resolve_environment_variables(self, content: str) -> str:
        """Resolve environment variables in configuration content."""
        # Pattern to match ${VAR_NAME} or ${VAR_NAME:default_value}
        pattern = r'\$\{([^}:]+)(?::([^}]*))?\}'

        def replace_env_var(match):
            var_name = match.group(1)
            default_value = match.group(2) if match.group(2) is not None else ''

            env_value = os.getenv(var_name)
            if env_value is not None:
                return env_value
            elif default_value:
                return default_value
            else:
                logger.warning(f"Environment variable {var_name} not found and no default provided")
                return match.group(0)  # Return original if not found

        return re.sub(pattern, replace_env_var, content)

    def _get_nested_value(self, data: Dict[str, Any], key_path: str, default: Any = None) -> Any:
        """Get nested dictionary value using dot notation."""
        keys = key_path.split('.')
        current = data

        try:
            for key in keys:
                current = current[key]
            return current
        except (KeyError, TypeError):
            if default is not None:
                return default
            raise KeyError(f"Configuration key not found: {key_path}")

    def _set_nested_value(self, data: Dict[str, Any], key_path: str, value: Any) -> None:
        """Set nested dictionary value using dot notation."""
        keys = key_path.split('.')
        current = data

        for key in keys[:-1]:
            if key not in current:
                current[key] = {}
            current = current[key]

        current[keys[-1]] = value

    def _initialize_default_validation_rules(self) -> None:
        """Initialize default validation rules."""
        rules = [
            # Database configuration validation
            ConfigurationValidationRule(
                key_path="database.filemaker.server",
                required=True,
                data_type=str
            ),
            ConfigurationValidationRule(
                key_path="database.filemaker.port",
                required=True,
                data_type=int,
                min_value=1,
                max_value=65535
            ),
            ConfigurationValidationRule(
                key_path="database.iseries.server",
                required=True,
                data_type=str
            ),

            # Validation configuration
            ConfigurationValidationRule(
                key_path="validation.vehicle_start_year",
                required=False,
                data_type=int,
                min_value=1900,
                max_value=2100
            ),
            ConfigurationValidationRule(
                key_path="validation.vehicle_end_year",
                required=False,
                data_type=int,
                min_value=1900,
                max_value=2100
            ),

            # Processing configuration
            ConfigurationValidationRule(
                key_path="processing.batch_size",
                required=False,
                data_type=int,
                min_value=1,
                max_value=10000
            ),
            ConfigurationValidationRule(
                key_path="processing.max_workers",
                required=False,
                data_type=int,
                min_value=1,
                max_value=20
            ),

            # File paths validation
            ConfigurationValidationRule(
                key_path="files.lookup_file",
                required=True,
                data_type=str,
                validation_function=lambda x: None if Path(x).exists() else f"File not found: {x}"
            )
        ]

        self._validation_rules.extend(rules)

    def _get_default_configuration(self) -> Dict[str, Any]:
        """Get default configuration when file is not available."""
        return {
            "database": {
                "filemaker": {
                    "server": "localhost",
                    "port": 2399,
                    "user": "admin",
                    "password": "password",
                    "database": "CrownMaster",
                    "fmjdbc_jar_path": "libs/fmjdbc.jar"
                },
                "iseries": {
                    "server": "localhost",
                    "user": "admin",
                    "password": "password",
                    "database": "DSTDATA",
                    "jt400_jar_path": "libs/jt400.jar"
                }
            },
            "validation": {
                "vehicle_start_year": 1900,
                "vehicle_end_year": 2030
            },
            "files": {
                "lookup_file": "data/application_replacements.json",
                "output_file": "output/application_data.xlsx"
            },
            "processing": {
                "batch_size": 1000,
                "max_workers": 4
            }
        }

    def save_configuration(self, output_path: Optional[Union[str, Path]] = None) -> None:
        """Save current configuration to file."""
        save_path = Path(output_path) if output_path else self.config_file_path

        try:
            save_path.parent.mkdir(parents=True, exist_ok=True)

            if save_path.suffix.lower() == '.yaml':
                with open(save_path, 'w', encoding='utf-8') as f:
                    yaml.dump(self._config_data, f, default_flow_style=False, indent=2, sort_keys=False)
            elif save_path.suffix.lower() == '.json':
                with open(save_path, 'w', encoding='utf-8') as f:
                    json.dump(self._config_data, f, indent=2)
            else:
                raise ValueError(f"Unsupported file format: {save_path.suffix}")

            logger.info(f"Configuration saved to: {save_path}")

        except Exception as e:
            logger.error(f"Failed to save configuration: {e}")
            raise