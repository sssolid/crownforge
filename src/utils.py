"""
Utilities Module - Common functions and helper classes
"""
import os
import re
import string
import yaml
import json
import logging
from pathlib import Path
from typing import Dict, List, Any, Optional, Union, Tuple
from datetime import datetime
from dataclasses import dataclass
import time
from contextlib import contextmanager
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

load_dotenv()

# ==============================================================================
# Configuration Utilities
# ==============================================================================

class ConfigManager:
    """Configuration management utility"""

    def __init__(self, config_path: Union[str, Path] = "config.yaml"):
        self.config_path = Path(config_path)
        self._config = None
        self.load_config()

    def env_resolver(self, val: str) -> str:
        # Replace ${VAR} with os.getenv("VAR")
        return re.sub(r"\$\{(\w+)\}", lambda m: os.getenv(m.group(1), m.group(0)), val)

    def load_config(self) -> Dict[str, Any]:
        """Load configuration from YAML with environment-sourced secrets."""
        try:
            if self.config_path.suffix.lower() == '.yaml':
                with open(self.config_path, 'r') as f:
                    raw = f.read()
                substituted = self.env_resolver(raw)
                self._config = yaml.safe_load(substituted)
            elif self.config_path.suffix.lower() == '.json':
                with open(self.config_path, 'r') as f:
                    with open(self.config_path, 'r') as f:
                        raw = f.read()
                    substituted = self.env_resolver(raw)
                    self._config = json.loads(substituted)
            else:
                raise ValueError(f"Unsupported config file format: {self.config_path.suffix}")

            logger.info(f"Configuration loaded from {self.config_path}")
            return self._config

        except Exception as e:
            logger.error(f"Failed to load configuration: {e}")
            self._config = self._get_default_config()
            return self._config

    def get(self, key_path: str, default: Any = None) -> Any:
        """Get configuration value using dot notation (e.g., 'database.username')"""
        if not self._config:
            return default

        keys = key_path.split('.')
        value = self._config

        try:
            for key in keys:
                value = value[key]
            return value
        except (KeyError, TypeError):
            return default

    def set(self, key_path: str, value: Any):
        """Set configuration value using dot notation"""
        if not self._config:
            self._config = {}

        keys = key_path.split('.')
        config_ref = self._config

        for key in keys[:-1]:
            if key not in config_ref:
                config_ref[key] = {}
            config_ref = config_ref[key]

        config_ref[keys[-1]] = value

    def save_config(self, output_path: Optional[Union[str, Path]] = None):
        """Save current configuration to file"""
        save_path = Path(output_path) if output_path else self.config_path

        try:
            if save_path.suffix.lower() == '.yaml':
                with open(save_path, 'w') as f:
                    yaml.dump(self._config, f, default_flow_style=False, indent=2)
            elif save_path.suffix.lower() == '.json':
                with open(save_path, 'w') as f:
                    json.dump(self._config, f, indent=2)

            logger.info(f"Configuration saved to {save_path}")

        except Exception as e:
            logger.error(f"Failed to save configuration: {e}")

    def _get_default_config(self) -> Dict[str, Any]:
        """Get default configuration"""
        return {
            "database": {
                "dsn": "CrownMaster",
                "username": "Ryan",
                "password": "crown1234"
            },
            "validation": {
                "vehicle_start_year": 1900,
                "vehicle_end_year": datetime.now().year + 1
            },
            "files": {
                "lookup_file": "applications/application_replacements.json",
                "output_file": "application_data.xlsx"
            }
        }

# ==============================================================================
# Java/JDBC Utilities
# ==============================================================================

class JavaStringConverter:
    """Utilities for handling Java String objects from JDBC connections"""

    @staticmethod
    def convert_to_python_string(value) -> Optional[str]:
        """Convert Java String objects to Python strings"""
        if value is None:
            return None

        # Check if it's a Java String object
        if hasattr(value, 'toString'):
            return str(value.toString())

        # Check if it's a Java object with string representation
        if hasattr(value, 'getClass') and 'String' in str(value.getClass()):
            return str(value)

        # If it's already a Python string or other type, convert to string
        return str(value) if value is not None else None

    @staticmethod
    def safe_string_operation(value, operation: str, *args, **kwargs):
        """Safely perform string operations on potentially Java String objects"""
        if value is None:
            return None

        # Convert to Python string first
        python_str = JavaStringConverter.convert_to_python_string(value)

        if python_str is None:
            return None

        # Perform the operation
        try:
            if hasattr(python_str, operation):
                method = getattr(python_str, operation)
                return method(*args, **kwargs)
            else:
                raise AttributeError(f"String has no attribute '{operation}'")
        except Exception as e:
            logger.warning(f"Error performing {operation} on string '{python_str}': {e}")
            return python_str

    @staticmethod
    def batch_convert_record(record_dict: Dict[str, Any]) -> Dict[str, Any]:
        """Convert all string values in a record from Java to Python strings"""
        converted = {}
        for key, value in record_dict.items():
            if isinstance(value, str) or hasattr(value, 'toString'):
                converted[key] = JavaStringConverter.convert_to_python_string(value)
            else:
                converted[key] = value
        return converted

# ==============================================================================
# String and Text Utilities
# ==============================================================================

class TextProcessor:
    """Text processing utilities with Java String support"""

    @staticmethod
    def clean_string(value: Any) -> str:
        """Remove non-printable characters from a string (handles Java strings)"""
        if value is None:
            return ""

        # Convert Java strings to Python strings
        python_str = JavaStringConverter.convert_to_python_string(value)
        if python_str is None:
            return ""

        return ''.join(char for char in python_str if char in string.printable)

    @staticmethod
    def safe_lower(value: Any) -> str:
        """Safely convert to lowercase (handles Java strings)"""
        python_str = JavaStringConverter.convert_to_python_string(value)
        return python_str.lower() if python_str else ""

    @staticmethod
    def safe_strip(value: Any) -> str:
        """Safely strip whitespace (handles Java strings)"""
        python_str = JavaStringConverter.convert_to_python_string(value)
        return python_str.strip() if python_str else ""

    @staticmethod
    def safe_rstrip(value: Any, chars: str = None) -> str:
        """Safely strip from right (handles Java strings)"""
        python_str = JavaStringConverter.convert_to_python_string(value)
        return python_str.rstrip(chars) if python_str else ""

    @staticmethod
    def safe_startswith(value: Any, prefix: str) -> bool:
        """Safely check if string starts with prefix (handles Java strings)"""
        python_str = JavaStringConverter.convert_to_python_string(value)
        return python_str.startswith(prefix) if python_str else False

    @staticmethod
    def safe_endswith(value: Any, suffix: str) -> bool:
        """Safely check if string ends with suffix (handles Java strings)"""
        python_str = JavaStringConverter.convert_to_python_string(value)
        return python_str.endswith(suffix) if python_str else False

    @staticmethod
    def safe_replace(value: Any, old: str, new: str) -> str:
        """Safely replace substring (handles Java strings)"""
        python_str = JavaStringConverter.convert_to_python_string(value)
        return python_str.replace(old, new) if python_str else ""

    @staticmethod
    def find_illegal_characters(text: Any) -> List[Tuple[str, int]]:
        """Find illegal characters in text and their positions (handles Java strings)"""
        python_str = JavaStringConverter.convert_to_python_string(text)
        if not python_str:
            return []

        return [(char, idx) for idx, char in enumerate(python_str)
                if char not in string.printable]

    @staticmethod
    def highlight_illegal_characters(text: Any, illegal_chars: List[Tuple[str, int]]) -> str:
        """Highlight illegal characters in text (handles Java strings)"""
        python_str = JavaStringConverter.convert_to_python_string(text)
        if not python_str or not illegal_chars:
            return python_str or ""

        highlighted = list(python_str)
        for char, idx in illegal_chars:
            if idx < len(highlighted):
                highlighted[idx] = f"[{char}]"

        return ''.join(highlighted)

    @staticmethod
    def normalize_whitespace(text: Any) -> str:
        """Normalize whitespace in text (handles Java strings)"""
        python_str = JavaStringConverter.convert_to_python_string(text)
        if not python_str:
            return ""

        return re.sub(r'\s+', ' ', python_str).strip()

    @staticmethod
    def extract_year_range(text: Any) -> Optional[Tuple[int, int]]:
        """Extract year range from text (YYYY-YYYY format) (handles Java strings)"""
        python_str = JavaStringConverter.convert_to_python_string(text)
        if not python_str:
            return None

        pattern = r'^(\d{4})-(\d{4})'
        match = re.match(pattern, python_str.strip())

        if match:
            start_year, end_year = map(int, match.groups())
            return start_year, end_year

        return None

    @staticmethod
    def format_description(desc: Any, width: int = 30) -> str:
        """Format description to fixed width (handles Java strings)"""
        python_str = JavaStringConverter.convert_to_python_string(desc)
        return python_str.ljust(width) if python_str else "".ljust(width)

# ==============================================================================
# Date and Time Utilities
# ==============================================================================

class DateProcessor:
    """Date processing utilities"""

    DATE_PATTERNS = [
        r"\d{1,2}[-/]\d{1,2}[-/]\d{2,4}",
        r"\d{4}[-/]\d{1,2}[-/]\d{1,2}",
        r"\d{1,2}[-/]\d{1,2}[-/]\d{2}",
    ]

    DATE_FORMATS = [
        "%m-%d-%Y", "%m/%d/%Y", "%m-%d-%y", "%m/%d/%y",
        "%Y-%m-%d", "%Y/%m/%d", "%m-%d", "%m/%d"
    ]

    @classmethod
    def find_dates_in_text(cls, text: str) -> List[str]:
        """Find all potential date strings in text"""
        dates = []
        for pattern in cls.DATE_PATTERNS:
            dates.extend(re.findall(pattern, text))
        return dates

    @classmethod
    def validate_and_format_date(cls, date_str: str) -> Optional[str]:
        """Validate and format date string to standard format"""
        for fmt in cls.DATE_FORMATS:
            try:
                parsed_date = datetime.strptime(date_str, fmt)
                return parsed_date.strftime("%m/%d/%Y")
            except ValueError:
                continue
        return None

    @classmethod
    def correct_dates_in_text(cls, text: str, part_number: str) -> Tuple[str, List[Dict]]:
        """Correct date formats in text and return corrections log"""
        corrections = []
        corrected_text = text

        dates_found = cls.find_dates_in_text(text)

        for date_str in dates_found:
            corrected_date = cls.validate_and_format_date(date_str)

            if corrected_date and corrected_date != date_str:
                corrections.append({
                    "PartNumber": part_number,
                    "OriginalDate": date_str,
                    "CorrectedDate": corrected_date,
                    "Text": text
                })
                corrected_text = corrected_text.replace(date_str, corrected_date)
            elif not corrected_date:
                corrections.append({
                    "PartNumber": part_number,
                    "OriginalDate": date_str,
                    "CorrectedDate": "Invalid",
                    "Text": text
                })

        return corrected_text, corrections

# ==============================================================================
# Performance and Monitoring Utilities
# ==============================================================================

@dataclass
class PerformanceMetrics:
    """Performance metrics tracking"""
    start_time: float
    end_time: Optional[float] = None
    items_processed: int = 0
    errors_count: int = 0
    warnings_count: int = 0

    @property
    def duration(self) -> float:
        """Get duration in seconds"""
        if self.end_time:
            return self.end_time - self.start_time
        return time.time() - self.start_time

    @property
    def items_per_second(self) -> float:
        """Get processing rate"""
        duration = self.duration
        return self.items_processed / duration if duration > 0 else 0

    def finish(self):
        """Mark processing as finished"""
        self.end_time = time.time()

class PerformanceMonitor:
    """Performance monitoring utility"""

    def __init__(self, name: str = "Operation"):
        self.name = name
        self.metrics = PerformanceMetrics(start_time=time.time())

    def increment_processed(self, count: int = 1):
        """Increment processed items count"""
        self.metrics.items_processed += count

    def increment_errors(self, count: int = 1):
        """Increment error count"""
        self.metrics.errors_count += count

    def increment_warnings(self, count: int = 1):
        """Increment warning count"""
        self.metrics.warnings_count += count

    def get_status_report(self) -> str:
        """Get current status report"""
        duration = self.metrics.duration
        rate = self.metrics.items_per_second

        return (f"{self.name}: {self.metrics.items_processed:,} items "
                f"in {duration:.1f}s ({rate:.1f} items/sec) "
                f"- Errors: {self.metrics.errors_count}, "
                f"Warnings: {self.metrics.warnings_count}")

    def finish_and_report(self) -> str:
        """Finish monitoring and return final report"""
        self.metrics.finish()
        return self.get_status_report()

@contextmanager
def performance_monitor(name: str = "Operation"):
    """Context manager for performance monitoring"""
    monitor = PerformanceMonitor(name)
    try:
        yield monitor
    finally:
        logger.info(monitor.finish_and_report())

# ==============================================================================
# Data Structure Utilities
# ==============================================================================

class DataStructureUtils:
    """Utilities for working with data structures"""

    @staticmethod
    def safe_get(data: Dict, key_path: str, default: Any = None) -> Any:
        """Safely get nested dictionary value"""
        keys = key_path.split('.')
        value = data

        try:
            for key in keys:
                value = value[key]
            return value
        except (KeyError, TypeError, AttributeError):
            return default

    @staticmethod
    def flatten_dict(data: Dict, separator: str = '.', prefix: str = '') -> Dict:
        """Flatten nested dictionary"""
        flattened = {}

        for key, value in data.items():
            new_key = f"{prefix}{separator}{key}" if prefix else key

            if isinstance(value, dict):
                flattened.update(
                    DataStructureUtils.flatten_dict(value, separator, new_key)
                )
            else:
                flattened[new_key] = value

        return flattened

    @staticmethod
    def group_by_key(data: List[Dict], key: str) -> Dict[Any, List[Dict]]:
        """Group list of dictionaries by key value"""
        grouped = {}

        for item in data:
            key_value = item.get(key)
            if key_value not in grouped:
                grouped[key_value] = []
            grouped[key_value].append(item)

        return grouped

    @staticmethod
    def merge_dicts(*dicts: Dict) -> Dict:
        """Merge multiple dictionaries"""
        result = {}
        for d in dicts:
            if d:
                result.update(d)
        return result

# ==============================================================================
# File and Path Utilities
# ==============================================================================

class FileUtils:
    """File and path utilities"""

    @staticmethod
    def ensure_directory(path: Union[str, Path]) -> Path:
        """Ensure directory exists, create if not"""
        dir_path = Path(path)
        dir_path.mkdir(parents=True, exist_ok=True)
        return dir_path

    @staticmethod
    def backup_file(file_path: Union[str, Path], backup_dir: str = "backups") -> Optional[Path]:
        """Create backup of file with timestamp"""
        source_path = Path(file_path)

        if not source_path.exists():
            return None

        backup_path = FileUtils.ensure_directory(backup_dir)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_filename = f"{source_path.stem}_{timestamp}{source_path.suffix}"
        backup_file_path = backup_path / backup_filename

        try:
            import shutil
            shutil.copy2(source_path, backup_file_path)
            logger.info(f"Backup created: {backup_file_path}")
            return backup_file_path
        except Exception as e:
            logger.error(f"Failed to create backup: {e}")
            return None

    @staticmethod
    def get_file_size_mb(file_path: Union[str, Path]) -> float:
        """Get file size in megabytes"""
        path = Path(file_path)
        if path.exists():
            return path.stat().st_size / (1024 * 1024)
        return 0.0

    @staticmethod
    def find_files(directory: Union[str, Path], pattern: str = "*") -> List[Path]:
        """Find files matching pattern in directory"""
        dir_path = Path(directory)
        if dir_path.exists() and dir_path.is_dir():
            return list(dir_path.glob(pattern))
        return []

# ==============================================================================
# Validation Utilities
# ==============================================================================

class ValidationUtils:
    """Data validation utilities"""

    @staticmethod
    def is_valid_year(year: Any, min_year: int = 1900, max_year: Optional[int] = None) -> bool:
        """Check if year is valid"""
        if max_year is None:
            max_year = datetime.now().year + 2

        try:
            year_int = int(year)
            return min_year <= year_int <= max_year
        except (ValueError, TypeError):
            return False

    @staticmethod
    def is_valid_year_range(start_year: Any, end_year: Any, max_range: int = 50) -> bool:
        """Check if year range is valid"""
        try:
            start = int(start_year)
            end = int(end_year)

            if start > end:
                return False

            if (end - start) > max_range:
                return False

            return (ValidationUtils.is_valid_year(start) and
                   ValidationUtils.is_valid_year(end))
        except (ValueError, TypeError):
            return False

    @staticmethod
    def validate_part_number(part_number: str) -> bool:
        """Validate part number format"""
        if not isinstance(part_number, str):
            return False

        # Basic validation - not empty and reasonable length
        return len(part_number.strip()) > 0 and len(part_number) <= 50

    @staticmethod
    def validate_required_fields(data: Dict, required_fields: List[str]) -> List[str]:
        """Validate that required fields are present and not empty"""
        missing_fields = []

        for field in required_fields:
            value = data.get(field)
            if value is None or (isinstance(value, str) and not value.strip()):
                missing_fields.append(field)

        return missing_fields

# ==============================================================================
# Export Functions
# ==============================================================================

# Make commonly used utilities easily accessible
__all__ = [
    'ConfigManager',
    'JavaStringConverter',
    'TextProcessor',
    'DateProcessor',
    'PerformanceMonitor',
    'performance_monitor',
    'DataStructureUtils',
    'FileUtils',
    'ValidationUtils',
    'PerformanceMetrics'
]