"""
Measurement Validation Module
Validates measurements and checks for discrepancies between systems
"""

import logging
from typing import List, Dict, Any, Tuple, Optional
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class MeasurementValidationConfig:
    """Measurement validation configuration"""
    enabled: bool = True
    max_length_inches: float = 120.0
    max_width_inches: float = 120.0
    max_height_inches: float = 120.0
    max_weight_pounds: float = 500.0
    tolerance_percentage: float = 5.0


@dataclass
class MeasurementDiscrepancy:
    """Represents a measurement discrepancy between systems"""
    part_number: str
    field: str
    filemaker_value: Optional[float]
    iseries_value: Optional[float]
    difference: Optional[float]
    percentage_diff: Optional[float]
    is_significant: bool


class MeasurementValidator:
    """Measurement validation service"""

    def __init__(self, config: Dict[str, Any]):
        self.config = MeasurementValidationConfig(**config)

    def validate_measurement_range(self, value: Any, field_name: str) -> Tuple[bool, str]:
        """
        Validate that a measurement is within acceptable range

        Args:
            value: Measurement value to validate
            field_name: Name of the field being validated

        Returns:
            Tuple of (is_valid, error_message)
        """
        if not self.config.enabled:
            return True, ""

        if value is None or value == "":
            return True, ""  # Allow empty values

        try:
            numeric_value = float(value)
        except (ValueError, TypeError):
            return False, f"Invalid numeric value for {field_name}: {value}"

        if numeric_value < 0:
            return False, f"Negative value not allowed for {field_name}: {numeric_value}"

        # Check field-specific ranges
        if field_name.lower().startswith('length') and numeric_value > self.config.max_length_inches:
            return False, f"Length exceeds maximum ({self.config.max_length_inches} inches): {numeric_value}"

        if field_name.lower().startswith('width') and numeric_value > self.config.max_width_inches:
            return False, f"Width exceeds maximum ({self.config.max_width_inches} inches): {numeric_value}"

        if field_name.lower().startswith('height') and numeric_value > self.config.max_height_inches:
            return False, f"Height exceeds maximum ({self.config.max_height_inches} inches): {numeric_value}"

        if field_name.lower().startswith('weight') and numeric_value > self.config.max_weight_pounds:
            return False, f"Weight exceeds maximum ({self.config.max_weight_pounds} pounds): {numeric_value}"

        return True, ""

    def compare_measurements(self, filemaker_data: List[Dict[str, Any]],
                             iseries_data: List[Dict[str, Any]]) -> List[MeasurementDiscrepancy]:
        """
        Compare measurements between Filemaker and Iseries systems

        Args:
            filemaker_data: Measurement data from Filemaker
            iseries_data: Measurement data from Iseries

        Returns:
            List of measurement discrepancies
        """
        if not self.config.enabled:
            return []

        logger.info("Creating filemaker lookup dictionary")
        fm_lookup = {record['PartNumber']: record for record in filemaker_data}
        logger.info("Creating iseries lookup dictionary")
        is_lookup = {record['PartNumber']: record for record in iseries_data}

        # Get all part numbers from both systems
        all_part_numbers = set(fm_lookup.keys()) | set(is_lookup.keys())

        discrepancies = []

        for part_number in all_part_numbers:
            fm_record = fm_lookup.get(part_number, {})
            is_record = is_lookup.get(part_number, {})

            # Define field mappings between systems
            field_mappings = [
                ('Length_FM', 'Length_AS400', 'Length'),
                ('Width_FM', 'Width_AS400', 'Width'),
                ('Height_FM', 'Height_AS400', 'Height'),
                ('Weight_FM', 'Weight_AS400', 'Weight')
            ]

            for fm_field, is_field, display_name in field_mappings:
                fm_value = fm_record.get(fm_field)
                is_value = is_record.get(is_field)

                # Skip if both values are missing
                if fm_value is None and is_value is None:
                    continue

                discrepancy = self._calculate_discrepancy(
                    part_number, display_name, fm_value, is_value
                )

                if discrepancy:
                    discrepancies.append(discrepancy)

        return discrepancies

    def _calculate_discrepancy(self, part_number: str, field: str,
                               fm_value: Any, is_value: Any) -> Optional[MeasurementDiscrepancy]:
        """Calculate discrepancy between two measurement values"""

        # Convert to float, handling None and empty values
        try:
            fm_float = float(fm_value) if fm_value is not None and fm_value != "" else None
        except (ValueError, TypeError):
            fm_float = None

        try:
            is_float = float(is_value) if is_value is not None and is_value != "" else None
        except (ValueError, TypeError):
            is_float = None

        # Calculate difference and percentage
        difference = None
        percentage_diff = None
        is_significant = False

        if fm_float is not None and is_float is not None:
            difference = abs(fm_float - is_float)

            # Calculate percentage difference (using average as base)
            average = (fm_float + is_float) / 2
            if average > 0:
                percentage_diff = (difference / average) * 100
                is_significant = percentage_diff > self.config.tolerance_percentage

        elif fm_float is not None or is_float is not None:
            # One system has value, other doesn't
            is_significant = True

        # Only return discrepancy if it's significant or if there's a missing value
        if is_significant or (fm_float is None) != (is_float is None):
            return MeasurementDiscrepancy(
                part_number=part_number,
                field=field,
                filemaker_value=fm_float,
                iseries_value=is_float,
                difference=difference,
                percentage_diff=percentage_diff,
                is_significant=is_significant
            )

        return None

    def validate_dataset(self, measurement_data: List[Dict[str, Any]],
                         system_name: str = "Unknown") -> Dict[str, Any]:
        """
        Validate entire measurement dataset

        Args:
            measurement_data: List of measurement records
            system_name: Name of the system being validated

        Returns:
            Dictionary with validation results
        """
        results = {
            'system': system_name,
            'total_records': len(measurement_data),
            'valid_records': 0,
            'invalid_records': 0,
            'validation_errors': [],
            'field_statistics': {}
        }

        if not self.config.enabled:
            results['message'] = "Measurement validation is disabled"
            return results

        logger.info(f"Validating {len(measurement_data)} measurement records from {system_name}")

        # Track field statistics
        field_counts = {}
        field_errors = {}

        for record in measurement_data:
            part_number = record.get('PartNumber', 'Unknown')
            record_valid = True

            # Check each measurement field
            measurement_fields = [
                key for key in record.keys()
                if any(measure in key.lower() for measure in ['length', 'width', 'height', 'weight'])
            ]

            for field in measurement_fields:
                value = record.get(field)

                # Track field presence
                if field not in field_counts:
                    field_counts[field] = {'total': 0, 'populated': 0, 'valid': 0}

                field_counts[field]['total'] += 1

                if value is not None and value != "":
                    field_counts[field]['populated'] += 1

                    is_valid, error_msg = self.validate_measurement_range(value, field)

                    if is_valid:
                        field_counts[field]['valid'] += 1
                    else:
                        record_valid = False
                        if field not in field_errors:
                            field_errors[field] = []
                        field_errors[field].append({
                            'PartNumber': part_number,
                            'Field': field,
                            'Value': value,
                            'Error': error_msg
                        })

            if record_valid:
                results['valid_records'] += 1
            else:
                results['invalid_records'] += 1

        # Compile field statistics
        for field, counts in field_counts.items():
            results['field_statistics'][field] = {
                'total_records': counts['total'],
                'populated_count': counts['populated'],
                'valid_count': counts['valid'],
                'population_rate': (counts['populated'] / max(1, counts['total'])) * 100,
                'validation_rate': (counts['valid'] / max(1, counts['populated'])) * 100,
                'errors': field_errors.get(field, [])
            }

        # Flatten all errors
        all_errors = []
        for field_error_list in field_errors.values():
            all_errors.extend(field_error_list)

        results['validation_errors'] = all_errors

        logger.info(f"Measurement validation completed for {system_name}: "
                    f"{results['valid_records']} valid, {results['invalid_records']} invalid")

        return results

    def generate_discrepancy_report(self, discrepancies: List[MeasurementDiscrepancy]) -> Dict[str, Any]:
        """Generate a summary report of measurement discrepancies"""

        report = {
            'total_discrepancies': len(discrepancies),
            'significant_discrepancies': len([d for d in discrepancies if d.is_significant]),
            'by_field': {},
            'top_discrepancies': [],
            'missing_data': {'filemaker_missing': [], 'iseries_missing': []}
        }

        # Group by field
        for discrepancy in discrepancies:
            field = discrepancy.field
            if field not in report['by_field']:
                report['by_field'][field] = {
                    'count': 0,
                    'significant_count': 0,
                    'avg_percentage_diff': 0,
                    'max_percentage_diff': 0
                }

            field_stats = report['by_field'][field]
            field_stats['count'] += 1

            if discrepancy.is_significant:
                field_stats['significant_count'] += 1

            if discrepancy.percentage_diff is not None:
                field_stats['max_percentage_diff'] = max(
                    field_stats['max_percentage_diff'],
                    discrepancy.percentage_diff
                )

            # Track missing data
            if discrepancy.filemaker_value is None:
                report['missing_data']['filemaker_missing'].append({
                    'PartNumber': discrepancy.part_number,
                    'Field': discrepancy.field,
                    'IseriesValue': discrepancy.iseries_value
                })
            elif discrepancy.iseries_value is None:
                report['missing_data']['iseries_missing'].append({
                    'PartNumber': discrepancy.part_number,
                    'Field': discrepancy.field,
                    'FilemakerValue': discrepancy.filemaker_value
                })

        # Calculate averages
        for field_stats in report['by_field'].values():
            if field_stats['count'] > 0:
                significant_discrepancies = [
                    d for d in discrepancies
                    if d.field == field and d.percentage_diff is not None
                ]
                if significant_discrepancies:
                    field_stats['avg_percentage_diff'] = sum(
                        d.percentage_diff for d in significant_discrepancies
                    ) / len(significant_discrepancies)

        # Top discrepancies by percentage
        sorted_discrepancies = sorted(
            [d for d in discrepancies if d.percentage_diff is not None],
            key=lambda x: x.percentage_diff,
            reverse=True
        )

        report['top_discrepancies'] = [
            {
                'PartNumber': d.part_number,
                'Field': d.field,
                'FilemakerValue': d.filemaker_value,
                'IseriesValue': d.iseries_value,
                'PercentageDiff': d.percentage_diff
            }
            for d in sorted_discrepancies[:20]  # Top 20
        ]

        return report