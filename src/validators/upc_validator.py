"""
UPC Validation Module
Validates UPC codes and checks for duplicates
"""

import logging
from typing import List, Dict, Any, Tuple, Set
from collections import defaultdict
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class UPCValidationResult:
    """Result of UPC validation"""
    is_valid: bool
    error_message: str = ""
    suggested_check_digit: str = ""


@dataclass
class UPCValidationConfig:
    """UPC validation configuration"""
    enabled: bool = True
    check_duplicates: bool = True
    validate_check_digit: bool = True
    allowed_lengths: List[int] = None

    def __post_init__(self):
        if self.allowed_lengths is None:
            self.allowed_lengths = [12, 13, 14]


class UPCValidator:
    """UPC validation service"""

    def __init__(self, config: Dict[str, Any]):
        self.config = UPCValidationConfig(**config)

    def validate_upc(self, upc_value: Any) -> UPCValidationResult:
        """
        Validate a single UPC code

        Args:
            upc_value: UPC value to validate

        Returns:
            UPCValidationResult with validation status and details
        """
        if not self.config.enabled:
            return UPCValidationResult(is_valid=True)

        # Convert to string and clean
        upc_str = str(upc_value).strip() if upc_value is not None else ""

        if not upc_str:
            return UPCValidationResult(is_valid=False, error_message="UPC is empty")

        # Remove any non-numeric characters
        upc_clean = ''.join(c for c in upc_str if c.isdigit())

        if not upc_clean:
            return UPCValidationResult(is_valid=False, error_message="UPC contains no digits")

        # Check length
        if len(upc_clean) not in self.config.allowed_lengths:
            return UPCValidationResult(
                is_valid=False,
                error_message=f"Invalid UPC length: {len(upc_clean)}. Expected: {self.config.allowed_lengths}"
            )

        # For 12-digit UPCs (UPC-A), validate check digit
        if len(upc_clean) == 12 and self.config.validate_check_digit:
            return self._validate_upc_12_check_digit(upc_clean)

        # For other lengths, just validate format
        return UPCValidationResult(is_valid=True)

    def _validate_upc_12_check_digit(self, upc: str) -> UPCValidationResult:
        """
        Validate 12-digit UPC check digit using UPC-A algorithm

        Args:
            upc: 12-digit UPC string

        Returns:
            UPCValidationResult
        """
        try:
            check_digit = 0

            # Add the digits in the odd-numbered positions (1st, 3rd, 5th, etc.)
            # and multiply by three
            for i in range(0, 11, 2):
                check_digit += int(upc[i])
            check_digit *= 3

            # Add the digits in the even-numbered positions (2nd, 4th, 6th, etc.)
            for i in range(1, 11, 2):
                check_digit += int(upc[i])

            # Take the remainder of the result divided by 10
            check_digit %= 10

            # If not 0, subtract from 10 to derive the check digit
            if check_digit != 0:
                check_digit = 10 - check_digit

            # Compare with actual check digit
            actual_check_digit = int(upc[11])

            if check_digit != actual_check_digit:
                return UPCValidationResult(
                    is_valid=False,
                    error_message=f"Invalid check digit. Expected: {check_digit}, Got: {actual_check_digit}",
                    suggested_check_digit=str(check_digit)
                )

            return UPCValidationResult(is_valid=True)

        except (ValueError, IndexError) as e:
            return UPCValidationResult(
                is_valid=False,
                error_message=f"Error validating UPC check digit: {e}"
            )

    def find_duplicate_upcs(self, upc_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Find duplicate UPC codes in the dataset

        Args:
            upc_data: List of dictionaries containing UPC data

        Returns:
            List of dictionaries with duplicate UPC information
        """
        if not self.config.check_duplicates:
            return []

        upc_groups = defaultdict(list)

        # Group by UPC
        for record in upc_data:
            upc = record.get('UPC', '')
            if upc:
                # Clean UPC for comparison
                upc_clean = ''.join(c for c in str(upc) if c.isdigit())
                if upc_clean:
                    upc_groups[upc_clean].append(record)

        # Find duplicates
        duplicates = []
        for upc, records in upc_groups.items():
            if len(records) > 1:
                part_numbers = [r.get('PartNumber', 'Unknown') for r in records]
                duplicates.append({
                    'UPC': upc,
                    'Count': len(records),
                    'PartNumbers': ', '.join(part_numbers),
                    'Records': records
                })

        return duplicates

    def validate_dataset(self, upc_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Validate entire UPC dataset

        Args:
            upc_data: List of dictionaries containing UPC data

        Returns:
            Dictionary with validation results
        """
        results = {
            'total_records': len(upc_data),
            'valid_upcs': 0,
            'invalid_upcs': 0,
            'empty_upcs': 0,
            'invalid_records': [],
            'duplicate_upcs': [],
            'summary': {}
        }

        if not self.config.enabled:
            results['summary']['message'] = "UPC validation is disabled"
            return results

        logger.info(f"Validating {len(upc_data)} UPC records...")

        # Validate individual UPCs
        for record in upc_data:
            upc = record.get('UPC', '')
            part_number = record.get('PartNumber', 'Unknown')

            if not upc:
                results['empty_upcs'] += 1
                continue

            validation_result = self.validate_upc(upc)

            if validation_result.is_valid:
                results['valid_upcs'] += 1
            else:
                results['invalid_upcs'] += 1
                results['invalid_records'].append({
                    'PartNumber': part_number,
                    'UPC': upc,
                    'ErrorMessage': validation_result.error_message,
                    'SuggestedCheckDigit': validation_result.suggested_check_digit
                })

        # Find duplicates
        results['duplicate_upcs'] = self.find_duplicate_upcs(upc_data)

        # Generate summary
        results['summary'] = {
            'total_records': results['total_records'],
            'valid_upcs': results['valid_upcs'],
            'invalid_upcs': results['invalid_upcs'],
            'empty_upcs': results['empty_upcs'],
            'duplicate_count': len(results['duplicate_upcs']),
            'validation_rate': (results['valid_upcs'] / max(1, results['total_records'] - results['empty_upcs'])) * 100
        }

        logger.info(f"UPC validation completed: {results['valid_upcs']} valid, "
                    f"{results['invalid_upcs']} invalid, {len(results['duplicate_upcs'])} duplicates")

        return results