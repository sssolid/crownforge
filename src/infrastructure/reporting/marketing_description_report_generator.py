# src/infrastructure/reporting/marketing_description_report_generator.py
"""
Specialized report generator for marketing description validation results.
"""

import logging
from typing import Dict, Any

from .excel_report_generator import ExcelReportGenerator, ExcelReportConfig, SheetDefinition
from ...domain.models import ProcessingResult

logger = logging.getLogger(__name__)


class MarketingDescriptionReportGenerator(ExcelReportGenerator):
    """Specialized report generator for marketing description validation."""

    def __init__(self, config: ExcelReportConfig):
        super().__init__(config)
        self.marketing_sheets = self._initialize_marketing_sheet_definitions()

    def generate_marketing_validation_report(self, validation_data: Dict[str, Any],
                                             output_path: str) -> ProcessingResult:
        """Generate marketing description validation report."""
        # Prepare data for Excel generation
        excel_data = self._prepare_marketing_data_for_excel(validation_data)

        # Generate report using parent class
        return self.generate_report(excel_data, output_path)

    @staticmethod
    def _initialize_marketing_sheet_definitions() -> Dict[str, SheetDefinition]:
        """Initialize marketing-specific sheet definitions."""
        return {
            'missing_descriptions': SheetDefinition(
                name='Missing Descriptions',
                data_key='missing_descriptions',
                description='Terminology IDs without marketing descriptions',
                sort_columns=['terminology_id']
            ),
            'invalid_descriptions': SheetDefinition(
                name='Invalid Descriptions',
                data_key='invalid_descriptions',
                description='Marketing descriptions with validation errors',
                sort_columns=['terminology_id']
            ),
            'fallback_required': SheetDefinition(
                name='Fallback Required',
                data_key='fallback_required',
                description='Items requiring RTOffRoadAdCopy fallback',
                sort_columns=['terminology_id']
            ),
            'validation_summary': SheetDefinition(
                name='Validation Summary',
                data_key='validation_summary',
                description='Overall validation statistics'
            )
        }

    @staticmethod
    def _prepare_marketing_data_for_excel(validation_data: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare marketing validation data for Excel report generation."""
        excel_data = {}

        # Summary data
        summary = validation_data.get('summary', {})
        excel_data['validation_summary'] = [
            {'Metric': 'Total Descriptions', 'Value': summary.get('total_descriptions', 0)},
            {'Metric': 'Missing Descriptions', 'Value': summary.get('missing_descriptions', 0)},
            {'Metric': 'Invalid Descriptions', 'Value': summary.get('invalid_descriptions', 0)},
            {'Metric': 'Fallback Required', 'Value': summary.get('fallback_required', 0)},
            {'Metric': 'Validation Rate (%)', 'Value': f"{summary.get('validation_rate', 0):.1f}%"},
            {'Metric': 'Report Generated', 'Value': validation_data.get('generated_at', 'Unknown')}
        ]

        # Missing descriptions
        missing_descriptions = validation_data.get('missing_descriptions', [])
        if missing_descriptions:
            excel_data['missing_descriptions'] = [
                {
                    'Terminology ID': item.get('terminology_id', ''),
                    'Status': item.get('status', 'Missing'),
                    'Action Required': 'Add marketing description',
                    'Priority': 'High' if 'jeep' in item.get('terminology_id', '').lower() else 'Medium'
                }
                for item in missing_descriptions
            ]

        # Invalid descriptions
        invalid_descriptions = validation_data.get('invalid_descriptions', [])
        if invalid_descriptions:
            excel_data['invalid_descriptions'] = [
                {
                    'Terminology ID': item.get('terminology_id', ''),
                    'Jeep Description': item.get('jeep_description', ''),
                    'Validation Status': item.get('validation_status', ''),
                    'Review Notes': item.get('review_notes', ''),
                    'Needs Addition': 'Yes' if item.get('needs_to_be_added', False) else 'No',
                    'Action Required': 'Review and correct description'
                }
                for item in invalid_descriptions
            ]

        # Fallback required
        fallback_required = validation_data.get('fallback_required', [])
        if fallback_required:
            excel_data['fallback_required'] = [
                {
                    'Terminology ID': item.get('terminology_id', ''),
                    'Reason': item.get('reason', 'Missing or invalid description'),
                    'Fallback Source': 'RTOffRoadAdCopy',
                    'Impact': 'SDC template will use fallback',
                    'Priority': 'Medium'
                }
                for item in fallback_required
            ]

        # Validation details (if provided)
        validation_details = validation_data.get('validation_details', [])
        if validation_details:
            detailed_results = []
            for idx, detail in enumerate(validation_details):
                if detail.get('has_errors') or detail.get('has_warnings'):
                    detailed_results.append({
                        'Record Index': idx + 1,
                        'Has Errors': 'Yes' if detail.get('has_errors', False) else 'No',
                        'Has Warnings': 'Yes' if detail.get('has_warnings', False) else 'No',
                        'Error Count': detail.get('error_count', 0),
                        'Warning Count': detail.get('warning_count', 0),
                        'Errors': '; '.join(detail.get('errors', [])),
                        'Warnings': '; '.join(detail.get('warnings', []))
                    })

            if detailed_results:
                excel_data['validation_details'] = detailed_results

        return excel_data