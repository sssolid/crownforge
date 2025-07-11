"""
Popularity Codes Service
Generates popularity codes based on sales data
"""

import logging
import csv
from datetime import datetime
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from pathlib import Path

from .database import IseriesService, FilemakerService
from .utils import performance_monitor

logger = logging.getLogger(__name__)


@dataclass
class PopularityConfig:
    """Popularity codes configuration"""
    default_branch: str = "1"
    default_brand: str = "All"
    start_date_format: str = "%Y%m%d"
    default_start_date: Optional[str] = None
    thresholds: Dict[str, float] = None

    def __post_init__(self):
        if self.default_start_date is None:
            self.default_start_date = datetime.now().strftime("%Y") + "0101"

        if self.thresholds is None:
            self.thresholds = {
                'top_tier': 60.0,  # A: Top 60%
                'second_tier': 20.0,  # B: Next 20%
                'third_tier': 15.0,  # C: Next 15%
                'bottom_tier': 5.0  # D: Last 5%
            }


class PopularityService:
    """Service for generating popularity codes"""

    def __init__(self, iseries_service: IseriesService, filemaker_service: FilemakerService, config: Dict[str, Any]):
        self.iseries = iseries_service
        self.filemaker = filemaker_service
        self.config = PopularityConfig(**config)

    def generate_popularity_codes(self, output_file: str, branch: str = None,
                                  brand: str = None, start_date: str = None) -> Dict[str, Any]:
        """
        Generate popularity codes report

        Args:
            output_file: Path to output CSV file
            branch: Branch number (default from config)
            brand: Brand filter (default from config)
            start_date: Start date for sales data (default from config)

        Returns:
            Dictionary with generation results
        """
        branch = branch or self.config.default_branch
        brand = brand or self.config.default_brand
        start_date = start_date or self.config.default_start_date

        logger.info(f"Generating popularity codes for branch {branch}, brand {brand}, from {start_date}")

        with performance_monitor("Popularity Codes Generation") as monitor:
            # Get active marketing products from Filemaker
            products = self._get_active_products(brand)
            monitor.increment_processed(len(products))

            # Get sales data from Iseries
            sales_data = self.iseries.get_popularity_sales_data(start_date, branch)
            monitor.increment_processed(len(sales_data))

            # Get stock data
            stock_data = self.iseries.get_stock_data(branch)
            monitor.increment_processed(len(stock_data))

            # Process and categorize data
            results = self._process_popularity_data(products, sales_data, stock_data)

            # Generate CSV output
            self._write_popularity_csv(results, output_file)

            summary = {
                'total_products': len(products),
                'total_sales_records': len(sales_data),
                'total_with_sales': len(results['with_sales']),
                'total_without_sales': len(results['without_sales']),
                'total_inactive': len(results['inactive']),
                'output_file': output_file,
                'categories': {
                    'A': len([r for r in results['with_sales'] if r.get('PopularityCode') == 'A']),
                    'B': len([r for r in results['with_sales'] if r.get('PopularityCode') == 'B']),
                    'C': len([r for r in results['with_sales'] if r.get('PopularityCode') == 'C']),
                    'D': len([r for r in results['with_sales'] if r.get('PopularityCode') == 'D']),
                }
            }

            logger.info(f"Popularity codes generated: {summary}")
            return summary

    def _get_active_products(self, brand: str) -> Dict[str, Dict[str, Any]]:
        """Get active products from Filemaker"""
        query_results = self.filemaker.get_popularity_data()

        products = {}
        for product in query_results:
            part_number = product.get('AS400_NumberStripped')
            product_brand = product.get('PartBrand', '')

            # Apply brand filter
            if brand != "All" and product_brand != brand:
                continue

            products[part_number] = {
                "brand": product_brand,
                "category_tertiary": product.get('PartTertiaryCategory', ''),
                "description": product.get('PartDescription', '')
            }

        logger.info(f"Found {len(products)} active products for brand '{brand}'")
        return products

    def _process_popularity_data(self, products: Dict[str, Dict], sales_data: List[Dict],
                                 stock_data: List[Dict]) -> Dict[str, List[Dict]]:
        """Process sales data and assign popularity codes"""

        # Create stock lookup
        stock_lookup = {record.get('SNSCHR', '').strip(): record for record in stock_data}

        # Separate active and inactive products based on sales
        active_sales = []
        inactive_products = []

        for record in sales_data:
            part_number = record.get('Number', '').strip()
            if part_number in products:
                # Add product info to sales record
                record.update(products[part_number])
                active_sales.append(record)
            else:
                # Product not in active list
                record['PopularityCode'] = 'K'  # Inactive
                inactive_products.append(record)

        # Calculate total units sold for percentage calculations
        total_units_sold = sum(record.get('Sold', 0) for record in active_sales)

        if total_units_sold == 0:
            logger.warning("No sales data found - all products will be assigned 'D' codes")
            for record in active_sales:
                record['PopularityCode'] = 'D'
            return {
                'with_sales': active_sales,
                'without_sales': [],
                'inactive': inactive_products
            }

        # Sort by units sold (descending)
        active_sales.sort(key=lambda x: x.get('Sold', 0), reverse=True)

        # Assign popularity codes based on thresholds
        self._assign_popularity_codes(active_sales, total_units_sold)

        # Handle products with no sales data
        products_without_sales = []
        sales_part_numbers = {r.get('Number', '').strip() for r in active_sales}

        for part_number, product_info in products.items():
            if part_number not in sales_part_numbers:
                stock_record = stock_lookup.get(part_number, {})
                record = {
                    'Number': part_number,
                    'Description': product_info.get('description', ''),
                    'Sold': 0,
                    'Stock': stock_record.get('SCLSK', 0),
                    'Allocated': stock_record.get('SALLOC', 0),
                    'Stock Less Allocated': stock_record.get('SCLSK', 0) - stock_record.get('SALLOC', 0),
                    'Jobber': stock_record.get('SRET1', 0),
                    'Revenue': 0,
                    'Cost': 0,
                    'PopularityCode': 'D',  # Default for no sales
                    'brand': product_info.get('brand', ''),
                    'category_tertiary': product_info.get('category_tertiary', '')
                }
                products_without_sales.append(record)

        return {
            'with_sales': active_sales,
            'without_sales': products_without_sales,
            'inactive': inactive_products
        }

    def _assign_popularity_codes(self, sales_data: List[Dict], total_units_sold: int):
        """Assign popularity codes based on cumulative sales percentages"""
        thresholds = self.config.thresholds
        cumulative_percentage = 0.0

        for record in sales_data:
            units_sold = record.get('Sold', 0)
            percentage = (units_sold / total_units_sold) * 100
            cumulative_percentage += percentage

            if cumulative_percentage <= thresholds['top_tier']:
                record['PopularityCode'] = 'A'
            elif cumulative_percentage <= thresholds['top_tier'] + thresholds['second_tier']:
                record['PopularityCode'] = 'B'
            elif cumulative_percentage <= thresholds['top_tier'] + thresholds['second_tier'] + thresholds['third_tier']:
                record['PopularityCode'] = 'C'
            else:
                record['PopularityCode'] = 'D'

    def _write_popularity_csv(self, results: Dict[str, List[Dict]], output_file: str):
        """Write popularity codes to CSV file"""

        # Ensure output directory exists
        Path(output_file).parent.mkdir(parents=True, exist_ok=True)

        with open(output_file, mode='w', newline='', encoding='utf-8') as fp:
            writer = csv.writer(fp, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)

            # Write header
            writer.writerow([
                'Brand',
                'Tertiary Category',
                'Part Number',
                'Description',
                'Sold',
                'Stock',
                'Allocated',
                'Stock Less Allocated',
                'Jobber',
                'Revenue',
                'Cost',
                'Popularity Code'
            ])

            # Write all records
            all_records = results['with_sales'] + results['without_sales'] + results['inactive']

            for record in all_records:
                writer.writerow([
                    record.get('brand', ''),
                    record.get('category_tertiary', ''),
                    record.get('Number', ''),
                    record.get('Description', ''),
                    record.get('Sold', 0),
                    record.get('Stock', 0),
                    record.get('Allocated', 0),
                    record.get('Stock Less Allocated', 0),
                    record.get('Jobber', 0),
                    record.get('Revenue', 0),
                    record.get('Cost', 0),
                    record.get('PopularityCode', 'D')
                ])

        logger.info(f"Popularity codes written to {output_file}")

    def get_popularity_mapping(self, csv_file: str) -> Dict[str, str]:
        """
        Load popularity codes from CSV file as a mapping

        Args:
            csv_file: Path to popularity codes CSV file

        Returns:
            Dictionary mapping part number to popularity code
        """
        mapping = {}

        try:
            with open(csv_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    part_number = row.get('Part Number', '').strip()
                    popularity_code = row.get('Popularity Code', 'D').strip()
                    if part_number:
                        mapping[part_number] = popularity_code

            logger.info(f"Loaded {len(mapping)} popularity code mappings from {csv_file}")

        except FileNotFoundError:
            logger.warning(f"Popularity codes file not found: {csv_file}")
        except Exception as e:
            logger.error(f"Error loading popularity codes: {e}")

        return mapping