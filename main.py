#!/usr/bin/env python3
"""
Main Application Runner - Automotive Parts Application Parser
Integrated version with workflow orchestration and enhanced features
"""

import sys
import logging
import argparse
from pathlib import Path
from typing import Optional
import traceback

from src.jvm_manager import start_jvm_once

# Add the project root to Python path
PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

# Import our modules
from src.utils import ConfigManager, FileUtils, performance_monitor
from src.workflow_service import WorkflowService

# Progress bar support
try:
    from tqdm import tqdm

    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False
    print("Warning: tqdm not available. Progress bars will be disabled.")


def setup_logging(config_manager: ConfigManager):
    """Setup logging configuration"""
    log_level = config_manager.get("logging.level", "INFO")
    log_format = config_manager.get("logging.format",
                                    "%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    console_output = config_manager.get("logging.console_output", True)

    # Setup file logging
    log_file = config_manager.get("files.log_file", "logs/application_parser.log")
    FileUtils.ensure_directory(Path(log_file).parent)

    # Create handlers
    handlers = [logging.FileHandler(log_file)]
    if console_output:
        handlers.append(logging.StreamHandler(sys.stdout))

    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format=log_format,
        handlers=handlers
    )

    # Set module-specific log levels
    module_loggers = config_manager.get("logging.loggers", {})
    for module, level in module_loggers.items():
        logging.getLogger(module).setLevel(getattr(logging, level.upper()))


def validate_dependencies(config_manager: ConfigManager) -> bool:
    """Validate that all required files and dependencies exist"""
    logger = logging.getLogger(__name__)
    errors = []

    # Check required files
    required_files = [
        config_manager.get("files.lookup_file"),
        config_manager.get("database.filemaker.dsn_file_path")
    ]

    for file_path in required_files:
        if file_path and not Path(file_path).exists():
            errors.append(f"Required file not found: {file_path}")

    # Check optional files (warn if missing)
    optional_files = [
        config_manager.get("files.verification_file"),
        config_manager.get("files.sdc_blank_template"),
        config_manager.get("files.missing_parts_list")
    ]

    for file_path in optional_files:
        if file_path and not Path(file_path).exists():
            logger.warning(f"Optional file not found: {file_path}")

    # Check Python dependencies
    try:
        import pandas
        import openpyxl
        import jaydebeapi  # For AS400 connection
    except ImportError as e:
        errors.append(f"Missing Python dependency: {e}")

    # Check progress bar availability if enabled
    if config_manager.get("monitoring.enable_progress_bars", False) and not TQDM_AVAILABLE:
        logger.warning("Progress bars requested but tqdm not available")

    if errors:
        for error in errors:
            logger.error(error)
        return False

    return True


def backup_existing_outputs(config_manager: ConfigManager):
    """Backup existing output files if they exist"""
    backup_dir = config_manager.get("files.backup_dir", "backups")

    output_files = [
        config_manager.get("files.application_data"),
        config_manager.get("files.popularity_codes"),
        config_manager.get("files.sdc_populated_template"),
        config_manager.get("files.sdc_final_template"),
        config_manager.get("files.upc_validation_report")
    ]

    for output_file in output_files:
        if output_file and Path(output_file).exists():
            backup_path = FileUtils.backup_file(output_file, backup_dir)
            if backup_path:
                logging.info(f"Backup created: {backup_path}")


def create_progress_bar(description: str, total: int, enabled: bool = True):
    """Create progress bar if available and enabled"""
    if enabled and TQDM_AVAILABLE:
        return tqdm(total=total, desc=description, unit="item")
    return None


def run_workflow(config_path: str, args: argparse.Namespace) -> bool:
    """Main workflow execution function"""
    logger = logging.getLogger(__name__)

    try:
        # Load configuration
        config_manager = ConfigManager(config_path)
        setup_logging(config_manager)

        logger.info("=" * 80)
        logger.info("AUTOMOTIVE PARTS DATA PROCESSING WORKFLOW")
        logger.info("=" * 80)
        logger.info(f"Configuration loaded from: {config_path}")

        # Validate dependencies
        if not validate_dependencies(config_manager):
            logger.error("Dependency validation failed")
            return False

        # Backup existing outputs if requested
        if args.backup_output:
            backup_existing_outputs(config_manager)

        # If specific step enable it only
        if args.step:
            config_manager._config['workflow']['enabled_steps'] = [args.step, ]
        # Initialize workflow service
        workflow_service = WorkflowService(config_manager)

        # Show workflow plan
        workflow_status = workflow_service.get_workflow_status()
        logger.info(f"Workflow plan: {len(workflow_status['enabled_steps'])} steps enabled")
        for step in workflow_status['enabled_steps']:
            logger.info(f"  - {step}")

        # Initialize jvms
        logger.info(f"Initializing JVMs")
        jar_paths = [config_manager._config["database"]["filemaker"]["fmjdbc_jar_path"],
                        config_manager._config["database"]["iseries"]["jt400_jar_path"]]
        start_jvm_once(jar_paths)

        # Execute workflow
        with performance_monitor("Complete Workflow Execution") as monitor:
            logger.info("Starting workflow execution...")

            # Create progress bar for workflow steps
            progress_enabled = config_manager.get("monitoring.enable_progress_bars", False)
            step_progress = create_progress_bar(
                "Workflow Steps",
                len(workflow_status['enabled_steps']),
                progress_enabled
            )

            result = workflow_service.execute_workflow()

            if step_progress:
                step_progress.update(len(result.completed_steps))
                step_progress.close()

            monitor.increment_processed(len(result.completed_steps))
            monitor.increment_errors(len(result.failed_steps))

            # Report results
            if result.success:
                logger.info("🎉 Workflow completed successfully!")
                logger.info(f"✅ Completed steps: {len(result.completed_steps)}")
                for step in result.completed_steps:
                    logger.info(f"   ✓ {step}")

                # Display step results
                for step, step_result in result.results.items():
                    if isinstance(step_result, dict) and 'output_file' in step_result:
                        logger.info(f"   📄 {step}: {step_result['output_file']}")

            else:
                logger.error("❌ Workflow completed with errors!")
                logger.info(f"✅ Completed steps: {len(result.completed_steps)}")
                logger.error(f"❌ Failed steps: {len(result.failed_steps)}")

                for step in result.failed_steps:
                    logger.error(f"   ✗ {step}")

                for error in result.errors:
                    logger.error(f"   📋 {error}")

            # Generate summary report
            _generate_workflow_summary(result, config_manager)

            return result.success

    except KeyboardInterrupt:
        logger.info("⏹️  Workflow interrupted by user")
        return False
    except Exception as e:
        logger.error(f"💥 Unexpected error during workflow: {e}")
        logger.debug(traceback.format_exc())
        return False


def _generate_workflow_summary(result, config_manager):
    """Generate workflow execution summary"""
    logger = logging.getLogger(__name__)

    summary_lines = []
    summary_lines.append("=" * 80)
    summary_lines.append("WORKFLOW EXECUTION SUMMARY")
    summary_lines.append("=" * 80)

    summary_lines.append(f"Total Steps: {len(result.completed_steps) + len(result.failed_steps)}")
    summary_lines.append(f"Completed: {len(result.completed_steps)}")
    summary_lines.append(f"Failed: {len(result.failed_steps)}")
    summary_lines.append(
        f"Success Rate: {len(result.completed_steps) / max(1, len(result.completed_steps) + len(result.failed_steps)) * 100:.1f}%")

    if result.results:
        summary_lines.append("")
        summary_lines.append("GENERATED FILES:")
        for step, step_result in result.results.items():
            if isinstance(step_result, dict):
                if 'output_file' in step_result:
                    summary_lines.append(f"  {step}: {step_result['output_file']}")
                elif 'input_files' in step_result and 'output_file' in step_result:
                    summary_lines.append(f"  {step}: {step_result['output_file']}")

    summary_lines.append("=" * 80)

    for line in summary_lines:
        logger.info(line)


def create_sample_config(output_path: str = "config.yaml"):
    """Create a sample configuration file"""
    config_manager = ConfigManager()
    config_manager.save_config(output_path)
    print(f"✅ Sample configuration created: {output_path}")
    print("📝 Please edit the configuration file with your specific settings.")


def validate_config(config_path: str):
    """Validate configuration file"""
    try:
        config_manager = ConfigManager(config_path)
        errors = []

        # Check required configuration sections
        required_sections = [
            "database.filemaker.dsn",
            "database.filemaker.username",
            "database.iseries.server",
            "files.lookup_file",
            "files.application_data"
        ]

        for section in required_sections:
            if config_manager.get(section) is None:
                errors.append(f"Missing required configuration: {section}")

        # Validate workflow configuration
        enabled_steps = config_manager.get("workflow.enabled_steps", [])
        valid_steps = ["applications", "popularity_codes", "sdc_template", "partshub_template", "validations"]

        for step in enabled_steps:
            if step not in valid_steps:
                errors.append(f"Unknown workflow step: {step}")

        if errors:
            print("❌ Configuration validation failed:")
            for error in errors:
                print(f"  ⚠️  {error}")
            return False
        else:
            print("✅ Configuration validation passed!")
            return True

    except Exception as e:
        print(f"💥 Configuration validation error: {e}")
        return False


def list_workflow_steps(config_path: str):
    """List available workflow steps and their status"""
    try:
        config_manager = ConfigManager(config_path)

        print("🔧 WORKFLOW CONFIGURATION")
        print("=" * 50)

        enabled_steps = config_manager.get("workflow.enabled_steps", [])
        all_steps = ["applications", "popularity_codes", "sdc_template", "partshub_template", "validations"]

        print("Available Steps:")
        for step in all_steps:
            status = "✅ ENABLED" if step in enabled_steps else "⭕ DISABLED"
            print(f"  {step:<20} {status}")

        print(f"\nTotal enabled: {len(enabled_steps)}/{len(all_steps)}")

        # Show dependencies
        dependencies = config_manager.get("workflow.step_dependencies", {})
        if dependencies:
            print("\nStep Dependencies:")
            for step, deps in dependencies.items():
                if deps:
                    print(f"  {step} depends on: {', '.join(deps)}")
                else:
                    print(f"  {step} has no dependencies")

    except Exception as e:
        print(f"💥 Error reading workflow configuration: {e}")


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="Automotive Parts Data Processing Workflow",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
🚗 Examples:
  %(prog)s                          # Run complete workflow with default config
  %(prog)s -c custom_config.yaml    # Run with custom configuration
  %(prog)s --create-config          # Create sample configuration
  %(prog)s --validate-config        # Validate configuration
  %(prog)s --list-steps             # Show workflow steps
  %(prog)s --backup-output          # Backup existing files before processing
  %(prog)s --dry-run                # Validate setup without running
  %(prog)s -v                       # Enable verbose output
  %(prog)s --step                   # Run only a specific step
        """
    )

    parser.add_argument(
        "-c", "--config",
        default="config.yaml",
        help="Path to configuration file (default: config.yaml)"
    )

    parser.add_argument(
        "--create-config",
        action="store_true",
        help="Create a sample configuration file and exit"
    )

    parser.add_argument(
        "--validate-config",
        action="store_true",
        help="Validate configuration file and exit"
    )

    parser.add_argument(
        "--list-steps",
        action="store_true",
        help="List workflow steps and their configuration"
    )

    parser.add_argument(
        "--backup-output",
        action="store_true",
        help="Backup existing output files before processing"
    )

    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose output"
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate setup without running processing"
    )

    parser.add_argument(
        "--step",
        help="Run only a specific workflow step"
    )

    args = parser.parse_args()

    # Handle special commands
    if args.create_config:
        create_sample_config(args.config)
        return 0

    if args.validate_config:
        success = validate_config(args.config)
        return 0 if success else 1

    if args.list_steps:
        list_workflow_steps(args.config)
        return 0

    # Check if config file exists
    if not Path(args.config).exists():
        print(f"❌ Configuration file not found: {args.config}")
        print("💡 Use --create-config to create a sample configuration file.")
        return 1

    if args.dry_run:
        print("🔍 Dry run mode - validating setup...")
        config_manager = ConfigManager(args.config)
        if validate_dependencies(config_manager):
            print("✅ Setup validation passed!")
            return 0
        else:
            print("❌ Setup validation failed!")
            return 1

    # Run main workflow
    success = run_workflow(args.config, args)
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
