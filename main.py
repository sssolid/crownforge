# src/main.py
"""
Enhanced main application entry point with improved error handling and logging.
"""

import sys
import logging
import argparse
from pathlib import Path
from typing import Optional
import traceback
import signal
from datetime import datetime

# Add project root to Python path
PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.application.bootstrap.application_bootstrap import ApplicationContainer
from src.infrastructure.configuration.configuration_manager import EnhancedConfigurationManager

# Global container for graceful shutdown
app_container: Optional[ApplicationContainer] = None


def setup_logging(config_manager: EnhancedConfigurationManager) -> None:
    """Setup comprehensive logging configuration."""
    log_level = config_manager.get_value("logging.level", "INFO")
    log_format = config_manager.get_value(
        "logging.format",
        "%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s"
    )
    console_output = config_manager.get_value("logging.console_output", True)

    # Ensure log directory exists
    log_file = config_manager.get_value("files.log_file", "logs/application.log")
    Path(log_file).parent.mkdir(parents=True, exist_ok=True)

    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, log_level.upper()))

    # Clear existing handlers
    root_logger.handlers.clear()

    # File handler
    file_handler = logging.FileHandler(log_file, mode='a', encoding='utf-8')
    file_handler.setLevel(getattr(logging, log_level.upper()))
    file_formatter = logging.Formatter(log_format)
    file_handler.setFormatter(file_formatter)
    root_logger.addHandler(file_handler)

    # Console handler
    if console_output:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(getattr(logging, log_level.upper()))
        console_formatter = logging.Formatter(
            "%(asctime)s - %(levelname)s - %(message)s"
        )
        console_handler.setFormatter(console_formatter)
        root_logger.addHandler(console_handler)

    # Set module-specific log levels
    module_loggers = config_manager.get_value("logging.loggers", {})
    for module, level in module_loggers.items():
        logging.getLogger(module).setLevel(getattr(logging, level.upper()))

    logging.info("Logging configuration completed")


def setup_signal_handlers() -> None:
    """Setup signal handlers for graceful shutdown."""

    def signal_handler(signum, frame):
        logger = logging.getLogger(__name__)
        logger.info(f"Received signal {signum}, initiating graceful shutdown...")

        if app_container:
            app_container.shutdown()

        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)


def validate_environment(config_manager: EnhancedConfigurationManager) -> bool:
    """Validate application environment and dependencies."""
    logger = logging.getLogger(__name__)
    errors = []

    # Check required files
    required_files = [
        config_manager.get_value("files.lookup_file"),
        config_manager.get_value("database.filemaker.fmjdbc_jar_path"),
        config_manager.get_value("database.iseries.jt400_jar_path")
    ]

    for file_path in required_files:
        if file_path and not Path(file_path).exists():
            errors.append(f"Required file not found: {file_path}")

    # Check Python dependencies
    try:
        import pandas
        import openpyxl
        import jaydebeapi
        import jpype
    except ImportError as e:
        errors.append(f"Missing Python dependency: {e}")

    # Validate configuration
    config_errors = config_manager.validate_configuration()
    errors.extend(config_errors)

    if errors:
        logger.error("Environment validation failed:")
        for error in errors:
            logger.error(f"  - {error}")
        return False

    logger.info("Environment validation passed")
    return True


def create_sample_configuration(output_path: str = "config.yaml") -> None:
    """Create a sample configuration file."""
    config_manager = EnhancedConfigurationManager()
    config_manager.save_configuration(output_path)
    print(f"✅ Sample configuration created: {output_path}")
    print("📝 Please edit the configuration file with your specific settings.")


def validate_configuration_file(config_path: str) -> bool:
    """Validate configuration file."""
    try:
        config_manager = EnhancedConfigurationManager(config_path)
        errors = config_manager.validate_configuration()

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


def list_workflow_steps(config_path: str) -> None:
    """List available workflow steps and their status."""
    try:
        config_manager = EnhancedConfigurationManager(config_path)

        print("🔧 WORKFLOW CONFIGURATION")
        print("=" * 50)

        enabled_steps = config_manager.get_value("workflow.enabled_steps", [])
        all_steps = [
            "applications", "marketing_descriptions", "popularity_codes",
            "sdc_template", "validation_reports"
        ]

        print("Available Steps:")
        for step in all_steps:
            status = "✅ ENABLED" if step in enabled_steps else "⭕ DISABLED"
            print(f"  {step:<25} {status}")

        print(f"\nTotal enabled: {len(enabled_steps)}/{len(all_steps)}")

        # Show dependencies
        dependencies = config_manager.get_value("workflow.step_dependencies", {})
        if dependencies:
            print("\nStep Dependencies:")
            for step, deps in dependencies.items():
                if deps:
                    print(f"  {step} depends on: {', '.join(deps)}")
                else:
                    print(f"  {step} has no dependencies")

    except Exception as e:
        print(f"💥 Error reading workflow configuration: {e}")


def run_application_workflow(config_path: str, args: argparse.Namespace) -> bool:
    """Run the main application workflow."""
    global app_container

    logger = logging.getLogger(__name__)

    try:
        # Initialize application container
        logger.info("Initializing application container...")
        app_container = ApplicationContainer(config_path)

        # Get workflow engine
        workflow_engine = app_container.get_workflow_engine()

        # Determine steps to execute
        steps_to_execute = None
        if args.step:
            steps_to_execute = [args.step]

        # Display workflow status
        workflow_status = workflow_engine.get_workflow_status()
        enabled_steps = workflow_status.get('enabled_steps', [])

        if steps_to_execute:
            logger.info(f"Executing specific step: {args.step}")
        else:
            logger.info(f"Executing workflow with {len(enabled_steps)} enabled steps: {enabled_steps}")

        # Execute workflow
        logger.info("🚀 Starting workflow execution...")
        start_time = datetime.now()

        result = workflow_engine.execute_workflow(steps_to_execute)

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        # Report results
        if result.success:
            logger.info("🎉 Workflow completed successfully!")
            logger.info(f"⏱️  Total execution time: {duration:.2f} seconds")
            logger.info(f"✅ Items processed: {result.items_processed}")

            # Display generated files
            if 'step_results' in result.data:
                logger.info("📄 Generated files:")
                for step, step_result in result.data['step_results'].items():
                    if 'output_file' in step_result:
                        logger.info(f"   {step}: {step_result['output_file']}")
        else:
            logger.error("❌ Workflow completed with errors!")
            logger.error(f"⏱️  Execution time: {duration:.2f} seconds")
            logger.error(f"❌ Items failed: {result.items_failed}")

            for error in result.errors:
                logger.error(f"   📋 {error}")

        return result.success

    except KeyboardInterrupt:
        logger.info("⏹️  Workflow interrupted by user")
        return False
    except Exception as e:
        logger.error(f"💥 Unexpected error during workflow: {e}")
        logger.debug(traceback.format_exc())
        return False
    finally:
        # Cleanup
        if app_container:
            app_container.shutdown()


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Automotive Parts Data Processing Application",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
🚗 Examples:
  %(prog)s                          # Run complete workflow
  %(prog)s -c custom_config.yaml    # Use custom configuration
  %(prog)s --create-config          # Create sample configuration
  %(prog)s --validate-config        # Validate configuration
  %(prog)s --list-steps             # Show available workflow steps
  %(prog)s --step applications      # Run specific step only
  %(prog)s -v                       # Enable verbose output
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
        "--step",
        choices=["applications", "marketing_descriptions", "popularity_codes", "sdc_template", "validation_reports"],
        help="Run only a specific workflow step"
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

    args = parser.parse_args()

    # Handle special commands
    if args.create_config:
        create_sample_configuration(args.config)
        return 0

    if args.validate_config:
        success = validate_configuration_file(args.config)
        return 0 if success else 1

    if args.list_steps:
        list_workflow_steps(args.config)
        return 0

    # Check if config file exists
    if not Path(args.config).exists():
        print(f"❌ Configuration file not found: {args.config}")
        print("💡 Use --create-config to create a sample configuration file.")
        return 1

    # Initialize configuration and logging
    try:
        config_manager = EnhancedConfigurationManager(args.config)
        setup_logging(config_manager)
        setup_signal_handlers()

        logger = logging.getLogger(__name__)
        logger.info("=" * 80)
        logger.info("AUTOMOTIVE PARTS DATA PROCESSING APPLICATION")
        logger.info("=" * 80)
        logger.info(f"Configuration: {args.config}")
        logger.info(f"Started at: {datetime.now().isoformat()}")

        # Validate environment
        if not validate_environment(config_manager):
            return 1

        if args.dry_run:
            logger.info("✅ Dry run completed - environment validation passed")
            return 0

        # Run main workflow
        success = run_application_workflow(args.config, args)
        return 0 if success else 1

    except Exception as e:
        print(f"💥 Fatal error: {e}")
        print(traceback.format_exc())
        return 1


if __name__ == "__main__":
    sys.exit(main())