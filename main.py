# main.py
"""
Enhanced main application entry point with terminal interface - works with existing bootstrap.
"""

import sys
import argparse
import traceback
import signal
from pathlib import Path
from typing import Optional
from datetime import datetime

import jpype

# Add project root to Python path
PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

# Import for initializing the jvm
from src.infrastructure.database.jvm_initializer import initialize_jvm_once

# Import existing application components
from src.application.bootstrap.application_bootstrap import ApplicationContainer
from src.infrastructure.configuration.configuration_manager import EnhancedConfigurationManager

# Import new terminal interface
try:
    from src.infrastructure.terminal.terminal_interface import get_terminal_interface, LogLevel

    TERMINAL_INTERFACE_AVAILABLE = True
except ImportError:
    TERMINAL_INTERFACE_AVAILABLE = False
    get_terminal_interface = None
    LogLevel = None
    print("Warning: Terminal interface not available, using basic output")

# Global references for graceful shutdown
app_container: Optional[ApplicationContainer] = None
terminal_interface = None


def setup_signal_handlers() -> None:
    """Setup signal handlers for graceful shutdown."""

    def signal_handler(_signum, _frame):
        if terminal_interface and TERMINAL_INTERFACE_AVAILABLE:
            terminal_interface.print_warning("Received interrupt signal, shutting down gracefully...")
        else:
            print("Received interrupt signal, shutting down gracefully...")

        if app_container:
            app_container.shutdown()

        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)


def handle_exception_with_traceback(exception: Exception, operation_context: str) -> None:
    """Common exception handling with traceback extraction."""
    # Get file and line number from traceback
    tb = traceback.extract_tb(exception.__traceback__)
    file_path = None
    line_number = None

    if tb:
        last_frame = tb[-1]
        file_path = last_frame.filename
        line_number = last_frame.lineno

    if terminal_interface and TERMINAL_INTERFACE_AVAILABLE:
        terminal_interface.print_error(
            f"{operation_context}: {exception}",
            file_path,
            line_number
        )

        # Show full traceback in verbose mode
        if terminal_interface.config.log_level in [LogLevel.VERBOSE, LogLevel.DEBUG]:
            if terminal_interface.console:
                terminal_interface.console.print_exception()
            else:
                traceback.print_exc()
    else:
        print(f"💥 {operation_context}: {exception}")
        if file_path and line_number:
            print(f"   Location: {Path(file_path).name}:{line_number}")
        traceback.print_exc()


def validate_environment(config_manager: EnhancedConfigurationManager) -> bool:
    """Validate application environment and dependencies."""
    if terminal_interface and TERMINAL_INTERFACE_AVAILABLE:
        terminal_interface.print_section("🔍 Environment Validation")
    else:
        print("\n=== Environment Validation ===")

    validation_errors = []
    warnings = []

    # Check required files
    required_files = {
        "Lookup File": config_manager.get_value("files.lookup_file"),
        "Filemaker JDBC": config_manager.get_value("database.filemaker.fmjdbc_jar_path"),
        "iSeries JDBC": config_manager.get_value("database.iseries.jt400_jar_path")
    }

    for name, file_path in required_files.items():
        if file_path and not Path(file_path).exists():
            validation_errors.append(f"{name} not found: {file_path}")

    # Check Python dependencies
    try:
        import pandas
        import openpyxl
        import jaydebeapi
        import jpype
        if terminal_interface and TERMINAL_INTERFACE_AVAILABLE:
            terminal_interface.print_success("Python dependencies verified")
        else:
            print("✅ Python dependencies verified")
    except ImportError as import_error:
        validation_errors.append(f"Missing Python dependency: {import_error}")

    # Validate configuration
    config_errors = config_manager.validate_configuration()
    if config_errors:
        warnings.extend(config_errors)

    # Report results
    if validation_errors:
        if terminal_interface and TERMINAL_INTERFACE_AVAILABLE:
            terminal_interface.print_error("Environment validation failed:")
            for error in validation_errors:
                terminal_interface.print_error(f"  • {error}")
        else:
            print("❌ Environment validation failed:")
            for error in validation_errors:
                print(f"  • {error}")
        return False

    if warnings:
        if terminal_interface and TERMINAL_INTERFACE_AVAILABLE:
            terminal_interface.print_warning("Environment validation warnings:")
            for warning in warnings:
                terminal_interface.print_warning(f"  • {warning}")
        else:
            print("⚠️  Environment validation warnings:")
            for warning in warnings:
                print(f"  • {warning}")

    if terminal_interface and TERMINAL_INTERFACE_AVAILABLE:
        terminal_interface.print_success("Environment validation passed")
    else:
        print("✅ Environment validation passed")
    return True


def create_sample_configuration(output_path: str = "config.yaml") -> int:
    """Create a sample configuration file."""
    if terminal_interface and TERMINAL_INTERFACE_AVAILABLE:
        terminal_interface.print_section("📝 Creating Sample Configuration")
    else:
        print("\n=== Creating Sample Configuration ===")

    try:
        config_manager = EnhancedConfigurationManager()
        config_manager.save_configuration(output_path)

        if terminal_interface and TERMINAL_INTERFACE_AVAILABLE:
            terminal_interface.print_success(f"Sample configuration created: {output_path}")
            terminal_interface.print_info("Please edit the configuration file with your specific settings.")
        else:
            print(f"✅ Sample configuration created: {output_path}")
            print("📝 Please edit the configuration file with your specific settings.")
        return 0
    except Exception as config_error:
        if terminal_interface and TERMINAL_INTERFACE_AVAILABLE:
            terminal_interface.print_error(f"Failed to create configuration: {config_error}")
        else:
            print(f"❌ Failed to create configuration: {config_error}")
        return 1


def validate_configuration_file(config_path: str) -> int:
    """Validate configuration file."""
    if terminal_interface and TERMINAL_INTERFACE_AVAILABLE:
        terminal_interface.print_section("🔧 Configuration Validation")
    else:
        print("\n=== Configuration Validation ===")

    try:
        config_manager = EnhancedConfigurationManager(config_path)
        config_validation_errors = config_manager.validate_configuration()

        if config_validation_errors:
            if terminal_interface and TERMINAL_INTERFACE_AVAILABLE:
                terminal_interface.print_error("Configuration validation failed:")
                for error in config_validation_errors:
                    terminal_interface.print_error(f"  • {error}")
            else:
                print("❌ Configuration validation failed:")
                for error in config_validation_errors:
                    print(f"  • {error}")
            return 1
        else:
            if terminal_interface and TERMINAL_INTERFACE_AVAILABLE:
                terminal_interface.print_success("Configuration validation passed!")
            else:
                print("✅ Configuration validation passed!")
            return 0

    except Exception as config_error:
        if terminal_interface and TERMINAL_INTERFACE_AVAILABLE:
            terminal_interface.print_error(f"Configuration validation error: {config_error}")
        else:
            print(f"❌ Configuration validation error: {config_error}")
        return 1


def list_workflow_steps(config_path: str) -> int:
    """List available workflow steps and their status."""
    if terminal_interface and TERMINAL_INTERFACE_AVAILABLE:
        terminal_interface.print_section("🔧 Workflow Configuration")
    else:
        print("\n=== Workflow Configuration ===")

    try:
        config_manager = EnhancedConfigurationManager(config_path)

        enabled_steps = config_manager.get_value("workflow.enabled_steps", [])
        all_steps = [
            "applications", "marketing_descriptions", "popularity_codes",
            "sdc_template", "validation_reports"
        ]

        if terminal_interface and TERMINAL_INTERFACE_AVAILABLE:
            # Create status data for table
            step_data = {}
            for step in all_steps:
                status = "✅ Enabled" if step in enabled_steps else "⭕ Disabled"
                step_data[step.replace('_', ' ').title()] = status
            step_data["Total Enabled"] = f"{len(enabled_steps)}/{len(all_steps)}"

            terminal_interface.print_results_table("Workflow Steps", step_data)
        else:
            print("Available Steps:")
            for step in all_steps:
                status = "✅ ENABLED" if step in enabled_steps else "⭕ DISABLED"
                print(f"  {step:<25} {status}")
            print(f"\nTotal enabled: {len(enabled_steps)}/{len(all_steps)}")

        # Show dependencies
        dependencies = config_manager.get_value("workflow.step_dependencies", {})
        if dependencies:
            if terminal_interface and TERMINAL_INTERFACE_AVAILABLE:
                terminal_interface.print_section("📋 Step Dependencies")
                for step, deps in dependencies.items():
                    if deps:
                        terminal_interface.print_info(f"{step} depends on: {', '.join(deps)}")
                    else:
                        terminal_interface.print_info(f"{step} has no dependencies")
            else:
                print("\nStep Dependencies:")
                for step, deps in dependencies.items():
                    if deps:
                        print(f"  {step} depends on: {', '.join(deps)}")
                    else:
                        print(f"  {step} has no dependencies")

        return 0

    except Exception as workflow_error:
        if terminal_interface and TERMINAL_INTERFACE_AVAILABLE:
            terminal_interface.print_error(f"Error reading workflow configuration: {workflow_error}")
        else:
            print(f"❌ Error reading workflow configuration: {workflow_error}")
        return 1


def run_application_workflow(config_path: str, args: argparse.Namespace) -> bool:
    """Run the main application workflow using existing bootstrap."""
    global app_container
    progress = None

    try:
        if terminal_interface and TERMINAL_INTERFACE_AVAILABLE:
            terminal_interface.print_section("🚀 Initializing Application")
        else:
            print("\n=== Initializing Application ===")

        # Initialize application container (existing)
        if terminal_interface and TERMINAL_INTERFACE_AVAILABLE:
            progress = terminal_interface.create_progress_tracker()
            progress.start(4, "Initializing application container...")

            app_container = ApplicationContainer(config_path)
            progress.update(1)

            # Get workflow engine (existing)
            workflow_engine = app_container.get_workflow_engine()
            progress.update(2)

            # Determine steps to execute
            steps_to_execute = None
            if args.step:
                steps_to_execute = [args.step]
            progress.update(3)

            # Display workflow status
            workflow_status = workflow_engine.get_workflow_status()
            enabled_steps = workflow_status.get('enabled_steps', [])
            progress.update(4)
            progress.finish(True)
        else:
            print("Initializing application container...")
            app_container = ApplicationContainer(config_path)
            workflow_engine = app_container.get_workflow_engine()
            steps_to_execute = None
            if args.step:
                steps_to_execute = [args.step]
            workflow_status = workflow_engine.get_workflow_status()
            enabled_steps = workflow_status.get('enabled_steps', [])

        if steps_to_execute:
            if terminal_interface and TERMINAL_INTERFACE_AVAILABLE:
                terminal_interface.print_info(f"Executing specific step: {args.step}")
            else:
                print(f"Executing specific step: {args.step}")
        else:
            if terminal_interface and TERMINAL_INTERFACE_AVAILABLE:
                terminal_interface.print_info(f"Executing workflow with {len(enabled_steps)} enabled steps")
            else:
                print(f"Executing workflow with {len(enabled_steps)} enabled steps")

        # Execute workflow (existing workflow engine)
        if terminal_interface and TERMINAL_INTERFACE_AVAILABLE:
            terminal_interface.print_section("⚙️ Workflow Execution")
        else:
            print("\n=== Workflow Execution ===")

        start_time = datetime.now()
        result = workflow_engine.execute_workflow(steps_to_execute)
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        # Report results
        if terminal_interface and TERMINAL_INTERFACE_AVAILABLE:
            terminal_interface.print_section("📊 Execution Results")

            result_data = {
                "Success": result.success,
                "Execution Time": f"{duration:.1f}s",
                "Items Processed": result.items_processed,
                "Items Failed": result.items_failed
            }

            if result.success:
                terminal_interface.print_results_table("Workflow Results", result_data)

                # Display generated files
                if 'step_results' in result.data:
                    terminal_interface.print_section("📄 Generated Files")
                    for step, step_result in result.data['step_results'].items():
                        if 'output_file' in step_result:
                            output_file = step_result['output_file']
                            terminal_interface.print_success(f"{step.title()}: {output_file}")
            else:
                result_data["Error Count"] = len(result.errors)
                terminal_interface.print_results_table("Workflow Results (Failed)", result_data)

                terminal_interface.print_error("Workflow completed with errors:")
                for error in result.errors:
                    terminal_interface.print_error(f"  • {error}")
        else:
            print("\n=== Execution Results ===")
            if result.success:
                print(f"🎉 Workflow completed successfully!")
                print(f"⏱️  Total execution time: {duration:.2f} seconds")
                print(f"✅ Items processed: {result.items_processed}")

                if 'step_results' in result.data:
                    print("📄 Generated files:")
                    for step, step_result in result.data['step_results'].items():
                        if 'output_file' in step_result:
                            print(f"   {step}: {step_result['output_file']}")
            else:
                print("❌ Workflow completed with errors!")
                print(f"⏱️  Execution time: {duration:.2f} seconds")
                print(f"❌ Items failed: {result.items_failed}")
                for error in result.errors:
                    print(f"   📋 {error}")

        return result.success

    except KeyboardInterrupt:
        if terminal_interface and TERMINAL_INTERFACE_AVAILABLE:
            terminal_interface.print_warning("Workflow interrupted by user")
        else:
            print("⏹️  Workflow interrupted by user")
        if progress:
            progress.finish(False)
        return False
    except Exception as workflow_exception:
        handle_exception_with_traceback(workflow_exception, "Unexpected error during workflow")
        if progress:
            progress.finish(False)
        return False
    finally:
        # Cleanup
        if app_container:
            app_container.shutdown()


def main() -> int:
    """Main entry point - enhanced but working with existing structure."""
    global terminal_interface

    # Initialize terminal interface early (if available)
    if TERMINAL_INTERFACE_AVAILABLE:
        terminal_interface = get_terminal_interface()

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
  %(prog)s --debug                  # Enable debug output
  %(prog)s -q                       # Quiet mode (minimal output)
  %(prog)s --silent                 # Silent mode (no output)
        """
    )

    parser.add_argument("-c", "--config", default="config.yaml", help="Path to configuration file")
    parser.add_argument("--create-config", action="store_true", help="Create a sample configuration file and exit")
    parser.add_argument("--validate-config", action="store_true", help="Validate configuration file and exit")
    parser.add_argument("--list-steps", action="store_true", help="List workflow steps and their configuration")
    parser.add_argument("--step", choices=["applications", "marketing_descriptions", "popularity_codes", "sdc_template",
                                           "validation_reports"], help="Run only a specific workflow step")
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose output")
    parser.add_argument("--debug", action="store_true", help="Enable debug output")
    parser.add_argument("-q", "--quiet", action="store_true", help="Quiet mode (minimal output)")
    parser.add_argument("--silent", action="store_true", help="Silent mode (no output)")
    parser.add_argument("--dry-run", action="store_true", help="Validate setup without running processing")

    args = parser.parse_args()

    # Update terminal config based on args (if available)
    if terminal_interface and TERMINAL_INTERFACE_AVAILABLE:
        if args.debug:
            terminal_interface.config.log_level = LogLevel.DEBUG
        elif args.verbose:
            terminal_interface.config.log_level = LogLevel.VERBOSE
        elif args.quiet:
            terminal_interface.config.log_level = LogLevel.MINIMAL
        elif args.silent:
            terminal_interface.config.log_level = LogLevel.SILENT

    # Print header
    if terminal_interface and TERMINAL_INTERFACE_AVAILABLE:
        terminal_interface.print_header(
            "🚗 AUTOMOTIVE PARTS DATA PROCESSING",
            f"Configuration: {args.config} | Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )
    else:
        print("=" * 80)
        print("🚗 AUTOMOTIVE PARTS DATA PROCESSING APPLICATION")
        print("=" * 80)
        print(f"Configuration: {args.config}")
        print(f"Started at: {datetime.now().isoformat()}")

    # Handle special commands
    if args.create_config:
        return create_sample_configuration(args.config)

    if args.validate_config:
        return validate_configuration_file(args.config)

    if args.list_steps:
        return list_workflow_steps(args.config)

    # Check if config file exists
    if not Path(args.config).exists():
        if terminal_interface and TERMINAL_INTERFACE_AVAILABLE:
            terminal_interface.print_error(f"Configuration file not found: {args.config}")
            terminal_interface.print_info("Use --create-config to create a sample configuration file.")
        else:
            print(f"❌ Configuration file not found: {args.config}")
            print("💡 Use --create-config to create a sample configuration file.")
        return 1

    # Initialize configuration and logging
    try:
        config_manager = EnhancedConfigurationManager(args.config)

        initialize_jvm_once([
            config_manager.get_value("database.filemaker.fmjdbc_jar_path"),
            config_manager.get_value("database.iseries.jt400_jar_path"),
        ])

        if terminal_interface and TERMINAL_INTERFACE_AVAILABLE:
            terminal_interface.setup_logging(config_manager.get_value("files.log_file"))

        setup_signal_handlers()

        # Validate environment
        if not validate_environment(config_manager):
            return 1

        if args.dry_run:
            if terminal_interface and TERMINAL_INTERFACE_AVAILABLE:
                terminal_interface.print_success("Dry run completed - environment validation passed")
            else:
                print("✅ Dry run completed - environment validation passed")
            return 0

        # Run main workflow (using existing workflow engine)
        success = run_application_workflow(args.config, args)

        # Final status
        if success:
            if terminal_interface and TERMINAL_INTERFACE_AVAILABLE:
                terminal_interface.print_success("🎉 Application completed successfully!")
            else:
                print("🎉 Application completed successfully!")
        else:
            if terminal_interface and TERMINAL_INTERFACE_AVAILABLE:
                terminal_interface.print_error("❌ Application completed with errors")
            else:
                print("❌ Application completed with errors")

        if jpype.isJVMStarted():
            jpype.shutdownJVM()

        return 0 if success else 1

    except Exception as fatal_exception:
        handle_exception_with_traceback(fatal_exception, "Fatal error")
        return 1


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(1)
    except Exception as main_exception:
        traceback.print_exc()
        sys.exit(1)
    finally:
        if jpype.isJVMStarted():
            jpype.shutdownJVM()