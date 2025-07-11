# src/infrastructure/terminal/terminal_interface.py
"""
Terminal interface with colored output and progress bars, with fallback support.
"""

import sys
import time
import logging
from typing import Optional, Dict, Any
from enum import Enum
from dataclasses import dataclass

try:
    from rich.console import Console
    from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn, TimeElapsedColumn, \
        TaskID
    from rich.table import Table
    from rich.panel import Panel
    from rich.text import Text
    from rich.logging import RichHandler
    from rich.traceback import install as install_rich_traceback

    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False
    Console = None
    Progress = None
    Table = None
    Panel = None
    Text = None
    RichHandler = None
    install_rich_traceback = None

from ...domain.interfaces import ProgressTracker


class LogLevel(Enum):
    """Log level enumeration."""
    SILENT = 0
    MINIMAL = 1
    NORMAL = 2
    VERBOSE = 3
    DEBUG = 4


@dataclass
class TerminalConfig:
    """Terminal interface configuration."""
    log_level: LogLevel = LogLevel.NORMAL
    use_colors: bool = True
    show_progress_bars: bool = True
    show_timestamps: bool = False


class TerminalInterface:
    """Enhanced terminal interface with fallback for environments without rich."""

    def __init__(self, config: TerminalConfig = None):
        self.config = config or TerminalConfig()
        self.console = Console() if RICH_AVAILABLE else None

        # Install rich traceback for better error display if available
        if RICH_AVAILABLE and install_rich_traceback:
            install_rich_traceback(show_locals=self.config.log_level == LogLevel.DEBUG)

    def setup_logging(self, log_file: Optional[str] = None) -> None:
        """Setup logging with rich formatting if available."""
        # Clear existing handlers
        root_logger = logging.getLogger()
        root_logger.handlers.clear()

        # Set log level based on config
        if self.config.log_level == LogLevel.SILENT:
            root_logger.setLevel(logging.CRITICAL)
        elif self.config.log_level == LogLevel.MINIMAL:
            root_logger.setLevel(logging.ERROR)
        elif self.config.log_level == LogLevel.NORMAL:
            root_logger.setLevel(logging.WARNING)
        elif self.config.log_level == LogLevel.VERBOSE:
            root_logger.setLevel(logging.INFO)
        else:  # DEBUG
            root_logger.setLevel(logging.DEBUG)

        # Console handler with rich formatting if available
        if self.config.log_level != LogLevel.SILENT:
            if RICH_AVAILABLE and self.console:
                console_handler = RichHandler(
                    console=self.console,
                    show_time=self.config.show_timestamps,
                    show_path=self.config.log_level == LogLevel.DEBUG,
                    rich_tracebacks=True
                )
            else:
                console_handler = logging.StreamHandler()
                formatter = logging.Formatter(
                    '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
                )
                console_handler.setFormatter(formatter)

            console_handler.setLevel(root_logger.level)
            root_logger.addHandler(console_handler)

        # File handler if specified
        if log_file:
            from pathlib import Path
            Path(log_file).parent.mkdir(parents=True, exist_ok=True)
            file_handler = logging.FileHandler(log_file, mode='a', encoding='utf-8')
            file_handler.setLevel(logging.DEBUG)
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
            )
            file_handler.setFormatter(formatter)
            root_logger.addHandler(file_handler)

    def print_header(self, title: str, subtitle: Optional[str] = None) -> None:
        """Print application header."""
        if self.config.log_level == LogLevel.SILENT:
            return

        if RICH_AVAILABLE and self.console:
            header_text = Text(title, style="bold blue")
            if subtitle:
                header_text.append(f"\n{subtitle}", style="italic")

            panel = Panel(header_text, border_style="blue", padding=(1, 2))
            self.console.print(panel)
            self.console.print()
        else:
            print("=" * 80)
            print(title)
            if subtitle:
                print(subtitle)
            print("=" * 80)

    def print_section(self, title: str, style: str = "bold cyan") -> None:
        """Print section header."""
        if self.config.log_level == LogLevel.SILENT:
            return

        if RICH_AVAILABLE and self.console:
            self.console.print(f"\n[{style}]{title}[/{style}]")
        else:
            print(f"\n{title}")

    def print_success(self, message: str) -> None:
        """Print success message."""
        if self.config.log_level != LogLevel.SILENT:
            if RICH_AVAILABLE and self.console:
                self.console.print(f"[green]✅ {message}[/green]")
            else:
                print(f"✅ {message}")

    def print_error(self, message: str, file_path: str = None, line_number: int = None) -> None:
        """Print error message with optional file location."""
        location = ""
        if file_path and line_number:
            from pathlib import Path
            location = f" [{Path(file_path).name}:{line_number}]"

        if self.config.log_level != LogLevel.SILENT:
            if RICH_AVAILABLE and self.console:
                self.console.print(f"[red]❌ {message}{location}[/red]")
            else:
                print(f"❌ {message}{location}")

    def print_warning(self, message: str) -> None:
        """Print warning message."""
        if self.config.log_level != LogLevel.SILENT:
            if RICH_AVAILABLE and self.console:
                self.console.print(f"[yellow]⚠️  {message}[/yellow]")
            else:
                print(f"⚠️  {message}")

    def print_info(self, message: str) -> None:
        """Print info message."""
        if self.config.log_level in [LogLevel.VERBOSE, LogLevel.DEBUG]:
            if RICH_AVAILABLE and self.console:
                self.console.print(f"[blue]ℹ️  {message}[/blue]")
            else:
                print(f"ℹ️  {message}")

    def print_results_table(self, title: str, data: Dict[str, Any]) -> None:
        """Print results in a formatted table."""
        if self.config.log_level == LogLevel.SILENT:
            return

        if RICH_AVAILABLE and self.console:
            table = Table(title=title, show_header=True, header_style="bold magenta")
            table.add_column("Metric", style="cyan", no_wrap=True)
            table.add_column("Value", style="green")

            for key, value in data.items():
                display_key = key.replace('_', ' ').title()

                if isinstance(value, float):
                    display_value = f"{value:.2f}"
                elif isinstance(value, bool):
                    display_value = "✅" if value else "❌"
                else:
                    display_value = str(value)

                table.add_row(display_key, display_value)

            self.console.print(table)
        else:
            # Fallback to simple table format
            print(f"\n{title}")
            print("-" * len(title))
            for key, value in data.items():
                display_key = key.replace('_', ' ').title()
                if isinstance(value, bool):
                    display_value = "✅" if value else "❌"
                else:
                    display_value = str(value)
                print(f"{display_key:<25} {display_value}")

    def create_progress_tracker(self) -> 'ProgressTracker':
        """Create a progress tracker."""
        if RICH_AVAILABLE:
            return RichProgressTracker(self)
        else:
            return FallbackProgressTracker(self)


class RichProgressTracker(ProgressTracker):
    """Progress tracker using Rich library."""

    def __init__(self, terminal: TerminalInterface):
        self.terminal = terminal
        self.progress: Optional[Progress] = None
        self.task_id: Optional[int] = None

    def start(self, total_items: int, description: str = "") -> None:
        """Start progress tracking."""
        if self.terminal.config.log_level == LogLevel.SILENT:
            return

        if not self.terminal.config.show_progress_bars:
            self.terminal.print_info(f"Starting: {description}")
            return

        if RICH_AVAILABLE and self.terminal.console:
            self.progress = Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TaskProgressColumn(),
                TimeElapsedColumn(),
                console=self.terminal.console
            )

            self.progress.start()
            self.task_id = self.progress.add_task(description, total=total_items)

    def update(self, items_completed: int) -> None:
        """Update progress."""
        if self.progress and self.task_id is not None:
            self.progress.update(self.task_id, completed=items_completed)

    def set_description(self, description: str) -> None:
        """Set current operation description."""
        if self.progress and self.task_id is not None:
            self.progress.update(self.task_id, description=description)

    def finish(self, success: bool = True) -> None:
        """Finish progress tracking."""
        if self.progress:
            if self.task_id is not None:
                if success:
                    self.progress.update(self.task_id, description="✅ Completed")
                else:
                    self.progress.update(self.task_id, description="❌ Failed")

            time.sleep(0.5)  # Brief pause to show completion
            self.progress.stop()
            self.progress = None
            self.task_id = None


class FallbackProgressTracker(ProgressTracker):
    """Fallback progress tracker for environments without Rich."""

    def __init__(self, terminal: TerminalInterface):
        self.terminal = terminal
        self.current_description = ""
        self.total_items = 0
        self.completed_items = 0

    def start(self, total_items: int, description: str = "") -> None:
        """Start progress tracking."""
        self.total_items = total_items
        self.current_description = description
        self.completed_items = 0
        if self.terminal.config.log_level != LogLevel.SILENT:
            print(f"Starting: {description}")

    def update(self, items_completed: int) -> None:
        """Update progress."""
        self.completed_items = items_completed
        if self.terminal.config.log_level in [LogLevel.VERBOSE, LogLevel.DEBUG]:
            percentage = (items_completed / self.total_items * 100) if self.total_items > 0 else 0
            print(f"Progress: {items_completed}/{self.total_items} ({percentage:.1f}%)")

    def set_description(self, description: str) -> None:
        """Set current operation description."""
        self.current_description = description
        if self.terminal.config.log_level in [LogLevel.VERBOSE, LogLevel.DEBUG]:
            print(f"Operation: {description}")

    def finish(self, success: bool = True) -> None:
        """Finish progress tracking."""
        if self.terminal.config.log_level != LogLevel.SILENT:
            status = "✅ Completed" if success else "❌ Failed"
            print(f"{status}: {self.current_description}")


def get_terminal_interface() -> TerminalInterface:
    """Get terminal interface instance with appropriate configuration."""
    # Determine log level from command line args or environment
    log_level = LogLevel.NORMAL

    if '--verbose' in sys.argv or '-v' in sys.argv:
        log_level = LogLevel.VERBOSE
    elif '--debug' in sys.argv:
        log_level = LogLevel.DEBUG
    elif '--quiet' in sys.argv or '-q' in sys.argv:
        log_level = LogLevel.MINIMAL
    elif '--silent' in sys.argv:
        log_level = LogLevel.SILENT

    config = TerminalConfig(
        log_level=log_level,
        use_colors=RICH_AVAILABLE,
        show_progress_bars=RICH_AVAILABLE,
        show_timestamps=log_level == LogLevel.DEBUG
    )

    return TerminalInterface(config)