# src/infrastructure/terminal/terminal_interface.py
"""
Terminal interface with colored output and progress bars for existing main.py.
"""

import sys
import time
import logging
from typing import Optional, Dict, Any
from enum import Enum
from dataclasses import dataclass

from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn, TimeElapsedColumn, \
    TaskID
from rich.table import Table
from rich.panel import Panel
from rich.text import Text
from rich.logging import RichHandler
from rich.traceback import install as install_rich_traceback

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
    """Enhanced terminal interface that works with existing main.py."""

    def __init__(self, config: TerminalConfig = None):
        self.config = config or TerminalConfig()
        self.console = Console()

        # Install rich traceback for better error display
        install_rich_traceback(show_locals=self.config.log_level == LogLevel.DEBUG)

    def setup_logging(self, log_file: Optional[str] = None) -> None:
        """Setup logging with rich formatting."""
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

        # Console handler with rich formatting
        if self.config.log_level != LogLevel.SILENT:
            console_handler = RichHandler(
                console=self.console,
                show_time=self.config.show_timestamps,
                show_path=self.config.log_level == LogLevel.DEBUG,
                rich_tracebacks=True
            )
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

        header_text = Text(title, style="bold blue")
        if subtitle:
            header_text.append(f"\n{subtitle}", style="italic")

        panel = Panel(header_text, border_style="blue", padding=(1, 2))
        self.console.print(panel)
        self.console.print()

    def print_section(self, title: str, style: str = "bold cyan") -> None:
        """Print section header."""
        if self.config.log_level == LogLevel.SILENT:
            return
        self.console.print(f"\n[{style}]{title}[/{style}]")

    def print_success(self, message: str) -> None:
        """Print success message."""
        if self.config.log_level != LogLevel.SILENT:
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
            self.console.print(f"[red]❌ {message}{location}[/red]")
        else:
            print(f"❌ {message}{location}")

    def print_warning(self, message: str) -> None:
        """Print warning message."""
        if self.config.log_level != LogLevel.SILENT:
            self.console.print(f"[yellow]⚠️  {message}[/yellow]")
        else:
            print(f"⚠️  {message}")

    def print_info(self, message: str) -> None:
        """Print info message."""
        if self.config.log_level in [LogLevel.VERBOSE, LogLevel.DEBUG]:
            self.console.print(f"[blue]ℹ️  {message}[/blue]")

    def print_results_table(self, title: str, data: Dict[str, Any]) -> None:
        """Print results in a formatted table."""
        if self.config.log_level == LogLevel.SILENT:
            return

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

    def create_progress_tracker(self) -> 'RichProgressTracker':
        """Create a progress tracker."""
        return RichProgressTracker(self)


class RichProgressTracker(ProgressTracker):
    """Progress tracker using Rich library."""

    def __init__(self, terminal: TerminalInterface):
        self.terminal = terminal
        self.progress: Optional[Progress] = None
        self.task_id: Optional[TaskID] = None

    def start(self, total_items: int, description: str = "") -> None:
        """Start progress tracking."""
        if self.terminal.config.log_level == LogLevel.SILENT:
            return

        if not self.terminal.config.show_progress_bars:
            self.terminal.print_info(f"Starting: {description}")
            return

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
        use_colors=True,
        show_progress_bars=True,
        show_timestamps=log_level == LogLevel.DEBUG
    )

    return TerminalInterface(config)