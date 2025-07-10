# src/infrastructure/database/jvm_initializer.py
from .connection_manager import JvmManager

def initialize_jvm_once(jar_paths: list[str]) -> None:
    """Initialize JVM with all required JDBC JARs."""
    jvm = JvmManager()
    for jar in jar_paths:
        jvm.add_jar_path(jar)
    jvm.start_jvm()