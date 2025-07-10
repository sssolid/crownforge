# src/application/orchestration/workflow_engine.py
"""
Workflow orchestration engine with dependency management and error handling.
"""

import logging
from typing import List, Dict, Any, Optional, Set
from dataclasses import dataclass
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

from ...domain.models import WorkflowStep, WorkflowExecution, ProcessingResult
from ...domain.interfaces import WorkflowEngine, EventPublisher

logger = logging.getLogger(__name__)


@dataclass
class WorkflowConfiguration:
    """Workflow execution configuration."""
    enabled_steps: List[str]
    step_definitions: Dict[str, WorkflowStep]
    max_parallel_steps: int = 3
    default_timeout_minutes: int = 30
    continue_on_error: bool = True
    retry_failed_steps: bool = False
    max_retries: int = 2


class WorkflowOrchestrationEngine(WorkflowEngine):
    """Main workflow orchestration engine."""

    def __init__(
            self,
            config: WorkflowConfiguration,
            event_publisher: Optional[EventPublisher] = None
    ):
        self.config = config
        self.event_publisher = event_publisher
        self._step_executors: Dict[str, callable] = {}
        self._execution_lock = threading.Lock()
        self._current_execution: Optional[WorkflowExecution] = None

    def register_step_executor(self, step_name: str, executor: callable) -> None:
        """Register executor function for a workflow step."""
        self._step_executors[step_name] = executor
        logger.debug(f"Registered executor for step: {step_name}")

    def execute_workflow(self, requested_steps: Optional[List[str]] = None) -> ProcessingResult:
        """Execute workflow with dependency resolution."""
        with self._execution_lock:
            if self._current_execution and self._current_execution.is_running():
                return ProcessingResult(
                    success=False,
                    errors=["Workflow is already running"]
                )

            # Determine steps to execute
            steps_to_execute = requested_steps or self.config.enabled_steps
            execution_plan = self._create_execution_plan(steps_to_execute)

            # Initialize execution tracking
            self._current_execution = WorkflowExecution(
                started_at=datetime.now()
            )

            try:
                logger.info(f"Starting workflow execution with {len(execution_plan)} steps")

                # Execute steps according to plan
                self._execute_steps_by_plan(execution_plan)

                # Finalize execution
                self._current_execution.completed_at = datetime.now()
                self._current_execution.overall_success = len(self._current_execution.failed_steps) == 0

                return self._create_workflow_result()

            except Exception as e:
                logger.error(f"Workflow execution failed: {e}")
                self._current_execution.completed_at = datetime.now()
                self._current_execution.overall_success = False

                return ProcessingResult(
                    success=False,
                    errors=[f"Workflow execution failed: {e}"],
                    execution_time_seconds=self._current_execution.get_duration_seconds()
                )

    def get_workflow_status(self) -> Dict[str, Any]:
        """Get current workflow status."""
        if not self._current_execution:
            return {
                'status': 'idle',
                'enabled_steps': self.config.enabled_steps,
                'registered_executors': list(self._step_executors.keys())
            }

        return {
            'status': 'running' if self._current_execution.is_running() else 'completed',
            'started_at': self._current_execution.started_at.isoformat(),
            'completed_steps': self._current_execution.completed_steps,
            'failed_steps': self._current_execution.failed_steps,
            'overall_success': self._current_execution.overall_success,
            'duration_seconds': self._current_execution.get_duration_seconds(),
            'success_rate': self._current_execution.get_overall_success_rate()
        }

    def _create_execution_plan(self, requested_steps: List[str]) -> List[List[str]]:
        """Create execution plan with dependency resolution."""
        # Validate requested steps
        invalid_steps = [step for step in requested_steps if step not in self.config.step_definitions]
        if invalid_steps:
            raise ValueError(f"Unknown workflow steps: {invalid_steps}")

        # Build dependency graph
        dependency_graph = {}
        for step_name in requested_steps:
            step_def = self.config.step_definitions[step_name]
            dependency_graph[step_name] = step_def.dependencies

        # Resolve dependencies and create execution levels
        execution_plan = []
        remaining_steps = set(requested_steps)
        completed_steps = set()

        while remaining_steps:
            # Find steps that can be executed (dependencies satisfied)
            ready_steps = []
            for step in remaining_steps:
                dependencies = dependency_graph[step]
                if all(dep in completed_steps for dep in dependencies):
                    ready_steps.append(step)

            if not ready_steps:
                # Circular dependency or missing dependency
                raise ValueError(f"Cannot resolve dependencies for remaining steps: {remaining_steps}")

            execution_plan.append(ready_steps)
            remaining_steps -= set(ready_steps)
            completed_steps.update(ready_steps)

        logger.info(f"Created execution plan with {len(execution_plan)} levels: {execution_plan}")
        return execution_plan

    def _execute_steps_by_plan(self, execution_plan: List[List[str]]) -> None:
        """Execute steps according to execution plan."""
        for level_index, steps_in_level in enumerate(execution_plan):
            logger.info(f"Executing level {level_index + 1}: {steps_in_level}")

            if len(steps_in_level) == 1 or self.config.max_parallel_steps == 1:
                # Execute sequentially
                for step_name in steps_in_level:
                    self._execute_single_step(step_name)
            else:
                # Execute in parallel
                self._execute_steps_in_parallel(steps_in_level)

            # Check if we should continue after failures
            if (self._current_execution.failed_steps and
                    not self.config.continue_on_error):
                logger.warning("Stopping workflow execution due to failures")
                break

    def _execute_steps_in_parallel(self, step_names: List[str]) -> None:
        """Execute multiple steps in parallel."""
        max_workers = min(len(step_names), self.config.max_parallel_steps)

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all steps
            future_to_step = {
                executor.submit(self._execute_single_step, step_name): step_name
                for step_name in step_names
            }

            # Wait for completion
            for future in as_completed(future_to_step):
                step_name = future_to_step[future]
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Parallel step {step_name} failed: {e}")

    def _execute_single_step(self, step_name: str) -> None:
        """Execute a single workflow step."""
        if step_name not in self._step_executors:
            error_msg = f"No executor registered for step: {step_name}"
            logger.error(error_msg)
            self._current_execution.failed_steps.append(step_name)
            return

        step_def = self.config.step_definitions[step_name]
        executor = self._step_executors[step_name]

        # Publish step started event
        if self.event_publisher:
            self.event_publisher.publish_step_started(step_name)

        logger.info(f"Executing step: {step_name}")
        start_time = datetime.now()

        try:
            # Execute with timeout if configured
            if step_def.timeout_seconds:
                result = self._execute_with_timeout(executor, step_def.timeout_seconds)
            else:
                result = executor()

            # Record successful execution
            execution_time = (datetime.now() - start_time).total_seconds()
            self._current_execution.completed_steps.append(step_name)
            self._current_execution.step_results[step_name] = result

            logger.info(f"Step {step_name} completed successfully in {execution_time:.2f}s")

            # Publish step completed event
            if self.event_publisher:
                self.event_publisher.publish_step_completed(step_name, result)

        except Exception as e:
            error_msg = f"Step {step_name} failed: {e}"
            logger.error(error_msg)

            self._current_execution.failed_steps.append(step_name)

            # Create failed result
            failed_result = ProcessingResult(
                success=False,
                errors=[error_msg],
                execution_time_seconds=(datetime.now() - start_time).total_seconds()
            )
            self._current_execution.step_results[step_name] = failed_result

            # Publish step failed event
            if self.event_publisher:
                self.event_publisher.publish_step_failed(step_name, error_msg)

    def _execute_with_timeout(self, executor: callable, timeout_seconds: int) -> ProcessingResult:
        """Execute step with timeout."""
        with ThreadPoolExecutor(max_workers=1) as timeout_executor:
            future = timeout_executor.submit(executor)
            try:
                return future.result(timeout=timeout_seconds)
            except TimeoutError:
                raise RuntimeError(f"Step execution timed out after {timeout_seconds} seconds")

    def _create_workflow_result(self) -> ProcessingResult:
        """Create final workflow processing result."""
        total_steps = len(self._current_execution.completed_steps) + len(self._current_execution.failed_steps)

        return ProcessingResult(
            success=self._current_execution.overall_success,
            items_processed=len(self._current_execution.completed_steps),
            items_failed=len(self._current_execution.failed_steps),
            data={
                'completed_steps': self._current_execution.completed_steps,
                'failed_steps': self._current_execution.failed_steps,
                'step_results': {
                    step: result.data if hasattr(result, 'data') else {}
                    for step, result in self._current_execution.step_results.items()
                },
                'execution_summary': {
                    'total_steps': total_steps,
                    'success_rate': self._current_execution.get_overall_success_rate(),
                    'duration_seconds': self._current_execution.get_duration_seconds()
                }
            },
            execution_time_seconds=self._current_execution.get_duration_seconds()
        )
