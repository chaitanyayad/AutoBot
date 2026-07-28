"""DAG validation and resolution.

Two responsibilities:

1. `validate_definition` — reject bad graphs at registration time (duplicate ids,
   dangling dependencies, self-edges, cycles) so a run can never be created from a
   workflow that cannot finish.
2. `get_runnable_tasks` — the scheduler primitive. Given the current state of a
   run's tasks, return the ones whose dependencies are all satisfied.
"""

from collections import deque
from dataclasses import dataclass

from app.models import (
    TASK_FAILED,
    TASK_PENDING,
    TASK_QUEUED,
    TASK_RUNNING,
    TASK_SUCCESS,
    Task,
)


class DagValidationError(ValueError):
    """Raised when a workflow definition is not a valid DAG."""


@dataclass(frozen=True)
class TaskSpec:
    """One node of a workflow definition."""

    id: str
    depends_on: tuple[str, ...]
    max_retries: int | None = None


def parse_definition(definition: dict) -> list[TaskSpec]:
    """Turn the raw definition JSON into TaskSpecs. Does not validate the graph."""
    nodes = definition.get("workflow")
    if not isinstance(nodes, list) or not nodes:
        raise DagValidationError("definition.workflow must be a non-empty list")

    specs: list[TaskSpec] = []
    for index, node in enumerate(nodes):
        if not isinstance(node, dict):
            raise DagValidationError(f"workflow[{index}] must be an object")
        task_id = node.get("id")
        if not isinstance(task_id, str) or not task_id.strip():
            raise DagValidationError(f"workflow[{index}].id must be a non-empty string")

        raw_deps = node.get("depends_on", [])
        if not isinstance(raw_deps, list) or not all(
            isinstance(dep, str) for dep in raw_deps
        ):
            raise DagValidationError(
                f"workflow[{index}].depends_on must be a list of task ids"
            )

        max_retries = node.get("max_retries")
        if max_retries is not None and (
            not isinstance(max_retries, int)
            or isinstance(max_retries, bool)
            or max_retries < 0
        ):
            raise DagValidationError(
                f"workflow[{index}].max_retries must be a non-negative integer"
            )

        specs.append(
            TaskSpec(
                id=task_id,
                depends_on=tuple(dict.fromkeys(raw_deps)),  # de-dupe, keep order
                max_retries=max_retries,
            )
        )
    return specs


def validate_definition(definition: dict) -> list[TaskSpec]:
    """Parse and fully validate a definition. Returns the parsed specs."""
    specs = parse_definition(definition)

    seen: set[str] = set()
    for spec in specs:
        if spec.id in seen:
            raise DagValidationError(f"duplicate task id: {spec.id!r}")
        seen.add(spec.id)

    for spec in specs:
        if spec.id in spec.depends_on:
            raise DagValidationError(f"task {spec.id!r} depends on itself")
        for dep in spec.depends_on:
            if dep not in seen:
                raise DagValidationError(
                    f"task {spec.id!r} depends on unknown task {dep!r}"
                )

    # Cycle detection via Kahn's algorithm: if the topological sort cannot consume
    # every node, the leftovers form at least one cycle.
    topological_order(specs)
    return specs


def topological_order(specs: list[TaskSpec]) -> list[str]:
    """Kahn's algorithm. Raises DagValidationError if the graph contains a cycle.

    Ties are broken by definition order, so the result is deterministic.
    """
    indegree = {spec.id: len(spec.depends_on) for spec in specs}
    dependents: dict[str, list[str]] = {spec.id: [] for spec in specs}
    for spec in specs:
        for dep in spec.depends_on:
            dependents[dep].append(spec.id)

    order_index = {spec.id: i for i, spec in enumerate(specs)}
    ready = deque(sorted((s.id for s in specs if indegree[s.id] == 0), key=order_index.get))

    ordered: list[str] = []
    while ready:
        node = ready.popleft()
        ordered.append(node)
        newly_ready = []
        for dependent in dependents[node]:
            indegree[dependent] -= 1
            if indegree[dependent] == 0:
                newly_ready.append(dependent)
        for dependent in sorted(newly_ready, key=order_index.get):
            ready.append(dependent)

    if len(ordered) != len(specs):
        stuck = sorted(set(order_index) - set(ordered))
        raise DagValidationError(f"workflow contains a cycle involving: {stuck}")
    return ordered


def execution_levels(specs: list[TaskSpec]) -> list[list[str]]:
    """Group tasks into waves that could run in parallel.

    Not used by the scheduler (which is event-driven off task completions) but it
    makes the resolution order easy to inspect and assert on.
    """
    depth: dict[str, int] = {}
    for task_id in topological_order(specs):
        spec = next(s for s in specs if s.id == task_id)
        depth[task_id] = (
            0 if not spec.depends_on else 1 + max(depth[d] for d in spec.depends_on)
        )

    levels: list[list[str]] = [[] for _ in range(max(depth.values(), default=-1) + 1)]
    for spec in specs:  # definition order within a level
        levels[depth[spec.id]].append(spec.id)
    return levels


def get_runnable_tasks(tasks: list[Task]) -> list[Task]:
    """Return the pending tasks whose dependencies have all succeeded.

    This is the scheduler's core call: run it after every task completion and push
    whatever comes back onto the queue. It is deliberately pure — it takes the
    task rows and returns task rows, so it is trivial to test and callable from
    both the API and the worker.
    """
    completed = {t.task_name for t in tasks if t.status == TASK_SUCCESS}

    runnable: list[Task] = []
    for task in tasks:
        if task.status != TASK_PENDING:
            continue
        if all(dep in completed for dep in (task.depends_on or [])):
            runnable.append(task)
    return runnable


def is_run_finished(tasks: list[Task]) -> bool:
    """A run is finished when no task can still make progress.

    A failure alone is not enough: tasks already handed to a worker must be
    allowed to report back first, otherwise their results land on a run that has
    already been closed out.
    """
    if any(t.status in (TASK_QUEUED, TASK_RUNNING) for t in tasks):
        return False  # work is still in flight
    if all(t.status == TASK_SUCCESS for t in tasks):
        return True
    # Nothing in flight — finished iff nothing further can be dispatched, i.e.
    # whatever is still pending is permanently blocked by an upstream failure.
    return not get_runnable_tasks(tasks)


def is_run_failed(tasks: list[Task]) -> bool:
    return any(t.status == TASK_FAILED for t in tasks)
