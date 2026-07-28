"""Unit tests for DAG validation and resolution."""

import pytest

from app.dag import (
    DagValidationError,
    execution_levels,
    get_runnable_tasks,
    is_run_failed,
    is_run_finished,
    topological_order,
    validate_definition,
)
from app.models import (
    TASK_FAILED,
    TASK_PENDING,
    TASK_QUEUED,
    TASK_RUNNING,
    TASK_SUCCESS,
    Task,
)

RESUME_PIPELINE = {
    "name": "resume_pipeline",
    "workflow": [
        {"id": "parse_resume", "depends_on": []},
        {"id": "extract_skills", "depends_on": ["parse_resume"]},
        {"id": "generate_embeddings", "depends_on": ["parse_resume"]},
        {"id": "save_results", "depends_on": ["extract_skills", "generate_embeddings"]},
        {"id": "send_notification", "depends_on": ["save_results"]},
    ],
}


def make_task(name, depends_on=(), status=TASK_PENDING):
    return Task(task_name=name, depends_on=list(depends_on), status=status)


# --- validation -------------------------------------------------------------


def test_valid_definition_parses_all_nodes():
    specs = validate_definition(RESUME_PIPELINE)
    assert [s.id for s in specs] == [
        "parse_resume",
        "extract_skills",
        "generate_embeddings",
        "save_results",
        "send_notification",
    ]


def test_empty_workflow_rejected():
    with pytest.raises(DagValidationError, match="non-empty list"):
        validate_definition({"name": "x", "workflow": []})


def test_duplicate_task_id_rejected():
    definition = {"workflow": [{"id": "a"}, {"id": "a"}]}
    with pytest.raises(DagValidationError, match="duplicate task id"):
        validate_definition(definition)


def test_unknown_dependency_rejected():
    definition = {"workflow": [{"id": "a", "depends_on": ["ghost"]}]}
    with pytest.raises(DagValidationError, match="unknown task"):
        validate_definition(definition)


def test_self_dependency_rejected():
    definition = {"workflow": [{"id": "a", "depends_on": ["a"]}]}
    with pytest.raises(DagValidationError, match="depends on itself"):
        validate_definition(definition)


def test_cycle_rejected():
    definition = {
        "workflow": [
            {"id": "a", "depends_on": ["c"]},
            {"id": "b", "depends_on": ["a"]},
            {"id": "c", "depends_on": ["b"]},
        ]
    }
    with pytest.raises(DagValidationError, match="cycle"):
        validate_definition(definition)


def test_negative_max_retries_rejected():
    definition = {"workflow": [{"id": "a", "max_retries": -1}]}
    with pytest.raises(DagValidationError, match="max_retries"):
        validate_definition(definition)


# --- ordering ---------------------------------------------------------------


def test_topological_order_respects_dependencies():
    specs = validate_definition(RESUME_PIPELINE)
    order = topological_order(specs)
    position = {name: i for i, name in enumerate(order)}

    for spec in specs:
        for dep in spec.depends_on:
            assert position[dep] < position[spec.id]


def test_execution_levels_group_parallel_tasks():
    specs = validate_definition(RESUME_PIPELINE)
    assert execution_levels(specs) == [
        ["parse_resume"],
        ["extract_skills", "generate_embeddings"],
        ["save_results"],
        ["send_notification"],
    ]


# --- get_runnable_tasks -----------------------------------------------------


def test_only_roots_runnable_at_start():
    tasks = [
        make_task("parse_resume"),
        make_task("extract_skills", ["parse_resume"]),
        make_task("generate_embeddings", ["parse_resume"]),
    ]
    assert [t.task_name for t in get_runnable_tasks(tasks)] == ["parse_resume"]


def test_fan_out_after_root_succeeds():
    tasks = [
        make_task("parse_resume", status=TASK_SUCCESS),
        make_task("extract_skills", ["parse_resume"]),
        make_task("generate_embeddings", ["parse_resume"]),
        make_task("save_results", ["extract_skills", "generate_embeddings"]),
    ]
    assert [t.task_name for t in get_runnable_tasks(tasks)] == [
        "extract_skills",
        "generate_embeddings",
    ]


def test_fan_in_waits_for_all_dependencies():
    tasks = [
        make_task("parse_resume", status=TASK_SUCCESS),
        make_task("extract_skills", ["parse_resume"], status=TASK_SUCCESS),
        make_task("generate_embeddings", ["parse_resume"], status=TASK_RUNNING),
        make_task("save_results", ["extract_skills", "generate_embeddings"]),
    ]
    assert get_runnable_tasks(tasks) == []


def test_non_pending_tasks_are_never_returned():
    tasks = [make_task("a", status=TASK_QUEUED), make_task("b", status=TASK_SUCCESS)]
    assert get_runnable_tasks(tasks) == []


def test_failed_dependency_blocks_downstream():
    tasks = [
        make_task("a", status=TASK_FAILED),
        make_task("b", ["a"]),
    ]
    assert get_runnable_tasks(tasks) == []


# --- run completion ---------------------------------------------------------


def test_run_finished_when_all_succeed():
    tasks = [make_task("a", status=TASK_SUCCESS), make_task("b", ["a"], status=TASK_SUCCESS)]
    assert is_run_finished(tasks)
    assert not is_run_failed(tasks)


def test_run_finished_and_failed_on_any_failure():
    tasks = [make_task("a", status=TASK_FAILED), make_task("b", ["a"])]
    assert is_run_finished(tasks)
    assert is_run_failed(tasks)


def test_run_not_finished_while_work_remains():
    tasks = [make_task("a", status=TASK_SUCCESS), make_task("b", ["a"])]
    assert not is_run_finished(tasks)
