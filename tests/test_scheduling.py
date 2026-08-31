"""Phase 5: cron-triggered runs.

A `schedule` on a workflow definition is validated at registration (same
principle as an invalid DAG — reject at the door, not at 3am when it fires),
armed immediately in the process-wide APScheduler instance, and re-armed from
the database on every API startup so it survives a restart.
"""

import uuid

import pytest

from app import cron
from app.models import Workflow, WorkflowRun

LINEAR = {
    "name": "daily_report",
    "workflow": [
        {"id": "a", "depends_on": []},
        {"id": "b", "depends_on": ["a"]},
    ],
}


def scheduled(cron_expr: str) -> dict:
    return {**LINEAR, "schedule": cron_expr}


# --- validation at registration ----------------------------------------------


def test_valid_cron_expression_is_accepted(client):
    response = client.post("/workflows", json=scheduled("0 9 * * *"))
    assert response.status_code == 201
    assert response.json()["schedule"] == "0 9 * * *"


def test_invalid_cron_expression_is_rejected(client):
    response = client.post("/workflows", json=scheduled("not a cron expression"))
    assert response.status_code == 422


def test_workflow_without_a_schedule_defaults_to_manual_only(client):
    response = client.post("/workflows", json=LINEAR)
    assert response.status_code == 201
    assert response.json()["schedule"] is None


def test_blank_schedule_is_treated_as_manual_only(client):
    response = client.post("/workflows", json=scheduled("   "))
    assert response.status_code == 201
    assert response.json()["schedule"] is None


@pytest.mark.parametrize(
    "expr", ["61 * * * *", "* * * *", "* * * * * *", ""]
)
def test_malformed_cron_fields_are_rejected(client, expr):
    payload = {**LINEAR, "schedule": expr}
    if expr == "":
        # empty string is treated as "no schedule", not malformed — covered above
        assert client.post("/workflows", json=payload).status_code == 201
    else:
        assert client.post("/workflows", json=payload).status_code == 422


# --- cron.py unit behaviour ---------------------------------------------------


def test_validate_cron_accepts_a_five_field_expression():
    cron.validate_cron("0 9 * * *")  # does not raise


def test_validate_cron_rejects_garbage():
    with pytest.raises(ValueError):
        cron.validate_cron("definitely not cron")
