import os
from unittest.mock import patch

import httpx
import pytest
import respx

from airflow_mcp_server.airflow_client import AirflowClient, AirflowError

BASE_URL = "http://airflow.example.com/api/v2"


@pytest.fixture(autouse=True)
def airflow_env(monkeypatch):
    monkeypatch.setenv("AIRFLOW_API_URL", BASE_URL)


@pytest.fixture
def client():
    c = AirflowClient()
    # Pre-seed the Bearer token so individual tests don't need to mock gcloud.
    c._client.headers["Authorization"] = "Bearer test-token"
    return c


def _url(path: str) -> str:
    return BASE_URL + path


# ---------------------------------------------------------------------------
# authenticate()
# ---------------------------------------------------------------------------

async def test_authenticate_uses_gcloud_token():
    with patch("airflow_mcp_server.airflow_client._get_gcloud_token", return_value="gcloud-token"):
        c = AirflowClient()
        await c.authenticate()
        await c.aclose()
    assert c._client.headers["Authorization"] == "Bearer gcloud-token"


async def test_authenticate_raises_when_gcloud_fails():
    with patch(
        "airflow_mcp_server.airflow_client._get_gcloud_token",
        side_effect=RuntimeError("gcloud auth print-access-token failed: ERROR"),
    ):
        c = AirflowClient()
        with pytest.raises(RuntimeError, match="gcloud auth print-access-token failed"):
            await c.authenticate()
        await c.aclose()


# ---------------------------------------------------------------------------
# Bearer token forwarded on API requests
# ---------------------------------------------------------------------------

@respx.mock
async def test_bearer_token_header_sent(client):
    route = respx.get(_url("/dags")).mock(
        return_value=httpx.Response(200, json={"dags": [], "total_entries": 0})
    )
    await client.list_dags()
    assert route.calls[0].request.headers["authorization"] == "Bearer test-token"


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------

@respx.mock
async def test_raises_airflow_error_on_404(client):
    respx.get(_url("/dags/missing_dag")).mock(
        return_value=httpx.Response(404, json={"title": "DAG not found"})
    )
    with pytest.raises(AirflowError) as exc_info:
        await client.get_dag("missing_dag")
    assert exc_info.value.status_code == 404


@respx.mock
async def test_raises_airflow_error_on_5xx(client):
    respx.get(_url("/dags/my_dag")).mock(
        return_value=httpx.Response(500, text="Internal Server Error")
    )
    with pytest.raises(AirflowError) as exc_info:
        await client.get_dag("my_dag")
    assert exc_info.value.status_code == 500


# ---------------------------------------------------------------------------
# list_dags
# ---------------------------------------------------------------------------

@respx.mock
async def test_list_dags_default_params(client):
    route = respx.get(_url("/dags")).mock(
        return_value=httpx.Response(200, json={"dags": [{"dag_id": "my_dag"}], "total_entries": 1})
    )
    result = await client.list_dags()
    assert result["dags"] == [{"dag_id": "my_dag"}]
    params = route.calls[0].request.url.params
    assert params["limit"] == "20"
    assert "dag_id_pattern" not in params
    assert "only_active" not in params


@respx.mock
async def test_list_dags_with_pattern(client):
    route = respx.get(_url("/dags")).mock(
        return_value=httpx.Response(200, json={"dags": [], "total_entries": 0})
    )
    await client.list_dags(dag_id_pattern="etl")
    assert route.calls[0].request.url.params["dag_id_pattern"] == "etl"


@respx.mock
async def test_list_dags_only_active(client):
    route = respx.get(_url("/dags")).mock(
        return_value=httpx.Response(200, json={"dags": [], "total_entries": 0})
    )
    await client.list_dags(only_active=True)
    assert route.calls[0].request.url.params["only_active"] == "true"


@respx.mock
async def test_list_dags_custom_limit(client):
    route = respx.get(_url("/dags")).mock(
        return_value=httpx.Response(200, json={"dags": [], "total_entries": 0})
    )
    await client.list_dags(limit=5)
    assert route.calls[0].request.url.params["limit"] == "5"


# ---------------------------------------------------------------------------
# get_dag
# ---------------------------------------------------------------------------

@respx.mock
async def test_get_dag_returns_dag_object(client):
    dag_data = {
        "dag_id": "my_dag",
        "is_paused": False,
        "is_active": True,
        "schedule_interval": {"__type": "CronExpression", "value": "0 0 * * *"},
        "owners": ["airflow"],
    }
    respx.get(_url("/dags/my_dag")).mock(
        return_value=httpx.Response(200, json=dag_data)
    )
    result = await client.get_dag("my_dag")
    assert result["dag_id"] == "my_dag"
    assert result["is_paused"] is False
    assert result["is_active"] is True


@respx.mock
async def test_get_dag_paused(client):
    dag_data = {"dag_id": "paused_dag", "is_paused": True, "is_active": True}
    respx.get(_url("/dags/paused_dag")).mock(
        return_value=httpx.Response(200, json=dag_data)
    )
    result = await client.get_dag("paused_dag")
    assert result["is_paused"] is True


# ---------------------------------------------------------------------------
# get_last_dag_run
# ---------------------------------------------------------------------------

@respx.mock
async def test_get_last_dag_run_returns_most_recent(client):
    run = {
        "dag_run_id": "scheduled__2024-01-01T00:00:00+00:00",
        "dag_id": "my_dag",
        "state": "success",
        "start_date": "2024-01-01T00:00:05+00:00",
        "end_date": "2024-01-01T00:01:00+00:00",
        "logical_date": "2024-01-01T00:00:00+00:00",
    }
    route = respx.get(_url("/dags/my_dag/dagRuns")).mock(
        return_value=httpx.Response(200, json={"dag_runs": [run], "total_entries": 10})
    )
    result = await client.get_last_dag_run("my_dag")
    assert result == run
    params = route.calls[0].request.url.params
    assert params["limit"] == "1"
    assert params["order_by"] == "-start_date"


@respx.mock
async def test_get_last_dag_run_returns_none_when_no_runs(client):
    respx.get(_url("/dags/my_dag/dagRuns")).mock(
        return_value=httpx.Response(200, json={"dag_runs": [], "total_entries": 0})
    )
    result = await client.get_last_dag_run("my_dag")
    assert result is None


@respx.mock
async def test_get_last_dag_run_failed_state(client):
    run = {
        "dag_run_id": "scheduled__2024-01-02T00:00:00+00:00",
        "dag_id": "my_dag",
        "state": "failed",
        "start_date": "2024-01-02T00:00:05+00:00",
        "end_date": "2024-01-02T00:00:30+00:00",
        "logical_date": "2024-01-02T00:00:00+00:00",
    }
    respx.get(_url("/dags/my_dag/dagRuns")).mock(
        return_value=httpx.Response(200, json={"dag_runs": [run], "total_entries": 5})
    )
    result = await client.get_last_dag_run("my_dag")
    assert result["state"] == "failed"


@respx.mock
async def test_get_last_dag_run_raises_on_404(client):
    respx.get(_url("/dags/missing/dagRuns")).mock(
        return_value=httpx.Response(404, json={"title": "DAG not found"})
    )
    with pytest.raises(AirflowError) as exc_info:
        await client.get_last_dag_run("missing")
    assert exc_info.value.status_code == 404


# ---------------------------------------------------------------------------
# get_failed_task_instances
# ---------------------------------------------------------------------------

@respx.mock
async def test_get_failed_task_instances_returns_failed_tasks(client):
    task_instance = {
        "task_id": "my_task",
        "dag_id": "my_dag",
        "dag_run_id": "run_1",
        "state": "failed",
        "try_number": 1,
        "start_date": "2024-01-02T00:00:05+00:00",
        "end_date": "2024-01-02T00:00:10+00:00",
    }
    route = respx.get(_url("/dags/my_dag/dagRuns/run_1/taskInstances")).mock(
        return_value=httpx.Response(200, json={"task_instances": [task_instance], "total_entries": 1})
    )
    result = await client.get_failed_task_instances("my_dag", "run_1")
    assert result == [task_instance]
    assert route.calls[0].request.url.params["state"] == "failed"


@respx.mock
async def test_get_failed_task_instances_returns_empty_when_none(client):
    respx.get(_url("/dags/my_dag/dagRuns/run_1/taskInstances")).mock(
        return_value=httpx.Response(200, json={"task_instances": [], "total_entries": 0})
    )
    result = await client.get_failed_task_instances("my_dag", "run_1")
    assert result == []


@respx.mock
async def test_get_failed_task_instances_raises_on_404(client):
    respx.get(_url("/dags/missing/dagRuns/run_1/taskInstances")).mock(
        return_value=httpx.Response(404, json={"title": "DAG not found"})
    )
    with pytest.raises(AirflowError) as exc_info:
        await client.get_failed_task_instances("missing", "run_1")
    assert exc_info.value.status_code == 404


# ---------------------------------------------------------------------------
# get_task_logs
# ---------------------------------------------------------------------------

@respx.mock
async def test_get_task_logs_returns_log_text(client):
    log_text = "*** Found local files:\n*** /opt/airflow/logs/my_task/attempt=1.log\n[2024-01-02 00:00:09] ERROR - Task failed with exception\nTraceback (most recent call last):\n  File \"dag.py\", line 10, in execute\n    raise ValueError('something went wrong')\nValueError: something went wrong\n"
    respx.get(_url("/dags/my_dag/dagRuns/run_1/taskInstances/my_task/logs/1")).mock(
        return_value=httpx.Response(200, text=log_text)
    )
    result = await client.get_task_logs("my_dag", "run_1", "my_task", 1)
    assert result == log_text


@respx.mock
async def test_get_task_logs_raises_on_error(client):
    respx.get(_url("/dags/my_dag/dagRuns/run_1/taskInstances/my_task/logs/1")).mock(
        return_value=httpx.Response(404, text="Not found")
    )
    with pytest.raises(AirflowError) as exc_info:
        await client.get_task_logs("my_dag", "run_1", "my_task", 1)
    assert exc_info.value.status_code == 404


# ---------------------------------------------------------------------------
# Missing env vars
# ---------------------------------------------------------------------------

def test_missing_api_url_raises(monkeypatch):
    monkeypatch.delenv("AIRFLOW_API_URL", raising=False)
    with pytest.raises(RuntimeError, match="AIRFLOW_API_URL"):
        AirflowClient()
