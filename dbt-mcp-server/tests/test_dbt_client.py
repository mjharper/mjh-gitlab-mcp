import os

import httpx
import pytest
import respx

from dbt_mcp_server.dbt_client import DbtClient, DbtError

BASE_URL = "https://cloud.getdbt.com/api/v3"
V2_BASE_URL = "https://cloud.getdbt.com/api/v2"
ACCOUNT_ID = "123"


@pytest.fixture(autouse=True)
def dbt_env(monkeypatch):
    monkeypatch.setenv("DBT_CLOUD_API_TOKEN", "test-token")
    monkeypatch.setenv("DBT_CLOUD_ACCOUNT_ID", ACCOUNT_ID)
    monkeypatch.setenv("DBT_CLOUD_BASE_URL", BASE_URL)
    monkeypatch.setenv("DBT_CLOUD_ADMIN_V2_URL", V2_BASE_URL)


@pytest.fixture
def client():
    return DbtClient()


def _url(path: str) -> str:
    return BASE_URL + path


def _v2_url(path: str) -> str:
    return V2_BASE_URL + path


# ---------------------------------------------------------------------------
# Authentication
# ---------------------------------------------------------------------------

@respx.mock
async def test_auth_token_header_sent(client):
    route = respx.get(_url(f"/accounts/{ACCOUNT_ID}/projects/")).mock(
        return_value=httpx.Response(200, json={"data": []})
    )
    await client.find_project_by_name("anything")
    assert route.calls[0].request.headers["Authorization"] == "Token test-token"


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------

@respx.mock
async def test_raises_dbt_error_on_4xx(client):
    respx.get(_url(f"/accounts/{ACCOUNT_ID}/projects/")).mock(
        return_value=httpx.Response(401, json={"status": {"user_message": "Unauthorized"}})
    )
    with pytest.raises(DbtError) as exc_info:
        await client.find_project_by_name("myproject")
    assert exc_info.value.status_code == 401


@respx.mock
async def test_raises_dbt_error_on_5xx(client):
    respx.get(_url(f"/accounts/{ACCOUNT_ID}/projects/")).mock(
        return_value=httpx.Response(500, text="Internal Server Error")
    )
    with pytest.raises(DbtError) as exc_info:
        await client.find_project_by_name("myproject")
    assert exc_info.value.status_code == 500


# ---------------------------------------------------------------------------
# find_project_by_name
# ---------------------------------------------------------------------------

@respx.mock
async def test_find_project_by_name_sends_icontains_param(client):
    route = respx.get(_url(f"/accounts/{ACCOUNT_ID}/projects/")).mock(
        return_value=httpx.Response(200, json={"data": []})
    )
    await client.find_project_by_name("analytics")
    assert route.calls[0].request.url.params["name__icontains"] == "analytics"


@respx.mock
async def test_find_project_by_name_returns_matching_project(client):
    respx.get(_url(f"/accounts/{ACCOUNT_ID}/projects/")).mock(
        return_value=httpx.Response(200, json={"data": [
            {"id": 42, "name": "Analytics", "created_at": "2024-01-01T00:00:00Z"},
            {"id": 99, "name": "Analytics Extra", "created_at": "2024-02-01T00:00:00Z"},
        ]})
    )
    result = await client.find_project_by_name("Analytics")
    assert result == {"id": 42, "name": "Analytics", "created_at": "2024-01-01T00:00:00Z"}


@respx.mock
async def test_find_project_by_name_case_insensitive_match(client):
    respx.get(_url(f"/accounts/{ACCOUNT_ID}/projects/")).mock(
        return_value=httpx.Response(200, json={"data": [
            {"id": 7, "name": "My Project", "created_at": "2024-03-01T00:00:00Z"},
        ]})
    )
    result = await client.find_project_by_name("my project")
    assert result is not None
    assert result["id"] == 7


@respx.mock
async def test_find_project_by_name_returns_none_when_not_found(client):
    respx.get(_url(f"/accounts/{ACCOUNT_ID}/projects/")).mock(
        return_value=httpx.Response(200, json={"data": [
            {"id": 1, "name": "Other Project", "created_at": "2024-01-01T00:00:00Z"},
        ]})
    )
    result = await client.find_project_by_name("Analytics")
    assert result is None


@respx.mock
async def test_find_project_by_name_returns_none_on_empty_results(client):
    respx.get(_url(f"/accounts/{ACCOUNT_ID}/projects/")).mock(
        return_value=httpx.Response(200, json={"data": []})
    )
    result = await client.find_project_by_name("Analytics")
    assert result is None


@respx.mock
async def test_find_project_by_name_only_returns_id_name_created_at(client):
    respx.get(_url(f"/accounts/{ACCOUNT_ID}/projects/")).mock(
        return_value=httpx.Response(200, json={"data": [
            {
                "id": 5,
                "name": "Warehouse",
                "created_at": "2024-06-01T00:00:00Z",
                "account_id": 123,
                "dbt_project_subdirectory": None,
            },
        ]})
    )
    result = await client.find_project_by_name("Warehouse")
    assert set(result.keys()) == {"id", "name", "created_at"}


# ---------------------------------------------------------------------------
# list_environment_variables
# ---------------------------------------------------------------------------

@respx.mock
async def test_list_environment_variables_url(client):
    route = respx.get(_url(f"/accounts/{ACCOUNT_ID}/projects/42/environment-variables/environment/")).mock(
        return_value=httpx.Response(200, json={"data": []})
    )
    await client.list_environment_variables("42")
    assert route.called


@respx.mock
async def test_list_environment_variables_returns_names(client):
    respx.get(_url(f"/accounts/{ACCOUNT_ID}/projects/42/environment-variables/environment/")).mock(
        return_value=httpx.Response(200, json={"data": [
            {"name": "DBT_ENV_SECRET_KEY", "type": "secret"},
            {"name": "DBT_DATASET", "type": "project"},
            {"name": "DBT_TARGET", "type": "environment"},
        ]})
    )
    result = await client.list_environment_variables("42")
    assert result == ["DBT_ENV_SECRET_KEY", "DBT_DATASET", "DBT_TARGET"]


@respx.mock
async def test_list_environment_variables_empty(client):
    respx.get(_url(f"/accounts/{ACCOUNT_ID}/projects/1/environment-variables/environment/")).mock(
        return_value=httpx.Response(200, json={"data": []})
    )
    result = await client.list_environment_variables("1")
    assert result == []


@respx.mock
async def test_list_environment_variables_raises_on_error(client):
    respx.get(_url(f"/accounts/{ACCOUNT_ID}/projects/99/environment-variables/environment/")).mock(
        return_value=httpx.Response(404, json={"status": {"user_message": "Not found"}})
    )
    with pytest.raises(DbtError) as exc_info:
        await client.list_environment_variables("99")
    assert exc_info.value.status_code == 404


# ---------------------------------------------------------------------------
# Missing env vars
# ---------------------------------------------------------------------------

def test_missing_token_raises(monkeypatch):
    monkeypatch.delenv("DBT_CLOUD_API_TOKEN", raising=False)
    with pytest.raises(RuntimeError, match="DBT_CLOUD_API_TOKEN"):
        DbtClient()


def test_missing_account_id_raises(monkeypatch):
    monkeypatch.delenv("DBT_CLOUD_ACCOUNT_ID", raising=False)
    with pytest.raises(RuntimeError, match="DBT_CLOUD_ACCOUNT_ID"):
        DbtClient()


# ---------------------------------------------------------------------------
# Base URL configuration
# ---------------------------------------------------------------------------

@respx.mock
async def test_uses_custom_base_url(monkeypatch):
    custom_url = "https://abc123.us1.dbt.com/api/v3"
    monkeypatch.setenv("DBT_CLOUD_API_TOKEN", "test-token")
    monkeypatch.setenv("DBT_CLOUD_ACCOUNT_ID", ACCOUNT_ID)
    monkeypatch.setenv("DBT_CLOUD_BASE_URL", custom_url)

    client = DbtClient()

    route = respx.get(custom_url + f"/accounts/{ACCOUNT_ID}/projects/").mock(
        return_value=httpx.Response(200, json={"data": []})
    )
    await client.find_project_by_name("anything")
    assert route.called


@respx.mock
async def test_default_base_url_when_not_set(monkeypatch):
    monkeypatch.delenv("DBT_CLOUD_BASE_URL", raising=False)
    monkeypatch.setenv("DBT_CLOUD_API_TOKEN", "test-token")
    monkeypatch.setenv("DBT_CLOUD_ACCOUNT_ID", ACCOUNT_ID)

    client = DbtClient()

    default_url = "https://cloud.getdbt.com/api/v3"
    route = respx.get(default_url + f"/accounts/{ACCOUNT_ID}/projects/").mock(
        return_value=httpx.Response(200, json={"data": []})
    )
    await client.find_project_by_name("anything")
    assert route.called


# ---------------------------------------------------------------------------
# list_jobs
# ---------------------------------------------------------------------------

@respx.mock
async def test_list_jobs_no_filter(client):
    route = respx.get(_v2_url(f"/accounts/{ACCOUNT_ID}/jobs/")).mock(
        return_value=httpx.Response(200, json={"data": [
            {"id": 1, "name": "Analytics Hourly", "project_id": 10, "environment_id": 20, "created_at": "2024-01-01"},
        ]})
    )
    result = await client.list_jobs()
    assert route.called
    assert len(result) == 1
    assert result[0]["id"] == 1
    assert result[0]["name"] == "Analytics Hourly"


@respx.mock
async def test_list_jobs_with_project_id(client):
    route = respx.get(_v2_url(f"/accounts/{ACCOUNT_ID}/jobs/")).mock(
        return_value=httpx.Response(200, json={"data": []})
    )
    await client.list_jobs(project_id="42")
    assert route.calls[0].request.url.params["project_id"] == "42"


# ---------------------------------------------------------------------------
# get_latest_failed_run
# ---------------------------------------------------------------------------

@respx.mock
async def test_get_latest_failed_run_found(client):
    run = {"id": 999, "job_definition_id": 5, "status": 20, "created_at": "2024-06-01T00:00:00Z"}
    route = respx.get(_v2_url(f"/accounts/{ACCOUNT_ID}/runs/")).mock(
        return_value=httpx.Response(200, json={"data": [run]})
    )
    result = await client.get_latest_failed_run("5")
    params = route.calls[0].request.url.params
    assert params["status"] == "20"
    assert params["order_by"] == "-created_at"
    assert params["limit"] == "1"
    assert result == run


@respx.mock
async def test_get_latest_failed_run_not_found(client):
    respx.get(_v2_url(f"/accounts/{ACCOUNT_ID}/runs/")).mock(
        return_value=httpx.Response(200, json={"data": []})
    )
    result = await client.get_latest_failed_run("5")
    assert result is None


# ---------------------------------------------------------------------------
# get_run_with_steps
# ---------------------------------------------------------------------------

@respx.mock
async def test_get_run_with_steps(client):
    run_data = {
        "id": 999,
        "status_humanized": "Error",
        "created_at": "2024-06-01T00:00:00Z",
        "finished_at": "2024-06-01T00:05:00Z",
        "run_steps": [
            {"name": "dbt run", "status_humanized": "Error", "logs": "Compilation Error in model orders"},
        ],
    }
    route = respx.get(_v2_url(f"/accounts/{ACCOUNT_ID}/runs/999/")).mock(
        return_value=httpx.Response(200, json={"data": run_data})
    )
    result = await client.get_run_with_steps("999")
    assert route.calls[0].request.url.params["include_related[]"] == "run_steps"
    assert result["id"] == 999
    assert len(result["run_steps"]) == 1


# ---------------------------------------------------------------------------
# v2 error handling
# ---------------------------------------------------------------------------

@respx.mock
async def test_v2_error_raises_dbt_error(client):
    respx.get(_v2_url(f"/accounts/{ACCOUNT_ID}/jobs/")).mock(
        return_value=httpx.Response(403, json={"status": {"user_message": "Forbidden"}})
    )
    with pytest.raises(DbtError) as exc_info:
        await client.list_jobs()
    assert exc_info.value.status_code == 403
