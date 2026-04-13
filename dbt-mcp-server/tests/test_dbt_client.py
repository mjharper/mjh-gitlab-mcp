import os

import httpx
import pytest
import respx

from dbt_mcp_server.dbt_client import DbtClient, DbtError

BASE_URL = "https://cloud.getdbt.com/api/v3"
ACCOUNT_ID = "123"


@pytest.fixture(autouse=True)
def dbt_env(monkeypatch):
    monkeypatch.setenv("DBT_CLOUD_API_TOKEN", "test-token")
    monkeypatch.setenv("DBT_CLOUD_ACCOUNT_ID", ACCOUNT_ID)


@pytest.fixture
def client():
    return DbtClient()


def _url(path: str) -> str:
    return BASE_URL + path


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
