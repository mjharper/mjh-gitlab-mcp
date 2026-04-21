import os
from typing import Any

import httpx


class DbtError(Exception):
    def __init__(self, status_code: int, message: str) -> None:
        self.status_code = status_code
        super().__init__(f"dbt Cloud API error {status_code}: {message}")


class DbtClient:
    def __init__(self) -> None:
        token = os.environ.get("DBT_CLOUD_API_TOKEN", "")
        account_id = os.environ.get("DBT_CLOUD_ACCOUNT_ID", "")
        base_url = os.environ.get("DBT_CLOUD_BASE_URL", "https://cloud.getdbt.com/api/v3").rstrip("/")

        if not token:
            raise RuntimeError("DBT_CLOUD_API_TOKEN environment variable is not set")
        if not account_id:
            raise RuntimeError("DBT_CLOUD_ACCOUNT_ID environment variable is not set")

        self._account_id = account_id
        self._client = httpx.AsyncClient(
            base_url=base_url,
            headers={"Authorization": f"Token {token}"},
            timeout=30.0,
        )
        v2_base_url = os.environ.get("DBT_CLOUD_ADMIN_V2_URL", "https://cloud.getdbt.com/api/v2").rstrip("/")
        self._v2_client = httpx.AsyncClient(
            base_url=v2_base_url,
            headers={"Authorization": f"Token {token}"},
            timeout=30.0,
        )

    async def _request(self, method: str, path: str, **kwargs: Any) -> Any:
        response = await self._client.request(method, path, **kwargs)
        if not response.is_success:
            raise DbtError(response.status_code, response.text)
        return response.json()

    async def _v2_request(self, method: str, path: str, **kwargs: Any) -> Any:
        response = await self._v2_client.request(method, path, **kwargs)
        if not response.is_success:
            raise DbtError(response.status_code, response.text)
        return response.json()

    async def find_project_by_name(self, name: str) -> dict[str, Any] | None:
        """Search for a project by exact name using name__icontains, returning id/name/created_at."""
        data = await self._request(
            "GET",
            f"/accounts/{self._account_id}/projects/",
            params={"name__icontains": name},
        )
        projects = data.get("data", [])
        # Filter to exact (case-insensitive) match
        for project in projects:
            if project.get("name", "").lower() == name.lower():
                return {
                    "id": project["id"],
                    "name": project["name"],
                    "created_at": project.get("created_at"),
                }
        return None

    async def list_environment_variables(self, project_id: int | str) -> list[str]:
        """Return the names of all environment variables for a project."""
        data = await self._request(
            "GET",
            f"/accounts/{self._account_id}/projects/{project_id}/environment-variables/environment/",
        )
        env_vars = data.get("data", [])
        return [ev["name"] for ev in env_vars if "name" in ev]

    async def list_jobs(self, project_id: str | None = None) -> list[dict[str, Any]]:
        params: dict[str, Any] = {}
        if project_id:
            params["project_id"] = project_id
        data = await self._v2_request("GET", f"/accounts/{self._account_id}/jobs/", params=params)
        return data.get("data", [])

    async def get_latest_failed_run(self, job_id: str) -> dict[str, Any] | None:
        # status=20 is "Error" in dbt Cloud run status codes
        data = await self._v2_request("GET", f"/accounts/{self._account_id}/runs/", params={
            "job_definition_id": job_id,
            "status": 20,
            "order_by": "-created_at",
            "limit": 1,
        })
        runs = data.get("data", [])
        return runs[0] if runs else None

    async def get_run_with_steps(self, run_id: str) -> dict[str, Any]:
        data = await self._v2_request(
            "GET",
            f"/accounts/{self._account_id}/runs/{run_id}/",
            params={"include_related[]": "run_steps"},
        )
        return data.get("data", {})

    async def aclose(self) -> None:
        await self._client.aclose()
        await self._v2_client.aclose()
