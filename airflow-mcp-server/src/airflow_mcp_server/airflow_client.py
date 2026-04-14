import os
from typing import Any

import httpx


class AirflowError(Exception):
    def __init__(self, status_code: int, message: str) -> None:
        self.status_code = status_code
        super().__init__(f"Airflow API error {status_code}: {message}")


class AirflowClient:
    def __init__(self) -> None:
        base_url = os.environ.get("AIRFLOW_API_URL", "").rstrip("/")
        self._username = os.environ.get("AIRFLOW_USERNAME", "")
        self._password = os.environ.get("AIRFLOW_PASSWORD", "")

        if not base_url:
            raise RuntimeError("AIRFLOW_API_URL environment variable is not set")
        if not self._username:
            raise RuntimeError("AIRFLOW_USERNAME environment variable is not set")
        if not self._password:
            raise RuntimeError("AIRFLOW_PASSWORD environment variable is not set")

        self._client = httpx.AsyncClient(
            base_url=base_url,
            timeout=30.0,
        )

    async def authenticate(self) -> None:
        """Obtain a JWT from /auth/token and store it as a Bearer header."""
        response = await self._client.post(
            "/auth/token",
            json={"username": self._username, "password": self._password},
        )
        if not response.is_success:
            raise AirflowError(response.status_code, response.text)
        token = response.json()["access_token"]
        self._client.headers["Authorization"] = f"Bearer {token}"

    async def _request(self, method: str, path: str, **kwargs: Any) -> Any:
        response = await self._client.request(method, path, **kwargs)
        if not response.is_success:
            raise AirflowError(response.status_code, response.text)
        return response.json()

    async def list_dags(
        self,
        dag_id_pattern: str | None = None,
        only_active: bool = False,
        limit: int = 20,
    ) -> Any:
        params: dict[str, Any] = {"limit": limit}
        if dag_id_pattern:
            params["dag_id_pattern"] = dag_id_pattern
        if only_active:
            params["only_active"] = True
        return await self._request("GET", "/dags", params=params)

    async def get_dag(self, dag_id: str) -> Any:
        return await self._request("GET", f"/dags/{dag_id}")

    async def get_last_dag_run(self, dag_id: str) -> dict[str, Any] | None:
        data = await self._request(
            "GET",
            f"/dags/{dag_id}/dagRuns",
            params={"limit": 1, "order_by": "-start_date"},
        )
        runs: list[Any] = data.get("dag_runs", [])
        return runs[0] if runs else None

    async def aclose(self) -> None:
        await self._client.aclose()
