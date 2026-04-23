import asyncio
import os
from typing import Any

import httpx


class AirflowError(Exception):
    def __init__(self, status_code: int, message: str) -> None:
        self.status_code = status_code
        super().__init__(f"Airflow API error {status_code}: {message}")


async def _get_gcloud_token() -> str:
    proc = await asyncio.create_subprocess_exec(
        "gcloud", "auth", "print-access-token",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await proc.communicate()
    if proc.returncode != 0:
        raise RuntimeError(f"gcloud auth print-access-token failed: {stderr.decode().strip()}")
    return stdout.decode().strip()


class AirflowClient:
    def __init__(self) -> None:
        base_url = os.environ.get("AIRFLOW_API_URL", "").rstrip("/")
        if not base_url:
            raise RuntimeError("AIRFLOW_API_URL environment variable is not set")

        self._client = httpx.AsyncClient(
            base_url=base_url,
            timeout=30.0,
            follow_redirects=True,
        )

    async def authenticate(self) -> None:
        token = await _get_gcloud_token()
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

    async def get_failed_task_instances(self, dag_id: str, dag_run_id: str) -> list[Any]:
        data = await self._request(
            "GET",
            f"/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances",
            params={"state": "failed"},
        )
        return data.get("task_instances", [])

    async def get_task_logs(self, dag_id: str, dag_run_id: str, task_id: str, try_number: int) -> str:
        response = await self._client.get(
            f"/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/logs/{try_number}"
        )
        if not response.is_success:
            raise AirflowError(response.status_code, response.text)
        return response.text

    async def aclose(self) -> None:
        await self._client.aclose()
