import json
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator

from mcp.server.fastmcp import Context, FastMCP

from .airflow_client import AirflowClient, AirflowError


@asynccontextmanager
async def lifespan(server: FastMCP) -> AsyncIterator[dict[str, Any]]:
    client = AirflowClient()
    await client.authenticate()
    try:
        yield {"client": client}
    finally:
        await client.aclose()


mcp = FastMCP("airflow-mcp-server", lifespan=lifespan)


def _get_client(ctx: Context) -> AirflowClient:
    return ctx.request_context.lifespan_context["client"]


def _fmt_error(e: AirflowError) -> str:
    return str(e)


@mcp.tool()
async def list_dags(
    dag_id_pattern: str | None = None,
    only_active: bool = False,
    limit: int = 20,
    ctx: Context = None,  # type: ignore[assignment]
) -> str:
    """List DAGs in Airflow.

    Args:
        dag_id_pattern: Optional substring to filter DAG IDs (case-insensitive contains match).
        only_active: If True, return only DAGs that are not paused (default False).
        limit: Maximum number of DAGs to return (default 20).

    Returns:
        JSON object with a 'dags' array. Each DAG entry includes dag_id, is_paused,
        is_active, schedule_interval, and owners fields.
    """
    try:
        result = await _get_client(ctx).list_dags(dag_id_pattern, only_active, limit)
        return json.dumps(result, indent=2)
    except AirflowError as e:
        return _fmt_error(e)


@mcp.tool()
async def get_dag(
    dag_id: str,
    ctx: Context = None,  # type: ignore[assignment]
) -> str:
    """Get details for a specific Airflow DAG, including whether it is enabled.

    A DAG is enabled when its is_paused field is false.

    Args:
        dag_id: The DAG ID to look up.

    Returns:
        JSON object with dag_id, is_paused, is_active, schedule_interval, owners,
        fileloc, and other metadata fields.
    """
    try:
        result = await _get_client(ctx).get_dag(dag_id)
        return json.dumps(result, indent=2)
    except AirflowError as e:
        return _fmt_error(e)


@mcp.tool()
async def get_last_dag_run(
    dag_id: str,
    ctx: Context = None,  # type: ignore[assignment]
) -> str:
    """Get the status of the most recent run for an Airflow DAG.

    Args:
        dag_id: The DAG ID to look up.

    Returns:
        JSON object with dag_run_id, dag_id, state, start_date, end_date, and
        logical_date fields for the most recent run. Returns a message if no
        runs exist yet.
    """
    try:
        result = await _get_client(ctx).get_last_dag_run(dag_id)
        if result is None:
            return f"No runs found for DAG: {dag_id!r}"
        return json.dumps(result, indent=2)
    except AirflowError as e:
        return _fmt_error(e)


def main() -> None:
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
