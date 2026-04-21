import json
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator

from mcp.server.fastmcp import Context, FastMCP

from .dbt_client import DbtClient, DbtError


@asynccontextmanager
async def lifespan(server: FastMCP) -> AsyncIterator[dict[str, Any]]:
    client = DbtClient()
    try:
        yield {"client": client}
    finally:
        await client.aclose()


mcp = FastMCP("dbt-mcp-server", lifespan=lifespan)


def _get_client(ctx: Context) -> DbtClient:
    return ctx.request_context.lifespan_context["client"]


def _fmt_error(e: DbtError) -> str:
    return str(e)


@mcp.tool()
async def find_project_by_name(
    name: str,
    ctx: Context = None,  # type: ignore[assignment]
) -> str:
    """Find a dbt Cloud project by its exact name.

    Searches using a case-insensitive contains filter and then matches the
    exact name from the results.

    Args:
        name: The exact project name to find.

    Returns:
        JSON object with id, name, and created_at fields, or an error message
        if the project is not found.
    """
    try:
        result = await _get_client(ctx).find_project_by_name(name)
        if result is None:
            return f"No project found with name: {name!r}"
        return json.dumps(result, indent=2)
    except DbtError as e:
        return _fmt_error(e)


@mcp.tool()
async def list_environment_variables(
    project_id: str,
    ctx: Context = None,  # type: ignore[assignment]
) -> str:
    """List environment variable names for a dbt Cloud project.

    Args:
        project_id: The numeric dbt Cloud project ID.

    Returns:
        JSON array of environment variable name strings.
    """
    try:
        result = await _get_client(ctx).list_environment_variables(project_id)
        return json.dumps(result, indent=2)
    except DbtError as e:
        return _fmt_error(e)


@mcp.tool()
async def list_jobs(
    ctx: Context = None,  # type: ignore[assignment]
    project_id: str | None = None,
) -> str:
    """List dbt Cloud jobs, optionally filtered by project.

    Args:
        project_id: Optional numeric dbt Cloud project ID to filter by.

    Returns:
        JSON array of jobs with id, name, project_id, environment_id, and created_at fields.
    """
    try:
        jobs = await _get_client(ctx).list_jobs(project_id=project_id)
        return json.dumps(
            [
                {
                    "id": j["id"],
                    "name": j["name"],
                    "project_id": j.get("project_id"),
                    "environment_id": j.get("environment_id"),
                    "created_at": j.get("created_at"),
                }
                for j in jobs
            ],
            indent=2,
        )
    except DbtError as e:
        return _fmt_error(e)


@mcp.tool()
async def get_job_run_errors(
    job_id: str,
    ctx: Context = None,  # type: ignore[assignment]
) -> str:
    """Get the most recent failed run for a dbt Cloud job, including step-level logs.

    Args:
        job_id: The numeric dbt Cloud job ID.

    Returns:
        JSON object with run metadata and per-step logs showing model/test errors,
        or a message if no failed runs exist for the job.
    """
    client = _get_client(ctx)
    try:
        run = await client.get_latest_failed_run(job_id)
        if run is None:
            return f"No failed runs found for job_id={job_id!r}"
        run_with_steps = await client.get_run_with_steps(str(run["id"]))
        steps = run_with_steps.get("run_steps", [])
        return json.dumps(
            {
                "run_id": run_with_steps.get("id"),
                "job_id": job_id,
                "status_humanized": run_with_steps.get("status_humanized"),
                "created_at": run_with_steps.get("created_at"),
                "finished_at": run_with_steps.get("finished_at"),
                "steps": [
                    {
                        "name": s.get("name"),
                        "status_humanized": s.get("status_humanized"),
                        "logs": s.get("logs"),
                    }
                    for s in steps
                ],
            },
            indent=2,
        )
    except DbtError as e:
        return _fmt_error(e)


def main() -> None:
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
