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


def main() -> None:
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
