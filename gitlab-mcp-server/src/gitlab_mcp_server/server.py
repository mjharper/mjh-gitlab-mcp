import json
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator

from mcp.server.fastmcp import Context, FastMCP

from .gitlab_client import GitLabClient, GitLabError


@asynccontextmanager
async def lifespan(server: FastMCP) -> AsyncIterator[dict[str, Any]]:
    client = GitLabClient()
    try:
        yield {"client": client}
    finally:
        await client.aclose()


mcp = FastMCP("gitlab-mcp-server", lifespan=lifespan)


def _get_client(ctx: Context) -> GitLabClient:
    return ctx.request_context.lifespan_context["client"]


def _fmt_error(e: GitLabError) -> str:
    return str(e)


@mcp.tool()
async def list_releases(project_id: str, per_page: int = 20, ctx: Context = None) -> str:  # type: ignore[assignment]
    """List releases for a GitLab project.

    Args:
        project_id: Numeric project ID or URL-encoded path (e.g. 'group%2Fproject').
        per_page: Number of results per page (default 20).

    Returns:
        JSON array of release objects.
    """
    try:
        result = await _get_client(ctx).list_releases(project_id, per_page)
        return json.dumps(result, indent=2)
    except GitLabError as e:
        return _fmt_error(e)


@mcp.tool()
async def get_file_contents(
    project_id: str,
    file_path: str,
    ref: str = "main",
    ctx: Context = None,  # type: ignore[assignment]
) -> str:
    """Get the decoded content of a single file from a GitLab repository.

    Args:
        project_id: Numeric project ID or URL-encoded path (e.g. 'group%2Fproject').
        file_path: Path to the file within the repository (e.g. 'src/main.py').
        ref: Branch name, tag, or commit SHA to read from (default 'main').

    Returns:
        The decoded UTF-8 file content as a string.
    """
    try:
        result = await _get_client(ctx).get_file_contents(project_id, file_path, ref)
        return result
    except GitLabError as e:
        return _fmt_error(e)


@mcp.tool()
async def list_projects(
    search: str | None = None,
    per_page: int = 20,
    ctx: Context = None,  # type: ignore[assignment]
) -> str:
    """List or search GitLab projects accessible to the authenticated user.

    Args:
        search: Optional search query to filter projects by name.
        per_page: Number of results per page (default 20).

    Returns:
        JSON array of project objects.
    """
    try:
        result = await _get_client(ctx).list_projects(search, per_page)
        return json.dumps(result, indent=2)
    except GitLabError as e:
        return _fmt_error(e)


@mcp.tool()
async def push_files(
    project_id: str,
    branch: str,
    commit_message: str,
    files: list[dict[str, str]],
    start_branch: str | None = None,
    ctx: Context = None,  # type: ignore[assignment]
) -> str:
    """Create a single commit with multiple file changes, optionally creating a new branch.

    This tool performs an atomic branch creation and multi-file commit in a single API
    call, which avoids triggering multiple CI pipelines.

    Args:
        project_id: Numeric project ID or URL-encoded path (e.g. 'group%2Fproject').
        branch: Target branch to commit to. May be a new branch name.
        commit_message: The commit message.
        files: List of file change objects. Each must have:
            - file_path (str): Path of the file in the repository.
            - content (str): New file content (not needed for 'delete' actions).
            - action (str): One of 'create', 'update', 'delete', 'move' (default 'update').
        start_branch: If provided, the new branch is created from this branch before
            committing. Enables atomic branch creation + commit without a separate
            create_branch call.

    Returns:
        JSON object representing the created commit.
    """
    try:
        result = await _get_client(ctx).push_files(
            project_id, branch, commit_message, files, start_branch
        )
        return json.dumps(result, indent=2)
    except GitLabError as e:
        return _fmt_error(e)


@mcp.tool()
async def create_merge_request(
    project_id: str,
    title: str,
    source_branch: str,
    target_branch: str,
    description: str | None = None,
    ctx: Context = None,  # type: ignore[assignment]
) -> str:
    """Create a merge request in a GitLab project.

    Args:
        project_id: Numeric project ID or URL-encoded path (e.g. 'group%2Fproject').
        title: Title of the merge request.
        source_branch: Branch containing the changes to merge.
        target_branch: Branch to merge changes into.
        description: Optional longer description for the merge request.

    Returns:
        JSON object representing the created merge request, including its web URL.
    """
    try:
        result = await _get_client(ctx).create_merge_request(
            project_id, title, source_branch, target_branch, description
        )
        return json.dumps(result, indent=2)
    except GitLabError as e:
        return _fmt_error(e)


@mcp.tool()
async def create_pipeline(
    project_id: str,
    ref: str,
    ctx: Context = None,  # type: ignore[assignment]
) -> str:
    """Trigger a new CI/CD pipeline for a branch or tag.

    Args:
        project_id: Numeric project ID or URL-encoded path (e.g. 'group%2Fproject').
        ref: Branch name or tag to run the pipeline on.

    Returns:
        JSON object representing the created pipeline, including its ID and status.
    """
    try:
        result = await _get_client(ctx).create_pipeline(project_id, ref)
        return json.dumps(result, indent=2)
    except GitLabError as e:
        return _fmt_error(e)


@mcp.tool()
async def get_repository_tree(
    project_id: str,
    path: str = "",
    ref: str = "main",
    recursive: bool = False,
    ctx: Context = None,  # type: ignore[assignment]
) -> str:
    """List files and directories in a GitLab repository.

    Args:
        project_id: Numeric project ID or URL-encoded path (e.g. 'group%2Fproject').
        path: Subdirectory path to list (default '' lists the root).
        ref: Branch name, tag, or commit SHA (default 'main').
        recursive: If True, list all files recursively (default False).

    Returns:
        JSON array of tree node objects with name, path, type, and id fields.
    """
    try:
        result = await _get_client(ctx).get_repository_tree(
            project_id, path, ref, recursive
        )
        return json.dumps(result, indent=2)
    except GitLabError as e:
        return _fmt_error(e)


def main() -> None:
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
