import asyncio
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


_MR_REVIEW_FIELDS = frozenset({
    "iid", "title", "description", "state", "author", "assignees", "reviewers",
    "source_branch", "target_branch", "labels", "web_url", "created_at",
    "updated_at", "diff_refs", "merge_status", "blocking_discussions_resolved",
    "changes_count", "head_pipeline",
})


def _shape_mr(mr: dict[str, Any]) -> dict[str, Any]:
    shaped = {k: v for k, v in mr.items() if k in _MR_REVIEW_FIELDS}
    if isinstance(shaped.get("author"), dict):
        shaped["author"] = {
            "name": shaped["author"].get("name"),
            "username": shaped["author"].get("username"),
        }
    for key in ("assignees", "reviewers"):
        if isinstance(shaped.get(key), list):
            shaped[key] = [
                {"name": u.get("name"), "username": u.get("username")}
                for u in shaped[key]
            ]
    if isinstance(shaped.get("head_pipeline"), dict):
        shaped["head_pipeline"] = {
            "status": shaped["head_pipeline"].get("status"),
            "web_url": shaped["head_pipeline"].get("web_url"),
        }
    return shaped


def _annotate_large_diffs(diffs: list[Any]) -> list[Any]:
    for diff in diffs:
        if diff.get("too_large"):
            diff["diff"] = (
                f"[diff unavailable — {diff.get('new_path', 'file')} is too large;"
                " use get_file_contents to read it]"
            )
    return diffs


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
    variables: list[dict[str, str]] | None = None,
    inputs: dict[str, Any] | None = None,
    ctx: Context = None,  # type: ignore[assignment]
) -> str:
    """Trigger a new CI/CD pipeline for a branch or tag.

    Args:
        project_id: Numeric project ID or URL-encoded path (e.g. 'group%2Fproject').
        ref: Branch name or tag to run the pipeline on.
        variables: Optional list of pipeline variables. Each entry must be a dict
            with keys 'key', 'value', and optionally 'variable_type' ('env_var'
            or 'file'). Example: [{"key": "MY_VAR", "value": "hello"}]
        inputs: Optional dict of pipeline inputs (for pipelines that declare
            spec.inputs). Keys are input names, values are the input values.
            Example: {"environment": "staging", "deploy": true}

    Returns:
        JSON object representing the created pipeline, including its ID and status.
    """
    try:
        result = await _get_client(ctx).create_pipeline(
            project_id, ref, variables=variables, inputs=inputs
        )
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


@mcp.tool()
async def get_latest_release(
    project_id: str,
    major_version: int | None = None,
    ctx: Context = None,  # type: ignore[assignment]
) -> str:
    """Get the latest release for a GitLab project, ranked by semantic version.

    Fetches all releases and returns the one with the highest semver tag. Tags that
    cannot be parsed as semver (X.Y.Z or vX.Y.Z) are ignored.

    Args:
        project_id: Numeric project ID or URL-encoded path (e.g. 'group%2Fproject').
        major_version: If provided, only consider releases whose major version matches
            this value (e.g. 2 returns the latest 2.x.x release).

    Returns:
        JSON object for the latest matching release, or an error message if none found.
    """
    try:
        result = await _get_client(ctx).get_latest_release(project_id, major_version)
        if result is None:
            scope = f"major version {major_version}" if major_version is not None else "any version"
            return f"No releases with a valid semver tag found for {scope}."
        return json.dumps(result, indent=2)
    except GitLabError as e:
        return _fmt_error(e)


@mcp.tool()
async def create_branch(
    project_id: str,
    branch: str,
    ref: str,
    ctx: Context = None,  # type: ignore[assignment]
) -> str:
    """Create a new branch in a GitLab repository.

    Args:
        project_id: Numeric project ID or URL-encoded path (e.g. 'group%2Fproject').
        branch: Name of the new branch to create.
        ref: Source branch name, tag, or commit SHA to create the branch from.

    Returns:
        JSON object representing the created branch.
    """
    try:
        result = await _get_client(ctx).create_branch(project_id, branch, ref)
        return json.dumps(result, indent=2)
    except GitLabError as e:
        return _fmt_error(e)


@mcp.tool()
async def get_pipeline(
    project_id: str,
    pipeline_id: int,
    ctx: Context = None,  # type: ignore[assignment]
) -> str:
    """Get the status and details of a specific pipeline.

    Args:
        project_id: Numeric project ID or URL-encoded path (e.g. 'group%2Fproject').
        pipeline_id: Numeric ID of the pipeline.

    Returns:
        JSON object with id, status, ref, sha, created_at, updated_at, duration,
        and web_url.
    """
    try:
        result = await _get_client(ctx).get_pipeline(project_id, pipeline_id)
        return json.dumps(result, indent=2)
    except GitLabError as e:
        return _fmt_error(e)


@mcp.tool()
async def list_pipelines(
    project_id: str,
    ref: str | None = None,
    sha: str | None = None,
    status: str | None = None,
    per_page: int = 20,
    ctx: Context = None,  # type: ignore[assignment]
) -> str:
    """List pipelines for a project, optionally filtered by branch, commit SHA, or status.

    To find pipelines triggered by a specific commit, pass the commit SHA returned
    by push_files as the sha parameter.

    Args:
        project_id: Numeric project ID or URL-encoded path (e.g. 'group%2Fproject').
        ref: Filter by branch or tag name.
        sha: Filter by commit SHA (exact match).
        status: Filter by status: created, pending, running, success, failed,
            canceled, skipped, manual, scheduled.
        per_page: Number of results to return (default 20).

    Returns:
        JSON array of pipeline objects.
    """
    try:
        result = await _get_client(ctx).list_pipelines(
            project_id, ref=ref, sha=sha, status=status, per_page=per_page
        )
        return json.dumps(result, indent=2)
    except GitLabError as e:
        return _fmt_error(e)


@mcp.tool()
async def list_pipeline_jobs(
    project_id: str,
    pipeline_id: int,
    ctx: Context = None,  # type: ignore[assignment]
) -> str:
    """List all jobs in a pipeline.

    Args:
        project_id: Numeric project ID or URL-encoded path (e.g. 'group%2Fproject').
        pipeline_id: Numeric ID of the pipeline.

    Returns:
        JSON array of job objects with id, name, stage, status, duration, and web_url.
    """
    try:
        result = await _get_client(ctx).list_pipeline_jobs(project_id, pipeline_id)
        return json.dumps(result, indent=2)
    except GitLabError as e:
        return _fmt_error(e)


@mcp.tool()
async def get_job_log(
    project_id: str,
    job_id: int,
    max_chars: int = 50000,
    ctx: Context = None,  # type: ignore[assignment]
) -> str:
    """Get the raw trace log for a CI job.

    Logs can be very large. By default, the last 50,000 characters are returned
    (errors appear at the end). A truncation notice is prepended if the log was cut.

    Args:
        project_id: Numeric project ID or URL-encoded path (e.g. 'group%2Fproject').
        job_id: Numeric ID of the job.
        max_chars: Maximum characters to return from the end of the log (default 50000).

    Returns:
        Plain text job log, possibly prefixed with a truncation notice.
    """
    try:
        return await _get_client(ctx).get_job_log(project_id, job_id, max_chars)
    except GitLabError as e:
        return _fmt_error(e)


@mcp.tool()
async def get_merge_request(
    project_id: str,
    mr_iid: int,
    ctx: Context = None,  # type: ignore[assignment]
) -> str:
    """Get full details of a single merge request.

    Use this as the first step when reviewing an MR — it returns the title,
    description, state, author, assignees, labels, source and target branches,
    diff statistics, pipeline status, and web URL.

    Args:
        project_id: Numeric project ID or URL-encoded path (e.g. 'group%2Fproject').
        mr_iid: The internal MR ID (iid) shown in the GitLab UI and URL, e.g. 42.

    Returns:
        JSON object with full MR metadata.
    """
    try:
        result = await _get_client(ctx).get_merge_request(project_id, mr_iid)
        return json.dumps(result, indent=2)
    except GitLabError as e:
        return _fmt_error(e)


@mcp.tool()
async def get_merge_request_diffs(
    project_id: str,
    mr_iid: int,
    ctx: Context = None,  # type: ignore[assignment]
) -> str:
    """Get the unified diffs for all files changed in a merge request.

    Returns all diff objects across all pages. Each object contains old_path,
    new_path, diff (unified diff text), new_file, deleted_file, and renamed_file.

    This is the primary tool for code review. After reading the diffs, use
    get_file_contents with ref=source_branch to load the full content of any
    file that needs deeper context beyond the diff hunks.

    Args:
        project_id: Numeric project ID or URL-encoded path (e.g. 'group%2Fproject').
        mr_iid: The internal MR ID (iid) shown in the GitLab UI and URL, e.g. 42.

    Returns:
        JSON array of diff objects, one per changed file.
    """
    try:
        result = await _get_client(ctx).get_merge_request_diffs(project_id, mr_iid)
        return json.dumps(_annotate_large_diffs(result), indent=2)
    except GitLabError as e:
        return _fmt_error(e)


@mcp.tool()
async def list_merge_requests(
    project_id: str,
    state: str = "opened",
    per_page: int = 20,
    ctx: Context = None,  # type: ignore[assignment]
) -> str:
    """List merge requests for a project.

    Use this for discovery when you don't already know the MR iid. Once you
    have the iid, call get_merge_request and get_merge_request_diffs to review.

    Args:
        project_id: Numeric project ID or URL-encoded path (e.g. 'group%2Fproject').
        state: Filter by MR state: 'opened' (default), 'closed', 'merged',
            'locked', or 'all'.
        per_page: Number of MRs to return (default 20, max 100).

    Returns:
        JSON array of MR summary objects.
    """
    try:
        result = await _get_client(ctx).list_merge_requests(
            project_id, state=state, per_page=per_page
        )
        return json.dumps(result, indent=2)
    except GitLabError as e:
        return _fmt_error(e)


@mcp.tool()
async def get_mr_for_review(
    project_id: str,
    mr_iid: int,
    ctx: Context = None,  # type: ignore[assignment]
) -> str:
    """Get everything needed to review a merge request in a single call.

    Fetches MR metadata and all file diffs in parallel, returning a combined
    object. The metadata is filtered to fields relevant for review (title,
    description, author, branches, labels, pipeline status, etc.) — noise fields
    from the raw API are omitted to reduce context. Files where GitLab's differ
    hit its size limit include an inline note to use get_file_contents instead.

    For existing review conversations, also call get_mr_discussions.

    Args:
        project_id: Numeric project ID or URL-encoded path (e.g. 'group%2Fproject').
        mr_iid: The internal MR ID (iid) shown in the GitLab UI and URL, e.g. 42.

    Returns:
        JSON object with 'mr' (shaped metadata) and 'diffs' (array of diff objects).
    """
    try:
        client = _get_client(ctx)
        mr, diffs = await asyncio.gather(
            client.get_merge_request(project_id, mr_iid),
            client.get_merge_request_diffs(project_id, mr_iid),
        )
        return json.dumps(
            {"mr": _shape_mr(mr), "diffs": _annotate_large_diffs(diffs)},
            indent=2,
        )
    except GitLabError as e:
        return _fmt_error(e)


@mcp.tool()
async def get_mr_discussions(
    project_id: str,
    mr_iid: int,
    ctx: Context = None,  # type: ignore[assignment]
) -> str:
    """Get all review discussions and comments on a merge request.

    Returns threaded discussions including inline code comments (with file path
    and line number) and general MR comments. Use this alongside get_mr_for_review
    to understand what feedback has already been given before adding your own review.

    Args:
        project_id: Numeric project ID or URL-encoded path (e.g. 'group%2Fproject').
        mr_iid: The internal MR ID (iid) shown in the GitLab UI and URL, e.g. 42.

    Returns:
        JSON array of discussion objects, each with a notes array containing
        author, body, position (for inline comments), and resolved status.
    """
    try:
        result = await _get_client(ctx).get_mr_discussions(project_id, mr_iid)
        return json.dumps(result, indent=2)
    except GitLabError as e:
        return _fmt_error(e)


def main() -> None:
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
