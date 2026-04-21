import base64
import os

import httpx
import pytest
import respx

from gitlab_mcp_server.gitlab_client import GitLabClient, GitLabError, _parse_semver, _next_link

BASE_URL = "https://gitlab.example.com/api/v4"


@pytest.fixture(autouse=True)
def gitlab_env(monkeypatch):
    monkeypatch.setenv("GITLAB_API_URL", BASE_URL)
    monkeypatch.setenv("GITLAB_PERSONAL_ACCESS_TOKEN", "test-token")


@pytest.fixture
def client():
    return GitLabClient()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _url(path: str) -> str:
    return BASE_URL + path


# ---------------------------------------------------------------------------
# Authentication
# ---------------------------------------------------------------------------

@respx.mock
async def test_private_token_header_sent(client):
    route = respx.get(_url("/projects")).mock(return_value=httpx.Response(200, json=[]))
    await client.list_projects()
    assert route.calls[0].request.headers["PRIVATE-TOKEN"] == "test-token"


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------

@respx.mock
async def test_raises_gitlab_error_on_4xx(client):
    respx.get(_url("/projects/99/releases")).mock(
        return_value=httpx.Response(404, json={"message": "Not found"})
    )
    with pytest.raises(GitLabError) as exc_info:
        await client.list_releases("99")
    assert exc_info.value.status_code == 404


@respx.mock
async def test_raises_gitlab_error_on_5xx(client):
    respx.get(_url("/projects/1/releases")).mock(
        return_value=httpx.Response(500, text="Internal Server Error")
    )
    with pytest.raises(GitLabError) as exc_info:
        await client.list_releases("1")
    assert exc_info.value.status_code == 500


# ---------------------------------------------------------------------------
# list_releases
# ---------------------------------------------------------------------------

@respx.mock
async def test_list_releases_url_and_params(client):
    route = respx.get(_url("/projects/42/releases")).mock(
        return_value=httpx.Response(200, json=[{"tag_name": "v1.0"}])
    )
    result = await client.list_releases("42", per_page=5)
    assert result == [{"tag_name": "v1.0"}]
    assert route.calls[0].request.url.params["per_page"] == "5"


@respx.mock
async def test_list_releases_encoded_project_id(client):
    route = respx.get(_url("/projects/group%2Fproject/releases")).mock(
        return_value=httpx.Response(200, json=[])
    )
    await client.list_releases("group%2Fproject")
    assert route.called


# ---------------------------------------------------------------------------
# get_file_contents
# ---------------------------------------------------------------------------

@respx.mock
async def test_get_file_contents_decodes_base64(client):
    content = base64.b64encode(b"hello world\n").decode()
    respx.get(_url("/projects/1/repository/files/README.md")).mock(
        return_value=httpx.Response(200, json={"content": content, "encoding": "base64"})
    )
    result = await client.get_file_contents("1", "README.md")
    assert result == "hello world\n"


@respx.mock
async def test_get_file_contents_encodes_path(client):
    content = base64.b64encode(b"x").decode()
    route = respx.get(_url("/projects/1/repository/files/src%2Fmain.py")).mock(
        return_value=httpx.Response(200, json={"content": content, "encoding": "base64"})
    )
    await client.get_file_contents("1", "src/main.py")
    assert route.called


@respx.mock
async def test_get_file_contents_passes_ref(client):
    content = base64.b64encode(b"x").decode()
    route = respx.get(_url("/projects/1/repository/files/README.md")).mock(
        return_value=httpx.Response(200, json={"content": content, "encoding": "base64"})
    )
    await client.get_file_contents("1", "README.md", ref="develop")
    assert route.calls[0].request.url.params["ref"] == "develop"


# ---------------------------------------------------------------------------
# list_projects
# ---------------------------------------------------------------------------

@respx.mock
async def test_list_projects_no_search(client):
    route = respx.get(_url("/projects")).mock(
        return_value=httpx.Response(200, json=[{"id": 1}])
    )
    result = await client.list_projects()
    assert result == [{"id": 1}]
    assert "search" not in route.calls[0].request.url.params


@respx.mock
async def test_list_projects_with_search(client):
    route = respx.get(_url("/projects")).mock(
        return_value=httpx.Response(200, json=[])
    )
    await client.list_projects(search="myproject")
    assert route.calls[0].request.url.params["search"] == "myproject"


# ---------------------------------------------------------------------------
# push_files
# ---------------------------------------------------------------------------

@respx.mock
async def test_push_files_basic_payload(client):
    route = respx.post(_url("/projects/1/repository/commits")).mock(
        return_value=httpx.Response(201, json={"id": "abc123"})
    )
    await client.push_files(
        project_id="1",
        branch="main",
        commit_message="Add file",
        files=[{"file_path": "foo.txt", "content": "bar", "action": "create"}],
    )
    body = route.calls[0].request.read()
    import json
    payload = json.loads(body)
    assert payload["branch"] == "main"
    assert payload["commit_message"] == "Add file"
    assert payload["actions"] == [{"action": "create", "file_path": "foo.txt", "content": "bar"}]
    assert "start_branch" not in payload


@respx.mock
async def test_push_files_includes_start_branch(client):
    route = respx.post(_url("/projects/1/repository/commits")).mock(
        return_value=httpx.Response(201, json={"id": "abc123"})
    )
    await client.push_files(
        project_id="1",
        branch="feature/x",
        commit_message="init",
        files=[{"file_path": "a.txt", "content": "a", "action": "create"}],
        start_branch="main",
    )
    import json
    payload = json.loads(route.calls[0].request.read())
    assert payload["start_branch"] == "main"


@respx.mock
async def test_push_files_delete_omits_content(client):
    route = respx.post(_url("/projects/1/repository/commits")).mock(
        return_value=httpx.Response(201, json={"id": "abc123"})
    )
    await client.push_files(
        project_id="1",
        branch="main",
        commit_message="remove file",
        files=[{"file_path": "old.txt", "action": "delete"}],
    )
    import json
    payload = json.loads(route.calls[0].request.read())
    action = payload["actions"][0]
    assert action["action"] == "delete"
    assert "content" not in action


@respx.mock
async def test_push_files_default_action_is_update(client):
    route = respx.post(_url("/projects/1/repository/commits")).mock(
        return_value=httpx.Response(201, json={"id": "abc123"})
    )
    await client.push_files(
        project_id="1",
        branch="main",
        commit_message="edit",
        files=[{"file_path": "README.md", "content": "new content"}],
    )
    import json
    payload = json.loads(route.calls[0].request.read())
    assert payload["actions"][0]["action"] == "update"


# ---------------------------------------------------------------------------
# create_merge_request
# ---------------------------------------------------------------------------

@respx.mock
async def test_create_merge_request_required_fields(client):
    route = respx.post(_url("/projects/1/merge_requests")).mock(
        return_value=httpx.Response(201, json={"iid": 1})
    )
    await client.create_merge_request("1", "My MR", "feature", "main")
    import json
    payload = json.loads(route.calls[0].request.read())
    assert payload["title"] == "My MR"
    assert payload["source_branch"] == "feature"
    assert payload["target_branch"] == "main"
    assert "description" not in payload


@respx.mock
async def test_create_merge_request_with_description(client):
    route = respx.post(_url("/projects/1/merge_requests")).mock(
        return_value=httpx.Response(201, json={"iid": 1})
    )
    await client.create_merge_request("1", "My MR", "feature", "main", description="Details")
    import json
    payload = json.loads(route.calls[0].request.read())
    assert payload["description"] == "Details"


# ---------------------------------------------------------------------------
# create_pipeline
# ---------------------------------------------------------------------------

@respx.mock
async def test_create_pipeline_payload(client):
    route = respx.post(_url("/projects/1/pipeline")).mock(
        return_value=httpx.Response(201, json={"id": 99, "status": "pending"})
    )
    result = await client.create_pipeline("1", "main")
    import json
    payload = json.loads(route.calls[0].request.read())
    assert payload == {"ref": "main"}
    assert result["id"] == 99


# ---------------------------------------------------------------------------
# get_repository_tree
# ---------------------------------------------------------------------------

@respx.mock
async def test_get_repository_tree_defaults(client):
    route = respx.get(_url("/projects/1/repository/tree")).mock(
        return_value=httpx.Response(200, json=[{"name": "src", "type": "tree"}])
    )
    await client.get_repository_tree("1")
    params = route.calls[0].request.url.params
    assert params["ref"] == "main"
    assert params["recursive"] == "false"
    assert "path" not in params


@respx.mock
async def test_get_repository_tree_with_path_and_recursive(client):
    route = respx.get(_url("/projects/1/repository/tree")).mock(
        return_value=httpx.Response(200, json=[])
    )
    await client.get_repository_tree("1", path="src", ref="develop", recursive=True)
    params = route.calls[0].request.url.params
    assert params["path"] == "src"
    assert params["ref"] == "develop"
    assert params["recursive"] == "true"


# ---------------------------------------------------------------------------
# create_branch
# ---------------------------------------------------------------------------

@respx.mock
async def test_create_branch_payload(client):
    route = respx.post(_url("/projects/1/repository/branches")).mock(
        return_value=httpx.Response(201, json={"name": "feature/x"})
    )
    result = await client.create_branch("1", "feature/x", "main")
    import json
    payload = json.loads(route.calls[0].request.read())
    assert payload == {"branch": "feature/x", "ref": "main"}
    assert result["name"] == "feature/x"


@respx.mock
async def test_create_branch_from_sha(client):
    route = respx.post(_url("/projects/1/repository/branches")).mock(
        return_value=httpx.Response(201, json={"name": "hotfix"})
    )
    await client.create_branch("1", "hotfix", "abc123def456")
    import json
    payload = json.loads(route.calls[0].request.read())
    assert payload["ref"] == "abc123def456"


# ---------------------------------------------------------------------------
# _parse_semver (unit tests — no HTTP needed)
# ---------------------------------------------------------------------------

def test_parse_semver_plain():
    assert _parse_semver("1.2.3") == (1, 2, 3)

def test_parse_semver_v_prefix():
    assert _parse_semver("v2.10.0") == (2, 10, 0)

def test_parse_semver_with_prerelease_suffix():
    # Pre-release suffix is ignored; only X.Y.Z is extracted
    assert _parse_semver("v1.2.3-beta.1") == (1, 2, 3)

def test_parse_semver_non_semver():
    assert _parse_semver("release-2024") is None
    assert _parse_semver("") is None
    assert _parse_semver("1.2") is None


# ---------------------------------------------------------------------------
# _next_link (unit tests — no HTTP needed)
# ---------------------------------------------------------------------------

def test_next_link_present():
    header = '<https://gitlab.example.com/api/v4/projects/1/releases?page=2>; rel="next", <https://gitlab.example.com/api/v4/projects/1/releases?page=1>; rel="first"'
    assert _next_link(header) == "https://gitlab.example.com/api/v4/projects/1/releases?page=2"

def test_next_link_absent():
    header = '<https://gitlab.example.com/api/v4/projects/1/releases?page=1>; rel="first"'
    assert _next_link(header) is None

def test_next_link_empty():
    assert _next_link("") is None


# ---------------------------------------------------------------------------
# get_latest_release
# ---------------------------------------------------------------------------

def _release(tag: str) -> dict:
    return {"tag_name": tag, "name": f"Release {tag}"}


@respx.mock
async def test_get_latest_release_returns_highest_semver(client):
    respx.get(_url("/projects/1/releases")).mock(
        return_value=httpx.Response(200, json=[
            _release("v1.0.0"),
            _release("v1.2.0"),
            _release("v1.1.3"),
        ])
    )
    result = await client.get_latest_release("1")
    assert result["tag_name"] == "v1.2.0"


@respx.mock
async def test_get_latest_release_filters_by_major(client):
    respx.get(_url("/projects/1/releases")).mock(
        return_value=httpx.Response(200, json=[
            _release("v2.5.0"),
            _release("v1.9.9"),
            _release("v2.0.1"),
        ])
    )
    result = await client.get_latest_release("1", major_version=1)
    assert result["tag_name"] == "v1.9.9"


@respx.mock
async def test_get_latest_release_skips_non_semver(client):
    respx.get(_url("/projects/1/releases")).mock(
        return_value=httpx.Response(200, json=[
            _release("nightly-build"),
            _release("v0.9.0"),
            _release("release-latest"),
        ])
    )
    result = await client.get_latest_release("1")
    assert result["tag_name"] == "v0.9.0"


@respx.mock
async def test_get_latest_release_returns_none_when_no_match(client):
    respx.get(_url("/projects/1/releases")).mock(
        return_value=httpx.Response(200, json=[
            _release("v1.0.0"),
            _release("v1.5.0"),
        ])
    )
    result = await client.get_latest_release("1", major_version=3)
    assert result is None


@respx.mock
async def test_get_latest_release_returns_none_when_no_semver_tags(client):
    respx.get(_url("/projects/1/releases")).mock(
        return_value=httpx.Response(200, json=[
            _release("nightly"),
            _release("experimental"),
        ])
    )
    result = await client.get_latest_release("1")
    assert result is None


# ---------------------------------------------------------------------------
# Pipeline monitoring
# ---------------------------------------------------------------------------

@respx.mock
async def test_request_text_returns_plain_text(client):
    respx.get(_url("/projects/1/jobs/5/trace")).mock(
        return_value=httpx.Response(200, text="Job log output\nline 2\n")
    )
    result = await client._request_text("GET", "/projects/1/jobs/5/trace")
    assert result == "Job log output\nline 2\n"


@respx.mock
async def test_request_text_raises_on_error(client):
    respx.get(_url("/projects/1/jobs/5/trace")).mock(
        return_value=httpx.Response(403, text="Forbidden")
    )
    with pytest.raises(GitLabError) as exc_info:
        await client._request_text("GET", "/projects/1/jobs/5/trace")
    assert exc_info.value.status_code == 403


@respx.mock
async def test_get_pipeline_url_and_result(client):
    route = respx.get(_url("/projects/1/pipelines/42")).mock(
        return_value=httpx.Response(200, json={"id": 42, "status": "success"})
    )
    result = await client.get_pipeline("1", 42)
    assert route.called
    assert result["id"] == 42
    assert result["status"] == "success"


@respx.mock
async def test_list_pipelines_no_filters(client):
    route = respx.get(_url("/projects/1/pipelines")).mock(
        return_value=httpx.Response(200, json=[{"id": 1}])
    )
    result = await client.list_pipelines("1")
    params = route.calls[0].request.url.params
    assert params["per_page"] == "20"
    assert "ref" not in params
    assert "sha" not in params
    assert "status" not in params
    assert result == [{"id": 1}]


@respx.mock
async def test_list_pipelines_with_sha(client):
    route = respx.get(_url("/projects/1/pipelines")).mock(
        return_value=httpx.Response(200, json=[{"id": 7}])
    )
    await client.list_pipelines("1", sha="abc123")
    assert route.calls[0].request.url.params["sha"] == "abc123"


@respx.mock
async def test_list_pipelines_with_all_filters(client):
    route = respx.get(_url("/projects/1/pipelines")).mock(
        return_value=httpx.Response(200, json=[])
    )
    await client.list_pipelines("1", ref="main", sha="deadbeef", status="failed", per_page=5)
    params = route.calls[0].request.url.params
    assert params["ref"] == "main"
    assert params["sha"] == "deadbeef"
    assert params["status"] == "failed"
    assert params["per_page"] == "5"


@respx.mock
async def test_list_pipeline_jobs_url(client):
    route = respx.get(_url("/projects/1/pipelines/42/jobs")).mock(
        return_value=httpx.Response(200, json=[{"id": 10, "name": "build", "status": "failed"}])
    )
    result = await client.list_pipeline_jobs("1", 42)
    assert route.called
    assert result[0]["name"] == "build"


@respx.mock
async def test_get_job_log_no_truncation(client):
    respx.get(_url("/projects/1/jobs/5/trace")).mock(
        return_value=httpx.Response(200, text="short log")
    )
    result = await client.get_job_log("1", 5, max_chars=50000)
    assert result == "short log"


@respx.mock
async def test_get_job_log_truncates_from_end(client):
    long_log = "A" * 100 + "B" * 10
    respx.get(_url("/projects/1/jobs/5/trace")).mock(
        return_value=httpx.Response(200, text=long_log)
    )
    result = await client.get_job_log("1", 5, max_chars=10)
    assert result.startswith("[Log truncated")
    tail = result.split("\n", 1)[1]
    assert tail == "B" * 10
    assert "A" not in tail


@respx.mock
async def test_get_job_log_default_max_chars(client):
    respx.get(_url("/projects/1/jobs/99/trace")).mock(
        return_value=httpx.Response(200, text="ok")
    )
    result = await client.get_job_log("1", 99)
    assert result == "ok"


@respx.mock
async def test_get_latest_release_follows_pagination(client):
    releases_url = _url("/projects/1/releases")
    page2_url = releases_url + "?page=2&per_page=100"

    # Use a side_effect iterator so the same route returns different responses
    # on successive calls (first call returns page 1 with a Link: next header,
    # second call returns page 2 with no Link header).
    pages = iter([
        httpx.Response(
            200,
            json=[_release("v1.0.0"), _release("v1.1.0")],
            headers={"link": f'<{page2_url}>; rel="next"'},
        ),
        httpx.Response(200, json=[_release("v1.3.0"), _release("v1.2.0")]),
    ])
    respx.get(releases_url).mock(side_effect=lambda _req: next(pages))
    result = await client.get_latest_release("1")
    assert result["tag_name"] == "v1.3.0"


# ---------------------------------------------------------------------------
# get_merge_request
# ---------------------------------------------------------------------------

@respx.mock
async def test_get_merge_request_url_and_result(client):
    route = respx.get(_url("/projects/1/merge_requests/5")).mock(
        return_value=httpx.Response(200, json={"iid": 5, "title": "My MR", "state": "opened"})
    )
    result = await client.get_merge_request("1", 5)
    assert route.called
    assert result["iid"] == 5
    assert result["title"] == "My MR"
    assert result["state"] == "opened"


@respx.mock
async def test_get_merge_request_404_raises(client):
    respx.get(_url("/projects/1/merge_requests/99")).mock(
        return_value=httpx.Response(404, json={"message": "Not found"})
    )
    with pytest.raises(GitLabError) as exc_info:
        await client.get_merge_request("1", 99)
    assert exc_info.value.status_code == 404


@respx.mock
async def test_get_merge_request_encoded_project_id(client):
    route = respx.get(_url("/projects/group%2Fproject/merge_requests/5")).mock(
        return_value=httpx.Response(200, json={"iid": 5})
    )
    await client.get_merge_request("group%2Fproject", 5)
    assert route.called


# ---------------------------------------------------------------------------
# get_merge_request_diffs
# ---------------------------------------------------------------------------

@respx.mock
async def test_get_merge_request_diffs_single_page(client):
    diffs = [
        {"old_path": "a.py", "new_path": "a.py", "diff": "@@ -1 +1 @@\n-old\n+new", "new_file": False, "deleted_file": False, "renamed_file": False},
        {"old_path": "b.py", "new_path": "b.py", "diff": "@@ -1 +1 @@\n-x\n+y", "new_file": False, "deleted_file": False, "renamed_file": False},
    ]
    route = respx.get(_url("/projects/1/merge_requests/5/diffs")).mock(
        return_value=httpx.Response(200, json=diffs)
    )
    result = await client.get_merge_request_diffs("1", 5)
    assert route.calls[0].request.url.params["per_page"] == "100"
    assert len(result) == 2
    assert result[0]["old_path"] == "a.py"


@respx.mock
async def test_get_merge_request_diffs_follows_pagination(client):
    diffs_url = _url("/projects/1/merge_requests/5/diffs")
    page2_url = diffs_url + "?page=2&per_page=100"

    diff1 = {"old_path": "a.py", "new_path": "a.py", "diff": "@@ -1 +1 @@\n-old\n+new", "new_file": False, "deleted_file": False, "renamed_file": False}
    diff2 = {"old_path": "b.py", "new_path": "b.py", "diff": "@@ -1 +1 @@\n-x\n+y", "new_file": False, "deleted_file": False, "renamed_file": False}

    pages = iter([
        httpx.Response(200, json=[diff1], headers={"link": f'<{page2_url}>; rel="next"'}),
        httpx.Response(200, json=[diff2]),
    ])
    respx.get(diffs_url).mock(side_effect=lambda _req: next(pages))
    result = await client.get_merge_request_diffs("1", 5)
    assert len(result) == 2
    assert result[0]["old_path"] == "a.py"
    assert result[1]["old_path"] == "b.py"


@respx.mock
async def test_get_merge_request_diffs_404_raises(client):
    respx.get(_url("/projects/1/merge_requests/99/diffs")).mock(
        return_value=httpx.Response(404, json={"message": "Not found"})
    )
    with pytest.raises(GitLabError) as exc_info:
        await client.get_merge_request_diffs("1", 99)
    assert exc_info.value.status_code == 404


# ---------------------------------------------------------------------------
# list_merge_requests
# ---------------------------------------------------------------------------

@respx.mock
async def test_list_merge_requests_default_params(client):
    route = respx.get(_url("/projects/1/merge_requests")).mock(
        return_value=httpx.Response(200, json=[{"iid": 1}, {"iid": 2}])
    )
    result = await client.list_merge_requests("1")
    params = route.calls[0].request.url.params
    assert params["state"] == "opened"
    assert params["per_page"] == "20"
    assert result == [{"iid": 1}, {"iid": 2}]


@respx.mock
async def test_list_merge_requests_custom_state_and_per_page(client):
    route = respx.get(_url("/projects/1/merge_requests")).mock(
        return_value=httpx.Response(200, json=[{"iid": 10}])
    )
    await client.list_merge_requests("1", state="merged", per_page=5)
    params = route.calls[0].request.url.params
    assert params["state"] == "merged"
    assert params["per_page"] == "5"


@respx.mock
async def test_list_merge_requests_state_all(client):
    route = respx.get(_url("/projects/1/merge_requests")).mock(
        return_value=httpx.Response(200, json=[])
    )
    await client.list_merge_requests("1", state="all")
    assert route.calls[0].request.url.params["state"] == "all"


@respx.mock
async def test_list_merge_requests_encoded_project_id(client):
    route = respx.get(_url("/projects/mygroup%2Fmyrepo/merge_requests")).mock(
        return_value=httpx.Response(200, json=[])
    )
    await client.list_merge_requests("mygroup%2Fmyrepo")
    assert route.called


@respx.mock
async def test_list_merge_requests_404_raises(client):
    respx.get(_url("/projects/1/merge_requests")).mock(
        return_value=httpx.Response(404, json={"message": "Not found"})
    )
    with pytest.raises(GitLabError) as exc_info:
        await client.list_merge_requests("1")
    assert exc_info.value.status_code == 404


# ---------------------------------------------------------------------------
# get_mr_discussions
# ---------------------------------------------------------------------------

@respx.mock
async def test_get_mr_discussions_url_and_result(client):
    discussions = [{"id": "abc", "notes": [{"body": "LGTM", "author": {"name": "Alice"}}]}]
    route = respx.get(_url("/projects/1/merge_requests/5/discussions")).mock(
        return_value=httpx.Response(200, json=discussions)
    )
    result = await client.get_mr_discussions("1", 5)
    assert route.calls[0].request.url.params["per_page"] == "100"
    assert len(result) == 1
    assert result[0]["id"] == "abc"


@respx.mock
async def test_get_mr_discussions_follows_pagination(client):
    discussions_url = _url("/projects/1/merge_requests/5/discussions")
    page2_url = discussions_url + "?page=2&per_page=100"

    d1 = {"id": "aaa", "notes": [{"body": "First comment"}]}
    d2 = {"id": "bbb", "notes": [{"body": "Second comment"}]}

    pages = iter([
        httpx.Response(200, json=[d1], headers={"link": f'<{page2_url}>; rel="next"'}),
        httpx.Response(200, json=[d2]),
    ])
    respx.get(discussions_url).mock(side_effect=lambda _req: next(pages))
    result = await client.get_mr_discussions("1", 5)
    assert len(result) == 2
    assert result[0]["id"] == "aaa"
    assert result[1]["id"] == "bbb"


@respx.mock
async def test_get_mr_discussions_404_raises(client):
    respx.get(_url("/projects/1/merge_requests/5/discussions")).mock(
        return_value=httpx.Response(404, json={"message": "Not found"})
    )
    with pytest.raises(GitLabError) as exc_info:
        await client.get_mr_discussions("1", 5)
    assert exc_info.value.status_code == 404


# ---------------------------------------------------------------------------
# _annotate_large_diffs (pure function — no HTTP)
# ---------------------------------------------------------------------------

def test_annotate_large_diffs_replaces_too_large_diff():
    from gitlab_mcp_server.server import _annotate_large_diffs
    diffs = [
        {"new_path": "big.py", "diff": "", "too_large": True},
        {"new_path": "small.py", "diff": "@@ -1 +1 @@\n-old\n+new", "too_large": False},
    ]
    result = _annotate_large_diffs(diffs)
    assert "get_file_contents" in result[0]["diff"]
    assert "big.py" in result[0]["diff"]
    assert result[1]["diff"] == "@@ -1 +1 @@\n-old\n+new"


def test_annotate_large_diffs_no_mutation_when_not_too_large():
    from gitlab_mcp_server.server import _annotate_large_diffs
    diffs = [{"new_path": "a.py", "diff": "some diff", "too_large": False}]
    result = _annotate_large_diffs(diffs)
    assert result[0]["diff"] == "some diff"


# ---------------------------------------------------------------------------
# _shape_mr (pure function — no HTTP)
# ---------------------------------------------------------------------------

def test_shape_mr_filters_to_review_fields():
    from gitlab_mcp_server.server import _shape_mr
    mr = {
        "iid": 5,
        "title": "Fix bug",
        "description": "Fixes #123",
        "state": "opened",
        "author": {"name": "Alice", "username": "alice", "id": 999, "avatar_url": "http://..."},
        "assignees": [{"name": "Bob", "username": "bob", "id": 888, "avatar_url": "http://..."}],
        "reviewers": [],
        "source_branch": "fix/bug",
        "target_branch": "main",
        "labels": ["bug"],
        "web_url": "https://gitlab.example.com/project/-/merge_requests/5",
        "created_at": "2024-01-01T00:00:00Z",
        "updated_at": "2024-01-02T00:00:00Z",
        "diff_refs": {"base_sha": "abc", "head_sha": "def", "start_sha": "ghi"},
        "merge_status": "can_be_merged",
        "blocking_discussions_resolved": True,
        "changes_count": "3",
        "head_pipeline": {"id": 42, "status": "success", "web_url": "http://pipeline"},
        # Noise fields that should be stripped
        "merge_commit_sha": "xyz",
        "squash": False,
        "force_remove_source_branch": None,
        "user": {"can_merge": True},
    }
    result = _shape_mr(mr)
    assert result["iid"] == 5
    assert result["title"] == "Fix bug"
    assert result["source_branch"] == "fix/bug"
    assert "merge_commit_sha" not in result
    assert "squash" not in result
    assert "user" not in result
    assert result["author"] == {"name": "Alice", "username": "alice"}
    assert "id" not in result["author"]
    assert result["assignees"] == [{"name": "Bob", "username": "bob"}]
    assert result["head_pipeline"] == {"status": "success", "web_url": "http://pipeline"}


def test_shape_mr_handles_null_head_pipeline():
    from gitlab_mcp_server.server import _shape_mr
    mr = {
        "iid": 1, "title": "T", "description": None, "state": "opened",
        "author": {"name": "A", "username": "a"},
        "assignees": [], "reviewers": [], "source_branch": "feat", "target_branch": "main",
        "labels": [], "web_url": "http://...", "created_at": "", "updated_at": "",
        "diff_refs": None, "merge_status": "can_be_merged",
        "blocking_discussions_resolved": True, "changes_count": "1",
        "head_pipeline": None,
    }
    result = _shape_mr(mr)
    assert result["head_pipeline"] is None
