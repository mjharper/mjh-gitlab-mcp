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
