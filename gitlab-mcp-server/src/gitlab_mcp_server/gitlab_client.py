import base64
import os
import re
from typing import Any

import httpx

_SEMVER_RE = re.compile(r"^v?(\d+)\.(\d+)\.(\d+)")


def _parse_semver(tag: str) -> tuple[int, int, int] | None:
    m = _SEMVER_RE.match(tag)
    if m:
        return (int(m.group(1)), int(m.group(2)), int(m.group(3)))
    return None


def _next_link(link_header: str) -> str | None:
    """Extract the URL from a Link header's rel="next" entry, if present."""
    for part in link_header.split(","):
        part = part.strip()
        if 'rel="next"' in part:
            return part.split(";")[0].strip().strip("<>")
    return None


class GitLabError(Exception):
    def __init__(self, status_code: int, message: str) -> None:
        self.status_code = status_code
        super().__init__(f"GitLab API error {status_code}: {message}")


class GitLabClient:
    def __init__(self) -> None:
        base_url = os.environ.get("GITLAB_API_URL", "").rstrip("/")
        token = os.environ.get("GITLAB_PERSONAL_ACCESS_TOKEN", "")

        if not base_url:
            raise RuntimeError("GITLAB_API_URL environment variable is not set")
        if not token:
            raise RuntimeError("GITLAB_PERSONAL_ACCESS_TOKEN environment variable is not set")

        self._client = httpx.AsyncClient(
            base_url=base_url,
            headers={"PRIVATE-TOKEN": token},
            timeout=30.0,
        )

    async def _request(self, method: str, path: str, **kwargs: Any) -> Any:
        response = await self._client.request(method, path, **kwargs)
        if not response.is_success:
            raise GitLabError(response.status_code, response.text)
        return response.json()

    async def list_releases(self, project_id: str, per_page: int = 20) -> Any:
        return await self._request(
            "GET",
            f"/projects/{project_id}/releases",
            params={"per_page": per_page},
        )

    async def get_file_contents(
        self, project_id: str, file_path: str, ref: str = "main"
    ) -> str:
        import urllib.parse
        encoded_path = urllib.parse.quote(file_path, safe="")
        data = await self._request(
            "GET",
            f"/projects/{project_id}/repository/files/{encoded_path}",
            params={"ref": ref},
        )
        raw = data.get("content", "")
        encoding = data.get("encoding", "base64")
        if encoding == "base64":
            return base64.b64decode(raw).decode("utf-8", errors="replace")
        return raw

    async def list_projects(
        self, search: str | None = None, per_page: int = 20
    ) -> Any:
        params: dict[str, Any] = {"per_page": per_page}
        if search:
            params["search"] = search
        return await self._request("GET", "/projects", params=params)

    async def push_files(
        self,
        project_id: str,
        branch: str,
        commit_message: str,
        files: list[dict[str, str]],
        start_branch: str | None = None,
    ) -> Any:
        actions = [
            {
                "action": f.get("action", "update"),
                "file_path": f["file_path"],
                **( {"content": f["content"]} if f.get("action", "update") != "delete" else {} ),
            }
            for f in files
        ]
        payload: dict[str, Any] = {
            "branch": branch,
            "commit_message": commit_message,
            "actions": actions,
        }
        if start_branch:
            payload["start_branch"] = start_branch
        return await self._request(
            "POST",
            f"/projects/{project_id}/repository/commits",
            json=payload,
        )

    async def create_merge_request(
        self,
        project_id: str,
        title: str,
        source_branch: str,
        target_branch: str,
        description: str | None = None,
    ) -> Any:
        payload: dict[str, Any] = {
            "title": title,
            "source_branch": source_branch,
            "target_branch": target_branch,
        }
        if description:
            payload["description"] = description
        return await self._request(
            "POST",
            f"/projects/{project_id}/merge_requests",
            json=payload,
        )

    async def create_pipeline(self, project_id: str, ref: str) -> Any:
        return await self._request(
            "POST",
            f"/projects/{project_id}/pipeline",
            json={"ref": ref},
        )

    async def get_repository_tree(
        self,
        project_id: str,
        path: str = "",
        ref: str = "main",
        recursive: bool = False,
    ) -> Any:
        params: dict[str, Any] = {
            "ref": ref,
            "recursive": recursive,
        }
        if path:
            params["path"] = path
        return await self._request(
            "GET",
            f"/projects/{project_id}/repository/tree",
            params=params,
        )

    async def create_branch(
        self, project_id: str, branch: str, ref: str
    ) -> Any:
        return await self._request(
            "POST",
            f"/projects/{project_id}/repository/branches",
            json={"branch": branch, "ref": ref},
        )

    async def _get_all_pages(self, path: str, params: dict[str, Any]) -> list[Any]:
        """Fetch all pages of a GET endpoint, following Link: next headers."""
        response = await self._client.get(path, params={**params, "per_page": 100})
        if not response.is_success:
            raise GitLabError(response.status_code, response.text)
        results: list[Any] = response.json()
        while next_url := _next_link(response.headers.get("link", "")):
            response = await self._client.get(next_url)
            if not response.is_success:
                raise GitLabError(response.status_code, response.text)
            results.extend(response.json())
        return results

    async def get_latest_release(
        self, project_id: str, major_version: int | None = None
    ) -> dict[str, Any] | None:
        releases = await self._get_all_pages(
            f"/projects/{project_id}/releases", params={}
        )
        candidates: list[tuple[tuple[int, int, int], dict[str, Any]]] = []
        for release in releases:
            version = _parse_semver(release.get("tag_name", ""))
            if version is None:
                continue
            if major_version is not None and version[0] != major_version:
                continue
            candidates.append((version, release))
        if not candidates:
            return None
        candidates.sort(key=lambda x: x[0], reverse=True)
        return candidates[0][1]

    async def aclose(self) -> None:
        await self._client.aclose()
