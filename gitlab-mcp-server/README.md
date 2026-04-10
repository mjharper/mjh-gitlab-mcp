# gitlab-mcp-server

A lightweight [Model Context Protocol](https://modelcontextprotocol.io/) (MCP) server that exposes a small subset of the GitLab REST API v4 as tools. Designed for use with VS Code in agent mode and Claude Code against self-managed GitLab instances.

## Tools

| Tool | Description |
|---|---|
| `list_projects` | List / search projects accessible to the authenticated user |
| `list_releases` | List releases for a project |
| `get_file_contents` | Read a single file from a repository (base64-decoded) |
| `get_repository_tree` | List files and directories in a repository |
| `push_files` | Atomic multi-file commit, with optional branch creation in one call |
| `create_merge_request` | Open a merge request |
| `create_pipeline` | Trigger a CI/CD pipeline on a branch or tag |

## Requirements

- Python 3.12+
- [uv](https://docs.astral.sh/uv/) for dependency management

## Installation

```bash
# Install from the project directory
cd gitlab-mcp-server
uv pip install -e .
```

Or install directly without cloning using `uvx`:

```bash
uvx --from . gitlab-mcp-server
```

## Configuration

The server reads two environment variables at startup:

| Variable | Description |
|---|---|
| `GITLAB_API_URL` | Base URL of the GitLab API, e.g. `https://gitlab.example.com/api/v4` |
| `GITLAB_PERSONAL_ACCESS_TOKEN` | A personal access token with `api` scope |

## Running

```bash
GITLAB_API_URL=https://gitlab.example.com/api/v4 \
GITLAB_PERSONAL_ACCESS_TOKEN=glpat-xxxx \
gitlab-mcp-server
```

The server communicates over **stdio** only.

## VS Code / Claude Code integration

### `.vscode/mcp.json`

```json
{
  "servers": {
    "gitlab": {
      "type": "stdio",
      "command": "uv",
      "args": [
        "--directory",
        "/path/to/gitlab-mcp-server",
        "run",
        "gitlab-mcp-server"
      ],
      "env": {
        "GITLAB_API_URL": "https://gitlab.example.com/api/v4",
        "GITLAB_PERSONAL_ACCESS_TOKEN": "${input:gitlabToken}"
      }
    }
  },
  "inputs": [
    {
      "id": "gitlabToken",
      "type": "promptString",
      "description": "GitLab Personal Access Token",
      "password": true
    }
  ]
}
```

The `${input:gitlabToken}` placeholder causes VS Code to prompt for the token once per session so it is never stored on disk.

### Claude Code (`~/.claude/mcp.json` or project `.mcp.json`)

```json
{
  "mcpServers": {
    "gitlab": {
      "command": "uv",
      "args": [
        "--directory",
        "/path/to/gitlab-mcp-server",
        "run",
        "gitlab-mcp-server"
      ],
      "env": {
        "GITLAB_API_URL": "https://gitlab.example.com/api/v4",
        "GITLAB_PERSONAL_ACCESS_TOKEN": "glpat-xxxx"
      }
    }
  }
}
```

## Notes on `push_files`

`push_files` wraps the GitLab [Create a commit](https://docs.gitlab.com/ee/api/commits.html#create-a-commit-with-multiple-files-and-actions) endpoint. Supplying `start_branch` (the branch to branch off from) together with a new `branch` name creates the branch **and** commits all file changes in a **single API call**. This means only one pipeline is triggered — the primary design goal of this tool.

Example:

```json
{
  "project_id": "42",
  "branch": "feature/my-change",
  "start_branch": "main",
  "commit_message": "Add feature X",
  "files": [
    {
      "action": "create",
      "file_path": "src/feature_x.py",
      "content": "# feature X\n"
    },
    {
      "action": "update",
      "file_path": "README.md",
      "content": "# Updated README\n"
    }
  ]
}
```
