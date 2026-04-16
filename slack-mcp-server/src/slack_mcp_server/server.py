import json
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator

from mcp.server.fastmcp import Context, FastMCP

from .slack_client import SlackClient, SlackError


@asynccontextmanager
async def lifespan(server: FastMCP) -> AsyncIterator[dict[str, Any]]:
    client = SlackClient()
    try:
        yield {"client": client}
    finally:
        await client.aclose()


mcp = FastMCP("slack-mcp-server", lifespan=lifespan)


def _get_client(ctx: Context) -> SlackClient:
    return ctx.request_context.lifespan_context["client"]


def _fmt_error(e: SlackError) -> str:
    return str(e)


@mcp.tool()
async def send_message(channel: str, text: str, ctx: Context = None) -> str:  # type: ignore[assignment]
    """Send a plain text message to a Slack channel or user.

    Args:
        channel: Channel ID (e.g. C1234567890), channel name (e.g. #general), or user ID for DMs.
        text: The message text to send.
    """
    try:
        result = await _get_client(ctx).send_message(channel, text)
        return json.dumps(result, indent=2)
    except SlackError as e:
        return _fmt_error(e)


@mcp.tool()
async def send_formatted_message(
    channel: str,
    blocks: list[dict[str, Any]],
    text: str,
    ctx: Context = None,  # type: ignore[assignment]
) -> str:
    """Send a Block Kit formatted message to a Slack channel or user.

    Args:
        channel: Channel ID (e.g. C1234567890), channel name (e.g. #general), or user ID for DMs.
        blocks: List of Slack Block Kit block objects.
        text: Fallback text shown in notifications and accessibility contexts.
    """
    try:
        result = await _get_client(ctx).send_formatted_message(channel, blocks, text)
        return json.dumps(result, indent=2)
    except SlackError as e:
        return _fmt_error(e)


def main() -> None:
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
