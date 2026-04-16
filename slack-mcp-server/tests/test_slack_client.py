import pytest
import respx
import httpx

from slack_mcp_server.slack_client import SlackClient, SlackError


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setenv("SLACK_BOT_TOKEN", "xoxb-test-token")
    return SlackClient()


@respx.mock
async def test_send_message(client):
    respx.post("https://slack.com/api/chat.postMessage").mock(
        return_value=httpx.Response(
            200,
            json={"ok": True, "channel": "C123", "ts": "1234567890.000001"},
        )
    )

    result = await client.send_message("C123", "Hello, world!")

    assert result["ok"] is True
    assert result["channel"] == "C123"


@respx.mock
async def test_send_formatted_message(client):
    blocks = [{"type": "section", "text": {"type": "mrkdwn", "text": "*Hello*"}}]
    respx.post("https://slack.com/api/chat.postMessage").mock(
        return_value=httpx.Response(
            200,
            json={"ok": True, "channel": "C123", "ts": "1234567890.000002"},
        )
    )

    result = await client.send_formatted_message("C123", blocks, "Hello")

    assert result["ok"] is True


@respx.mock
async def test_send_message_slack_error(client):
    respx.post("https://slack.com/api/chat.postMessage").mock(
        return_value=httpx.Response(200, json={"ok": False, "error": "channel_not_found"})
    )

    with pytest.raises(SlackError, match="channel_not_found"):
        await client.send_message("C_BAD", "Hello")


@respx.mock
async def test_send_message_http_error(client):
    respx.post("https://slack.com/api/chat.postMessage").mock(
        return_value=httpx.Response(500, text="Internal Server Error")
    )

    with pytest.raises(SlackError) as exc_info:
        await client.send_message("C123", "Hello")

    assert exc_info.value.status_code == 500


def test_missing_token(monkeypatch):
    monkeypatch.delenv("SLACK_BOT_TOKEN", raising=False)
    with pytest.raises(RuntimeError, match="SLACK_BOT_TOKEN"):
        SlackClient()
