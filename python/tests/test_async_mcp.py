"""Tests for the async MCP resource."""

import asyncio
import json
import typing as t
import warnings
from unittest.mock import AsyncMock, Mock

import httpx
import pytest
from composio_client import ProjectAuth

from composio import ComposioDeprecationWarning
from composio.client import AsyncHttpClient
from composio.core.models import mcp as mcp_module
from composio.core.models._async.mcp import AsyncMCP
from composio.exceptions import ValidationError


def _server():
    response = Mock()
    response.id = "mcp_123"
    response.name = "test-server"
    response.allowed_tools = ["GITHUB_CREATE_ISSUE"]
    response.auth_config_ids = ["ac_456"]
    return response


@pytest.fixture
def mcp() -> AsyncMCP:
    client = Mock()
    client.mcp.update = AsyncMock(return_value=_server())
    client.mcp.custom.create = AsyncMock(return_value=_server())
    client.mcp.list = AsyncMock()
    client.mcp.retrieve = AsyncMock(return_value=_server())
    client.mcp.delete = AsyncMock()
    client.mcp.generate.url = AsyncMock(
        return_value=Mock(user_ids_url=["https://mcp.test/u"])
    )
    return AsyncMCP(client)


@pytest.fixture
def fresh_managed_auth_notice(monkeypatch):
    monkeypatch.setattr(mcp_module, "_managed_auth_deprecation_warned", False)


def _sdk_notices(caught):
    return [
        w
        for w in caught
        if w.category is ComposioDeprecationWarning
        and "include_composio_helper_actions" in str(w.message)
    ]


def test_update_sends_tool_only_updates_via_allowed_tools(mcp):
    asyncio.run(mcp.update("mcp_123", allowed_tools=["GITHUB_CREATE_ISSUE"]))

    mcp._client.mcp.update.assert_awaited_once_with(
        "mcp_123", allowed_tools=["GITHUB_CREATE_ISSUE"]
    )


def test_update_dedupes_toolkits_and_inverts_manual_connections(mcp):
    asyncio.run(
        mcp.update(
            "mcp_123",
            name="renamed",
            toolkits=[
                "github",
                {"toolkit": "github", "auth_config_id": "ac_1"},
                {"toolkit": "slack", "auth_config_id": "ac_1"},
            ],
            manually_manage_connections=True,
        )
    )

    mcp._client.mcp.update.assert_awaited_once_with(
        "mcp_123",
        name="renamed",
        toolkits=["github", "slack"],
        auth_config_ids=["ac_1"],
        managed_auth_via_composio=False,
    )


def test_create_sends_allowed_tools_and_attaches_awaitable_generate(
    mcp, fresh_managed_auth_notice
):
    async def run():
        server = await mcp.create(
            "server", toolkits=["github"], allowed_tools=["GITHUB_CREATE_ISSUE"]
        )
        return await server.generate("user_1", manually_manage_connections=True)

    instance = asyncio.run(run())

    kwargs = mcp._client.mcp.custom.create.await_args.kwargs
    assert kwargs == {
        "name": "server",
        "toolkits": ["github"],
        "auth_config_ids": [],
        "allowed_tools": ["GITHUB_CREATE_ISSUE"],
        "managed_auth_via_composio": True,
    }
    assert instance == {
        "id": "mcp_123",
        "name": "test-server",
        "type": "streamable_http",
        "url": "https://mcp.test/u",
        "user_id": "user_1",
        "allowed_tools": ["GITHUB_CREATE_ISSUE"],
        "auth_configs": ["ac_456"],
    }
    mcp._client.mcp.retrieve.assert_awaited_once_with("mcp_123")


def test_create_requires_a_toolkit(mcp):
    with pytest.raises(ValidationError, match="At least one toolkit"):
        asyncio.run(mcp.create("server", toolkits=[]))
    mcp._client.mcp.custom.create.assert_not_awaited()


def test_api_failures_are_wrapped(mcp):
    for name in ("list", "retrieve", "update", "delete"):
        getattr(mcp._client.mcp, name).side_effect = RuntimeError("boom")
    mcp._client.mcp.custom.create.side_effect = RuntimeError("boom")

    for call, message in (
        (lambda: mcp.create("s", toolkits=["github"]), "Failed to create"),
        (lambda: mcp.list(), "Failed to list"),
        (lambda: mcp.get("mcp_1"), "Failed to retrieve MCP server mcp_1"),
        (lambda: mcp.update("mcp_1", name="n"), "Failed to update MCP server mcp_1"),
        (lambda: mcp.delete("mcp_1"), "Failed to delete MCP server mcp_1"),
        (lambda: mcp.generate("u", "mcp_1"), "Failed to parse MCP server instance"),
    ):
        with pytest.raises(ValidationError, match=message) as raised:
            asyncio.run(call())
        assert isinstance(raised.value.__cause__, RuntimeError)


def test_list_normalizes_the_page(mcp):
    mcp._client.mcp.list.return_value = Mock(items=["a"], current_page=2, total_pages=3)

    assert asyncio.run(mcp.list(page_no=2, toolkits="github")) == {
        "items": ["a"],
        "current_page": 2,
        "total_pages": 3,
    }


def test_delete_reports_the_server_id(mcp):
    mcp._client.mcp.delete.return_value = Mock(deleted=True)

    assert asyncio.run(mcp.delete("mcp_1")) == {"id": "mcp_1", "deleted": True}


def test_generate_without_a_url_is_a_validation_error(mcp):
    mcp._client.mcp.generate.url.return_value = Mock(user_ids_url=[])

    with pytest.raises(ValidationError, match="No server URL generated"):
        asyncio.run(mcp.generate("u", "mcp_1", manually_manage_connections=True))


def _transport_mcp(requests: t.List[httpx.Request]) -> AsyncMCP:
    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path.endswith("/generate"):
            return httpx.Response(
                200,
                json={
                    "mcp_url": "https://mcp.test",
                    "connected_account_urls": [],
                    "user_ids_url": ["https://mcp.test/u"],
                },
            )
        return httpx.Response(
            200,
            json={
                "id": "mcp_123",
                "name": "server",
                "allowed_tools": [],
                "auth_config_ids": [],
            },
        )

    return AsyncMCP(
        AsyncHttpClient(
            provider="test",
            auth=ProjectAuth("ak_test"),
            base_url="https://backend.composio.dev",
            http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        )
    )


def test_generate_warns_once_per_process_at_the_callers_line(
    fresh_managed_auth_notice,
):
    requests: t.List[httpx.Request] = []

    async def run():
        first, second = _transport_mcp(requests), _transport_mcp(requests)
        server = await first.generate("user_123", "mcp_123")
        await first.generate("user_123", "mcp_123")
        await second.generate("user_123", "mcp_123")
        return server

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        server = asyncio.run(run())

    assert server["url"] == "https://mcp.test/u"
    notices = _sdk_notices(caught)
    assert len(notices) == 1
    assert notices[0].filename == __file__
    assert json.loads(requests[1].content)["managed_auth_by_composio"] is True


def test_generate_without_managed_auth_omits_field_and_does_not_warn(
    fresh_managed_auth_notice,
):
    requests: t.List[httpx.Request] = []

    async def run():
        await _transport_mcp(requests).generate(
            "user_123", "mcp_123", manually_manage_connections=True
        )

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        asyncio.run(run())

    assert caught == []
    assert "managed_auth_by_composio" not in json.loads(requests[-1].content)
