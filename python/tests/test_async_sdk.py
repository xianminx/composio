"""AsyncComposio end to end over a mocked transport."""

import asyncio
import inspect
import json
import os
import typing as t
from unittest.mock import patch

import httpx
import pytest

from composio import AsyncComposio, Composio, exceptions
from composio.core.models._async import (
    AsyncToolRouterSession,
    AsyncWebhookSubscriptions,
)

BASE_URL = "https://backend.composio.dev"
MCP_URL = f"{BASE_URL}/v3/mcp/session_123"

SESSION = {
    "session_id": "session_123",
    "mcp": {"type": "http", "url": MCP_URL},
    "config": {
        "user_id": "user_123",
        "execute": {},
        "search": {},
        "preload": {"tools": []},
    },
    "config_version": 3,
    "warnings": [],
}
TOOL = {
    "slug": "GITHUB_GET_USER",
    "name": "Get user",
    "description": "Get a user",
    "input_parameters": {"type": "object", "properties": {}},
    "output_parameters": {"type": "object", "properties": {}},
    "toolkit": {"slug": "github", "name": "GitHub"},
    "version": "20250101_00",
    "available_versions": ["20250101_00"],
    "tags": [],
    "no_auth": False,
    "is_deprecated": False,
    "scopes": [],
    "deprecated": {
        "available_versions": [],
        "display_name": "Get user",
        "is_deprecated": False,
        "toolkit": {"logo": ""},
        "version": "20250101_00",
    },
}


def _sdk(**kwargs: t.Any) -> t.Tuple[AsyncComposio, t.List[httpx.Request]]:
    requests: t.List[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        path = request.url.path
        if path.endswith("/tool_router/session"):
            return httpx.Response(200, json=SESSION)
        if "/tools/execute/" in path:
            return httpx.Response(
                200,
                json={
                    "data": {"login": "octocat"},
                    "error": None,
                    "successful": True,
                    "log_id": "log_1",
                },
            )
        if path.endswith("/tools/GITHUB_GET_USER"):
            return httpx.Response(200, json=TOOL)
        if path.endswith("/tools"):
            return httpx.Response(
                200, json={"items": [TOOL], "total_pages": 1, "next_cursor": None}
            )
        if path.endswith("/webhook_subscriptions"):
            if request.method == "GET":
                return httpx.Response(200, json={"items": [], "next_cursor": None})
            return httpx.Response(
                200,
                json={
                    "id": "ws_1",
                    "webhook_url": "https://example.com/hook",
                    "version": "V3",
                    "enabled_events": ["composio.trigger.message"],
                    "secret": "whsec",
                    "created_at": "2026-01-01T00:00:00Z",
                    "updated_at": "2026-01-01T00:00:00Z",
                },
            )
        return httpx.Response(200, json={})

    kwargs.setdefault("api_key", "ak_test")
    sdk = AsyncComposio(
        base_url=BASE_URL,
        allow_tracking=False,
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        **kwargs,
    )
    return sdk, requests


class TestConstruction:
    def test_resolves_credentials_like_the_sync_sdk(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(exceptions.ApiKeyNotProvidedError):
                AsyncComposio()
            with pytest.raises(exceptions.UserApiKeyNotProvidedError):
                AsyncComposio(disable_api_key=True)
            with pytest.raises(exceptions.InvalidParams):
                AsyncComposio(api_key="ak", org_id="org_only")
        with patch.dict(os.environ, {"COMPOSIO_API_KEY": "ak_env"}, clear=True):
            sdk = AsyncComposio()
            assert sdk.client.auth_mode == Composio().client.auth_mode

    def test_exposes_every_resource_the_sync_sdk_does(self) -> None:
        sync_sdk = Composio(api_key="ak")
        async_sdk = AsyncComposio(api_key="ak")
        sync_names = {name for name in vars(sync_sdk) if not name.startswith("_")} | {
            "sessions",
            "client",
        }
        missing = {name for name in sync_names if not hasattr(async_sdk, name)}
        assert missing == set()
        assert async_sdk.custom_toolkits is async_sdk.experimental.custom_toolkits

    @pytest.mark.parametrize(
        "resource",
        [
            "tools",
            "toolkits",
            "auth_configs",
            "connected_accounts",
            "mcp",
            "logs",
            "keyring",
            "sessions",
        ],
    )
    def test_public_methods_match_the_sync_resource(self, resource: str) -> None:
        sync_resource = getattr(Composio(api_key="ak"), resource)
        async_resource = getattr(AsyncComposio(api_key="ak"), resource)
        # Read statically: `Tools` never initialises `WithLogger`, so its
        # `logger` property raises when touched.
        names = {
            name
            for name in dir(sync_resource)
            if not name.startswith("_")
            and callable(inspect.getattr_static(sync_resource, name, None))
        } | {
            name
            for name, value in vars(sync_resource).items()
            if not name.startswith("_") and callable(value)
        }
        missing = {name for name in names if not hasattr(async_resource, name)}
        assert missing == set()


class TestRequests:
    def test_session_create_and_tools(self) -> None:
        sdk, requests = _sdk()

        async def run() -> t.Tuple[AsyncToolRouterSession, t.Any]:
            async with sdk as composio:
                session = await composio.create(user_id="user_123")
                via_namespace = await composio.sessions.create(user_id="user_123")
                assert via_namespace.session_id == session.session_id
                return session, composio.client

        session, client = asyncio.run(run())
        assert isinstance(session, AsyncToolRouterSession)
        assert session.session_id == "session_123"
        assert requests[0].method == "POST"
        assert requests[0].headers["x-api-key"] == "ak_test"
        assert json.loads(requests[0].content)["user_id"] == "user_123"
        assert client.is_closed()

    def test_tools_get_and_execute(self) -> None:
        sdk, requests = _sdk(toolkit_versions={"github": "20250101_00"})

        async def run() -> t.Tuple[t.Any, t.Any]:
            tools = await sdk.tools.get(user_id="user_123", tools=["GITHUB_GET_USER"])
            result = await sdk.tools.execute("GITHUB_GET_USER", {}, user_id="user_123")
            return tools, result

        tools, result = asyncio.run(run())
        assert tools[0]["function"]["name"] == "GITHUB_GET_USER"
        assert result["successful"] is True
        assert result["data"] == {"login": "octocat"}
        execute = requests[-1]
        assert execute.url.path.endswith("/tools/execute/GITHUB_GET_USER")
        body = json.loads(execute.content)
        assert body["user_id"] == "user_123"
        assert body["version"] == "20250101_00"

    def test_provider_helper_executes_through_the_async_sdk(self) -> None:
        sdk, _ = _sdk()

        async def run() -> t.Any:
            return await sdk.provider.aexecute_tool(
                slug="GITHUB_GET_USER", arguments={}, user_id="user_123"
            )

        assert asyncio.run(run())["data"] == {"login": "octocat"}
        with pytest.raises(exceptions.ComposioError, match="aexecute_tool"):
            sdk.provider.execute_tool(slug="GITHUB_GET_USER", arguments={})

    def test_triggers_reach_the_async_webhook_subscriptions(self) -> None:
        sdk, requests = _sdk()

        async def run() -> None:
            await sdk.triggers.set_webhook_subscription(
                webhook_url="https://example.com/hook"
            )

        asyncio.run(run())
        assert isinstance(sdk.webhooks.subscriptions, AsyncWebhookSubscriptions)
        assert [request.method for request in requests] == ["GET", "POST"]
        assert json.loads(requests[1].content)["webhook_url"] == (
            "https://example.com/hook"
        )

    def test_concurrent_calls_share_one_client(self) -> None:
        sdk, requests = _sdk()

        async def run() -> t.List[t.Any]:
            return await asyncio.gather(
                *(sdk.create(user_id=f"user_{index}") for index in range(8))
            )

        sessions = asyncio.run(run())
        assert len(sessions) == len(requests) == 8


def test_async_sdk_methods_are_coroutines() -> None:
    sdk = AsyncComposio(api_key="ak")
    for method in (
        sdk.create,
        sdk.use,
        sdk.tools.get,
        sdk.tools.execute,
        sdk.tools.proxy,
        sdk.toolkits.get,
        sdk.toolkits.authorize,
        sdk.auth_configs.list,
        sdk.connected_accounts.initiate,
        sdk.connected_accounts.get,
        sdk.triggers.create,
        sdk.triggers.subscribe,
        sdk.mcp.create,
        sdk.logs.search,
        sdk.keyring.list_transfer_keys,
        sdk.aclose,
    ):
        assert inspect.iscoroutinefunction(method), method
