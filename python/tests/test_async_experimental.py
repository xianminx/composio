"""AsyncExperimentalAPI, AsyncCustomToolkits and AsyncExperimentalUsage."""

import asyncio
import json
import typing as t
import warnings

import httpx
import pytest
from composio_client import ProjectAuth
from pydantic import BaseModel

from composio.client import AsyncHttpClient
from composio.core.models import experimental as sync_experimental
from composio.core.models._async.experimental import (
    AsyncCustomToolkits,
    AsyncExperimentalAPI,
    AsyncExperimentalUsage,
)
from composio.core.models.custom_tool import ExperimentalToolkit
from composio.core.models.custom_tool_types import CustomTool
from composio.core.models.experimental import UsageSummaryResponse
from composio.exceptions import (
    ComposioAclOnlyForSharedError,
    ComposioDeprecationWarning,
    ValidationError,
)


class Backend:
    def __init__(self, respond: t.Callable[[httpx.Request], httpx.Response]) -> None:
        self.requests: t.List[httpx.Request] = []
        self.respond = respond

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        return self.respond(request)

    @property
    def last(self) -> httpx.Request:
        return self.requests[-1]


def _api(respond: t.Callable[[httpx.Request], httpx.Response]):
    backend = Backend(respond)
    client = AsyncHttpClient(
        provider="openai",
        auth=ProjectAuth("ak_test"),
        base_url="https://backend.composio.dev",
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(backend)),
    )
    return AsyncExperimentalAPI(client=client), backend


def _json(body: t.Dict[str, t.Any]) -> t.Callable[[httpx.Request], httpx.Response]:
    return lambda request: httpx.Response(200, json=body)


class EchoInput(BaseModel):
    text: str


class TestDecorators:
    def test_tool_and_toolkit_builders_match_the_sync_namespace(self):
        api = AsyncExperimentalAPI()

        @api.tool
        def echo(input: EchoInput, ctx: t.Any) -> t.Dict[str, str]:
            """Echo the text."""
            return {"text": input.text}

        @api.tool(slug="SHOUT", extends_toolkit="gmail")
        def shout(input: EchoInput, ctx: t.Any) -> t.Dict[str, str]:
            """Shout the text."""
            return {"text": input.text.upper()}

        assert isinstance(echo, CustomTool) and echo.slug == "ECHO"
        assert shout.slug == "SHOUT" and shout.extends_toolkit == "gmail"
        assert AsyncExperimentalAPI.Toolkit is ExperimentalToolkit

    def test_async_tool_functions_are_still_rejected(self):
        api = AsyncExperimentalAPI()
        with pytest.raises(Exception, match="(?i)async|coroutine"):

            @api.tool
            async def echo(input: EchoInput, ctx: t.Any) -> t.Dict[str, str]:
                """Echo the text."""
                return {"text": input.text}


class TestUsage:
    def test_summary_keeps_the_instant_charge(self):
        api, backend = _api(
            _json(
                {
                    "entities": {},
                    "from": 1,
                    "to": 2,
                    "instant_charge": "0.25",
                }
            )
        )
        summary = asyncio.run(api.usage.summary(entity_types=["tool_calls"]))
        assert isinstance(summary, UsageSummaryResponse)
        assert summary.instant_charge == "0.25"
        assert backend.last.url.path.endswith("/usage/summary")

    def test_breakdown_requests_the_entity_type(self):
        api, backend = _api(
            _json({"entity_type": "tool_calls", "from": 1, "to": 2, "groups": []})
        )
        asyncio.run(api.usage.breakdown("tool_calls", group_by="tool_slug"))
        assert backend.last.url.path.endswith("/usage/tool_calls")

    def test_usage_without_a_client_raises(self):
        with pytest.raises(ValidationError, match="requires a Composio client"):
            asyncio.run(AsyncExperimentalUsage().summary())


class TestCustomToolkits:
    def test_create_update_sync_delete(self):
        api, backend = _api(
            _json(
                {
                    "slug": "CUSTOM_X",
                    "connect_link": None,
                    "success": True,
                    "version": "1",
                    "synced_count": 2,
                }
            )
        )

        async def run() -> t.Any:
            toolkits = api.custom_toolkits
            created = await toolkits.create(
                name="X",
                mcp_url="https://mcp.example.com/mcp",
                auth_schemes=[{"mode": "NO_AUTH"}],
                user_id="all",
            )
            await toolkits.update("CUSTOM_X", name="Y")
            synced = await toolkits.sync("CUSTOM_X", connected_account_id="ca_1")
            await toolkits.delete("CUSTOM_X")
            return created, synced

        created, synced = asyncio.run(run())
        assert created.slug == "CUSTOM_X"
        assert synced.synced_count == 2
        create, update, sync, delete = backend.requests
        assert (create.method, create.url.path) == (
            "POST",
            "/api/v3.1/custom/toolkits",
        )
        assert json.loads(create.content)["mcp_url"] == "https://mcp.example.com/mcp"
        assert update.url.path == "/api/v3.1/custom/toolkits/CUSTOM_X"
        assert json.loads(update.content) == {"name": "Y"}
        assert sync.url.path == "/api/v3.1/custom/toolkits/sync"
        assert json.loads(sync.content) == {
            "slug": "CUSTOM_X",
            "connected_account_id": "ca_1",
        }
        assert (delete.method, delete.url.path) == (
            "DELETE",
            "/api/v3.1/custom/toolkits/CUSTOM_X",
        )

    def test_upsert_warns_once_per_process(self, monkeypatch):
        monkeypatch.setattr(sync_experimental, "_upsert_warning_emitted", False)
        api, backend = _api(_json({"slug": "CUSTOM_X"}))

        async def run() -> None:
            for _ in range(2):
                await api.custom_toolkits.upsert(  # type: ignore[deprecated]
                    slug="X", toolkit_config={"name": "X"}
                )

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            asyncio.run(run())
        deprecations = [
            w for w in caught if issubclass(w.category, ComposioDeprecationWarning)
        ]
        assert len(deprecations) == 1
        assert len(backend.requests) == 2

    def test_custom_toolkits_without_a_client_raises(self):
        with pytest.raises(ValidationError, match="requires a Composio client"):
            asyncio.run(AsyncCustomToolkits().delete("CUSTOM_X"))


class TestUpdateAcl:
    def test_update_acl_delegates_to_connected_accounts(self):
        api, backend = _api(_json({"id": "ca_1", "status": "ACTIVE", "success": True}))
        response = asyncio.run(
            api.update_acl("ca_1", allow_all_users=True, not_allowed_user_ids=["bob"])
        )
        assert response.success is True
        assert backend.last.method == "PATCH"
        assert backend.last.url.path.endswith("/connected_accounts/ca_1")
        body = json.loads(backend.last.content)
        assert "bob" in json.dumps(body)

    def test_update_acl_on_a_private_connection_is_a_typed_error(self):
        api, _ = _api(
            lambda request: httpx.Response(
                400,
                json={
                    "error": {
                        "message": "acl_config_for_shared is only valid on SHARED"
                    }
                },
            )
        )
        with pytest.raises(ComposioAclOnlyForSharedError):
            asyncio.run(api.update_acl("ca_1", allow_all_users=True))

    def test_update_acl_without_a_client_raises(self):
        with pytest.raises(ValidationError, match="requires a Composio client"):
            asyncio.run(AsyncExperimentalAPI().update_acl("ca_1", allow_all_users=True))
