"""AsyncToolRouter / AsyncToolRouterSession against a mocked async transport."""

import asyncio
import json
import threading
import typing as t
import warnings

import httpx
import pytest
from composio_client import ProjectAuth
from pydantic import BaseModel, Field

from composio.client import AsyncHttpClient
from composio.client.types import Tool
from composio.core.models._async.connected_accounts import AsyncConnectionRequest
from composio.core.models._async.tool_router import (
    AsyncToolRouter,
    AsyncToolRouterSession,
    AsyncToolRouterSessionWithMcp,
)
from composio.core.models._async.tool_router_session_files import (
    AsyncToolRouterSessionFilesMount,
)
from composio.core.models.experimental import ExperimentalAPI
from composio.core.models.tool_router import ToolRouterMCPServerType
from composio.core.provider import AgenticProvider, get_execution_target
from composio.core.provider._openai import OpenAIProvider
from composio.core.provider.agentic import AgenticProviderExecuteFn
from composio.exceptions import (
    ComposioDeprecationWarning,
    InvalidParams,
    MCPDestinationError,
    SessionConfigConflictError,
    ToolInputRequiredError,
    ValidationError,
)
from composio.utils.async_bridge import SyncCallOnEventLoopError

BASE_URL = "https://backend.composio.dev"
SESSION_PATH = "/api/v3.1/tool_router/session"
MCP_URL = f"{BASE_URL}/api/v3/tool_router/session/session_123/mcp"

experimental_api = ExperimentalAPI()

Handler = t.Callable[[httpx.Request], t.Optional[httpx.Response]]


def _session_json(**overrides: t.Any) -> t.Dict[str, t.Any]:
    body: t.Dict[str, t.Any] = {
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
    body.update(overrides)
    return body


def _tool_json(slug: str) -> t.Dict[str, t.Any]:
    return {
        "slug": slug,
        "name": slug.title(),
        "description": f"{slug} description",
        "input_parameters": {"type": "object", "properties": {}},
        "output_parameters": {},
        "toolkit": {"slug": "composio", "name": "Composio", "logo": ""},
        "tags": [],
        "scopes": [],
        "version": "latest",
        "available_versions": [],
        "no_auth": True,
        "is_deprecated": False,
        "deprecated": {
            "available_versions": [],
            "displayName": slug,
            "is_deprecated": False,
            "toolkit": {"logo": ""},
            "version": "latest",
        },
    }


_COMPLETED = {"data": {"ok": True}, "error": None, "log_id": "log_1"}
_BLOCKED = {
    "result_type": "blocked",
    "reason": {"type": "request_blocked", "hook_id": "no_external_email"},
    "message": "Blocked by hook",
    "log_id": "log_blocked",
}
_INPUT_REQUIRED = {
    "result_type": "input_required",
    "input_requests": {
        "approval_1": {
            "type": "elicitation",
            "mode": "form",
            "message": "Allow?",
            "requested_schema": {"type": "object", "properties": {}},
        }
    },
    "request_state": "opaque-state-token",
}


class Backend:
    """Records requests and answers the session endpoints."""

    def __init__(self, override: t.Optional[Handler] = None) -> None:
        self.requests: t.List[httpx.Request] = []
        self.override = override
        self.session = _session_json()
        self.execute_response: t.Dict[str, t.Any] = {
            **_COMPLETED,
            "result_type": "completed",
        }
        self.tools = [_tool_json("COMPOSIO_SEARCH_TOOLS")]

    def bodies(self, suffix: str) -> t.List[t.Dict[str, t.Any]]:
        return [
            json.loads(request.content or b"{}")
            for request in self.requests
            if request.url.path.endswith(suffix)
        ]

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if self.override is not None:
            response = self.override(request)
            if response is not None:
                return response
        path = request.url.path
        if path.endswith(("/execute", "/execute_meta")):
            return httpx.Response(200, json=self.execute_response)
        if path.endswith("/tools"):
            return httpx.Response(
                200, json={"items": self.tools, "next_cursor": None, "total_pages": 1}
            )
        if path.endswith("/link"):
            return httpx.Response(
                200,
                json={
                    "connected_account_id": "ca_1",
                    "redirect_url": "https://connect.example/redirect",
                    "link_token": "lt_1",
                },
            )
        if path.endswith("/toolkits"):
            return httpx.Response(
                200,
                json={
                    "items": [
                        {
                            "slug": "github",
                            "name": "GitHub",
                            "is_no_auth": False,
                            "enabled": True,
                            "composio_managed_auth_schemes": [],
                            "meta": {"logo": "logo.png", "description": "d"},
                            "connected_account": {
                                "id": "ca_1",
                                "status": "ACTIVE",
                                "user_id": "user_123",
                                "created_at": "2026-01-01T00:00:00Z",
                                "auth_config": {
                                    "id": "ac_1",
                                    "auth_scheme": "OAUTH2",
                                    "is_composio_managed": True,
                                },
                            },
                        }
                    ],
                    "next_cursor": None,
                    "total_pages": 1,
                    "current_page": 1,
                    "total_items": 1,
                },
            )
        if path.endswith("/proxy_execute"):
            return httpx.Response(
                200,
                json={
                    "result_type": "completed",
                    "status": 200,
                    "data": {"id": 1},
                    "headers": {"content-type": "application/json"},
                },
            )
        if path.endswith("/config_history"):
            return httpx.Response(
                200, json={"items": [], "next_cursor": None, "total_pages": 1}
            )
        if request.method == "DELETE":
            return httpx.Response(
                200, json={"session_id": "session_123", "deleted": True}
            )
        if request.method == "PATCH":
            return httpx.Response(
                200,
                json=_session_json(
                    config={
                        "user_id": "user_123",
                        "execute": {},
                        "search": {},
                        "preload": {"tools": "all"},
                    },
                    config_version=4,
                ),
            )
        return httpx.Response(200, json=self.session)


def _client(backend: Backend, **kwargs: t.Any) -> AsyncHttpClient:
    return AsyncHttpClient(
        provider="openai",
        auth=ProjectAuth("ak_test"),
        base_url=BASE_URL,
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(backend)),
        **kwargs,
    )


def _router(
    backend: Backend, provider: t.Any = None, **client_kwargs: t.Any
) -> AsyncToolRouter[t.Any, t.Any]:
    return AsyncToolRouter(
        client=_client(backend, **client_kwargs),
        provider=provider if provider is not None else OpenAIProvider(),
    )


class CallableProvider(
    AgenticProvider[t.Callable[[t.Dict], t.Dict], t.List[t.Callable[[t.Dict], t.Dict]]],
    name="callable",
):
    """Agentic provider whose tools are plain blocking callables."""

    def wrap_tools(
        self, tools: t.Sequence[Tool], execute_tool: AgenticProviderExecuteFn
    ) -> t.List[t.Callable[[t.Dict], t.Dict]]:
        self.execute_tool_fn = execute_tool

        def bind(slug: str) -> t.Callable[[t.Dict], t.Dict]:
            return lambda arguments: execute_tool(slug, arguments)

        wrapped = [bind(tool.slug) for tool in tools]
        self.slugs = [tool.slug for tool in tools]
        return wrapped


class GrepInput(BaseModel):
    pattern: str = Field(description="Pattern to search for")


def _grep_tool(calls: t.Optional[t.List[t.Any]] = None) -> t.Any:
    @experimental_api.tool
    def grep(input: GrepInput, ctx: t.Any) -> t.Dict[str, t.Any]:
        """Search for a pattern."""
        if calls is not None:
            calls.append(threading.current_thread())
        return {"matches": [input.pattern], "user": ctx.user_id}

    return grep


def _custom_experimental(slug: str = "GREP") -> t.Dict[str, t.Any]:
    return {
        "custom_tools": [
            {"slug": f"LOCAL_{slug}", "original_slug": slug},
        ]
    }


class TestCreate:
    def test_create_sends_params_and_returns_async_session(self):
        backend = Backend()

        async def run() -> AsyncToolRouterSession[t.Any, t.Any]:
            return await _router(backend).create(
                user_id="user_123",
                toolkits=["github"],
                tools={"gmail": ["GMAIL_SEND_EMAIL"]},
                tags=["readOnlyHint"],
                manage_connections={"enable": True, "callback_url": "https://cb"},
                connected_accounts={"github": "ca_1"},
            )

        session = asyncio.run(run())
        assert type(session) is AsyncToolRouterSession
        assert session.session_id == "session_123"
        assert session.config_version == 3
        assert isinstance(session.experimental.files, AsyncToolRouterSessionFilesMount)
        request = backend.requests[0]
        assert request.method == "POST" and request.url.path == SESSION_PATH
        assert json.loads(request.content) == {
            "user_id": "user_123",
            "manage_connections": {"enable": True, "callback_url": "https://cb"},
            "connected_accounts": {"github": ["ca_1"]},
            "toolkits": {"enable": ["github"]},
            "tools": {"gmail": {"enable": ["GMAIL_SEND_EMAIL"]}},
            "tags": {"enable": ["readOnlyHint"]},
        }

    def test_direct_tools_preset_matches_sync_defaults(self):
        backend = Backend()
        asyncio.run(
            _router(backend).create(user_id="user_123", session_preset="direct_tools")
        )
        body = json.loads(backend.requests[0].content)
        assert body["manage_connections"] == {"enable": False}
        assert body["workbench"] == {"enable": False}
        assert body["preload"] == {"tools": "all"}
        assert body["search"] == {"enable": False}
        assert body["execute"] == {"enable_multi_execute": False}

    def test_invalid_params_raise_before_any_request(self):
        backend = Backend()
        with pytest.raises(InvalidParams):
            asyncio.run(
                _router(backend).create(
                    user_id="user_123",
                    sandbox={"enable": True},
                    workbench={"enable": True},
                )
            )
        assert backend.requests == []

    def test_mcp_true_returns_typed_session_with_credential_headers(self):
        backend = Backend()
        session = asyncio.run(_router(backend).create(user_id="user_123", mcp=True))
        assert isinstance(session, AsyncToolRouterSessionWithMcp)
        assert session.mcp.type is ToolRouterMCPServerType.HTTP
        assert session.mcp.url == MCP_URL
        assert session.mcp.headers == {"x-api-key": "ak_test"}

    def test_mcp_true_rejects_a_foreign_mcp_origin(self):
        backend = Backend()
        backend.session = _session_json(
            mcp={"type": "http", "url": "https://evil.example/mcp"}
        )
        with pytest.raises(MCPDestinationError):
            asyncio.run(_router(backend).create(user_id="user_123", mcp=True))

    def test_foreign_mcp_origin_without_mcp_leaves_headers_empty(self):
        backend = Backend()
        backend.session = _session_json(
            mcp={"type": "http", "url": "https://evil.example/mcp"}
        )
        session = asyncio.run(_router(backend).create(user_id="user_123"))
        assert getattr(session, "mcp").headers == {}

    def test_create_without_provider_raises(self):
        backend = Backend()
        router: AsyncToolRouter[t.Any, t.Any] = AsyncToolRouter(client=_client(backend))
        with pytest.raises(ValueError, match="Provider is required"):
            asyncio.run(router.create(user_id="user_123"))
        assert backend.requests == []

    def test_create_with_local_tools_serializes_and_maps_them(self):
        backend = Backend()
        backend.session = _session_json(experimental=_custom_experimental())
        session = asyncio.run(
            _router(backend).create(
                user_id="user_123", experimental={"local_tools": [_grep_tool()]}
            )
        )
        sent = json.loads(backend.requests[0].content)["experimental"]["custom_tools"]
        assert sent[0]["slug"] == "GREP"
        assert [tool.slug for tool in session.local_tools()] == ["LOCAL_GREP"]

    def test_deprecated_custom_tools_alias_warns_at_the_caller(self):
        backend = Backend()
        backend.session = _session_json(experimental=_custom_experimental())

        async def run() -> None:
            await _router(backend).use("session_123", custom_tools=[_grep_tool()])

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            asyncio.run(run())
        deprecations = [
            w for w in caught if issubclass(w.category, ComposioDeprecationWarning)
        ]
        assert len(deprecations) == 1
        assert deprecations[0].filename == __file__


class TestUse:
    def test_use_retrieves_without_local_tools(self):
        backend = Backend()
        session = asyncio.run(_router(backend).use("session_123"))
        assert type(session) is AsyncToolRouterSession
        request = backend.requests[0]
        assert request.method == "GET"
        assert request.url.path == f"{SESSION_PATH}/session_123"

    def test_use_attaches_local_tools(self):
        backend = Backend()
        backend.session = _session_json(experimental=_custom_experimental())
        session = asyncio.run(
            _router(backend).use("session_123", local_tools=[_grep_tool()], mcp=True)
        )
        assert isinstance(session, AsyncToolRouterSessionWithMcp)
        request = backend.requests[0]
        assert request.url.path == f"{SESSION_PATH}/session_123/attach"
        assert json.loads(request.content)["experimental"]["custom_tools"][0][
            "slug"
        ] == ("GREP")
        assert [tool.slug for tool in session.local_tools()] == ["LOCAL_GREP"]

    def test_router_delete(self):
        backend = Backend()
        result = asyncio.run(_router(backend).delete("session_123"))
        assert result == {"session_id": "session_123", "deleted": True}
        assert backend.requests[0].method == "DELETE"

    def test_router_delete_rejects_a_malformed_response(self):
        backend = Backend(
            lambda request: httpx.Response(200, json={"session_id": "session_123"})
        )
        with pytest.raises(ValidationError):
            asyncio.run(_router(backend).delete("session_123"))


class TestSessionTools:
    def test_non_agentic_provider_gets_wrapped_tools(self):
        backend = Backend()

        async def run() -> t.Any:
            session = await _router(backend).create(user_id="user_123")
            return await session.tools()

        tools = asyncio.run(run())
        assert [tool["function"]["name"] for tool in tools] == ["COMPOSIO_SEARCH_TOOLS"]

    def test_preloaded_local_tools_are_appended(self):
        backend = Backend()
        backend.session = _session_json(
            experimental=_custom_experimental(),
            config={
                "user_id": "user_123",
                "execute": {},
                "search": {},
                "preload": {"tools": "all"},
            },
        )

        async def run() -> t.Any:
            session = await _router(backend).create(
                user_id="user_123",
                preload={"tools": "all"},
                experimental={"local_tools": [_grep_tool()]},
            )
            return await session.tools()

        tools = asyncio.run(run())
        assert [tool["function"]["name"] for tool in tools] == [
            "COMPOSIO_SEARCH_TOOLS",
            "LOCAL_GREP",
        ]

    def test_agentic_tool_executes_from_a_worker_thread_without_retries(self):
        attempts: t.List[httpx.Request] = []

        def failing_execute(request: httpx.Request) -> t.Optional[httpx.Response]:
            if request.url.path.endswith("/execute"):
                attempts.append(request)
                return httpx.Response(500, json={"error": {"message": "boom"}})
            return None

        backend = Backend()
        provider = CallableProvider()

        async def run() -> t.Tuple[t.Any, t.Any]:
            session = await _router(backend, provider).create(user_id="user_123")
            (tool,) = await session.tools()
            ok = await asyncio.to_thread(tool, {"queries": []})
            backend.override = failing_execute
            failed: t.Any = None
            try:
                await asyncio.to_thread(tool, {"queries": []})
            except Exception as error:  # noqa: BLE001
                failed = error
            assert get_execution_target(provider.execute_tool_fn) is session
            return ok, failed

        ok, failed = asyncio.run(run())
        assert ok["successful"] is True and ok["data"] == {"ok": True}
        assert ok["log_id"] == "log_1"
        body = backend.bodies("/execute")[0]
        assert body["tool_slug"] == "COMPOSIO_SEARCH_TOOLS"
        assert body["enable_auto_workbench_offload"] is True
        assert getattr(failed, "status_code", None) == 500
        assert len(attempts) == 1

    def test_agentic_tool_called_on_the_loop_raises_instead_of_deadlocking(self):
        backend = Backend()

        async def run() -> None:
            session = await _router(backend, CallableProvider()).create(
                user_id="user_123"
            )
            (tool,) = await session.tools()
            tool({"queries": []})

        with pytest.raises(SyncCallOnEventLoopError):
            asyncio.run(run())
        assert backend.bodies("/execute") == []


class TestSessionExecute:
    def _session(self, backend: Backend) -> t.Coroutine[t.Any, t.Any, t.Any]:
        return _router(backend).create(user_id="user_123")

    def test_execute_returns_the_normalized_result(self):
        backend = Backend()

        async def run() -> t.Any:
            session = await self._session(backend)
            return await session.execute(
                "GMAIL_SEND_EMAIL", arguments={"to": "a@b.c"}, account="work"
            )

        result = asyncio.run(run())
        assert result.result_type == "completed"
        assert result.data == {"ok": True}
        assert result.log_id == "log_1"
        assert backend.bodies("/execute") == [
            {
                "tool_slug": "GMAIL_SEND_EMAIL",
                "arguments": {"to": "a@b.c"},
                "account": "work",
            }
        ]

    @pytest.mark.parametrize("method", ["execute", "execute_meta"])
    def test_blocked_call_is_returned_as_a_blocked_result(self, method):
        backend = Backend()
        backend.execute_response = _BLOCKED

        async def run() -> t.Any:
            session = await self._session(backend)
            return await getattr(session, method)("COMPOSIO_SEARCH_TOOLS")

        result = asyncio.run(run())
        assert result.result_type == "blocked"
        assert result.error == "Blocked by hook"
        assert result.data == {}
        assert result.block_reason == {
            "type": "request_blocked",
            "hook_id": "no_external_email",
        }

    @pytest.mark.parametrize("method", ["execute", "execute_meta"])
    def test_input_required_raises(self, method):
        backend = Backend()
        backend.execute_response = _INPUT_REQUIRED

        async def run() -> t.Any:
            session = await self._session(backend)
            return await getattr(session, method)("COMPOSIO_SEARCH_TOOLS")

        with pytest.raises(ToolInputRequiredError) as raised:
            asyncio.run(run())
        assert raised.value.request_state == "opaque-state-token"
        assert "approval_1" in raised.value.input_requests

    @pytest.mark.parametrize("method", ["execute", "execute_meta"])
    @pytest.mark.parametrize(
        "response", [_COMPLETED, {**_COMPLETED, "result_type": "mystery"}]
    )
    def test_unknown_result_type_raises(self, method, response):
        backend = Backend()
        backend.execute_response = response

        async def run() -> t.Any:
            session = await self._session(backend)
            return await getattr(session, method)("COMPOSIO_SEARCH_TOOLS")

        with pytest.raises(ValidationError, match="result_type"):
            asyncio.run(run())

    @pytest.mark.parametrize(
        "method,suffix", [("execute", "/execute"), ("execute_meta", "/execute_meta")]
    )
    def test_execute_is_never_retried(self, method, suffix):
        attempts: t.List[httpx.Request] = []

        def flaky(request: httpx.Request) -> t.Optional[httpx.Response]:
            if request.url.path.endswith(suffix):
                attempts.append(request)
                return httpx.Response(503, json={"error": {"message": "down"}})
            return None

        backend = Backend(flaky)

        async def run() -> t.Any:
            session = await self._session(backend)
            return await getattr(session, method)("COMPOSIO_SEARCH_TOOLS")

        with pytest.raises(Exception) as raised:
            asyncio.run(run())
        assert getattr(raised.value, "status_code", None) == 503
        assert len(attempts) == 1

    def test_input_responses_travel_on_the_repeated_call(self):
        backend = Backend()

        async def run() -> t.Any:
            session = await self._session(backend)
            return await session.execute(
                "GMAIL_SEND_EMAIL",
                arguments={},
                input_responses={"approval_1": {"action": "accept"}},
                request_state="opaque-state-token",
            )

        asyncio.run(run())
        body = backend.bodies("/execute")[0]
        assert body["input_responses"] == {"approval_1": {"action": "accept"}}
        assert body["request_state"] == "opaque-state-token"

    def test_proxy_execute_projects_the_response(self):
        backend = Backend()

        async def run() -> t.Any:
            session = await self._session(backend)
            return await session.proxy_execute(
                toolkit="github",
                endpoint="https://api.github.com/user",
                method="GET",
                parameters=[{"name": "q", "value": 1, "in": "query"}],
            )

        result = asyncio.run(run())
        assert result == {
            "result_type": "completed",
            "status": 200,
            "data": {"id": 1},
            "headers": {"content-type": "application/json"},
        }
        assert backend.bodies("/proxy_execute")[0]["parameters"] == [
            {"name": "q", "type": "query", "value": "1"}
        ]

    def test_proxy_execute_validates_before_any_request(self):
        backend = Backend()

        async def run() -> t.Any:
            session = await self._session(backend)
            return await session.proxy_execute(
                toolkit="", endpoint="https://x", method="GET"
            )

        with pytest.raises(ValidationError, match="toolkit is required"):
            asyncio.run(run())
        assert backend.bodies("/proxy_execute") == []


class TestLocalTools:
    def _session(
        self, backend: Backend, tools: t.List[t.Any], provider: t.Any = None
    ) -> t.Coroutine[t.Any, t.Any, t.Any]:
        backend.session = _session_json(
            experimental={
                "custom_tools": [
                    {
                        "slug": f"LOCAL_{tool.slug}",
                        "original_slug": tool.slug,
                    }
                    for tool in tools
                ]
            }
        )
        return _router(backend, provider).create(
            user_id="user_123", experimental={"local_tools": tools}
        )

    def test_local_tool_runs_off_the_loop_and_is_not_sent_to_the_backend(self):
        backend = Backend()
        threads: t.List[t.Any] = []

        async def run() -> t.Any:
            session = await self._session(backend, [_grep_tool(threads)])
            result = await session.execute("GREP", arguments={"pattern": "todo"})
            return result, threading.current_thread()

        result, loop_thread = asyncio.run(run())
        assert result.result_type == "completed"
        assert result.data == {"matches": ["todo"], "user": "user_123"}
        assert result.log_id == ""
        assert threads and threads[0] is not loop_thread
        assert backend.bodies("/execute") == []

    def test_local_tool_validation_failure_is_a_failed_result(self):
        backend = Backend()

        async def run() -> t.Any:
            session = await self._session(backend, [_grep_tool()])
            return await session.execute("LOCAL_GREP", arguments={})

        result = asyncio.run(run())
        assert result.result_type == "failed"
        assert "Input validation failed" in result.error

    def test_ctx_execute_and_proxy_bridge_back_to_the_loop(self):
        backend = Backend()

        @experimental_api.tool
        def chain(input: GrepInput, ctx: t.Any) -> t.Dict[str, t.Any]:
            """Call a remote tool, a sibling, and the proxy."""
            remote = ctx.execute("GMAIL_SEND_EMAIL", {"to": input.pattern})
            sibling = ctx.execute("GREP", {"pattern": "inner"})
            proxied = ctx.proxy_execute(
                toolkit="github", endpoint="https://api.github.com/user", method="GET"
            )
            return {
                "remote": remote.data,
                "sibling": sibling.data,
                "status": proxied["status"],
            }

        async def run() -> t.Any:
            session = await self._session(backend, [chain, _grep_tool()])
            return await session.execute("CHAIN", arguments={"pattern": "a@b.c"})

        result = asyncio.run(run())
        assert result.result_type == "completed", result.error
        assert result.data == {
            "remote": {"ok": True},
            "sibling": {"matches": ["inner"], "user": "user_123"},
            "status": 200,
        }
        (body,) = backend.bodies("/execute")
        assert body["tool_slug"] == "GMAIL_SEND_EMAIL"
        assert body["arguments"] == {"to": "a@b.c"}
        assert "custom_tools" in body["experimental"]
        assert len(backend.bodies("/proxy_execute")) == 1

    def test_ctx_input_required_propagates(self):
        backend = Backend()
        backend.execute_response = _INPUT_REQUIRED

        @experimental_api.tool
        def needs_input(input: GrepInput, ctx: t.Any) -> t.Dict[str, t.Any]:
            """Call a remote tool that asks for approval."""
            return ctx.execute("GMAIL_SEND_EMAIL", {}).data

        async def run() -> t.Any:
            session = await self._session(backend, [needs_input])
            return await session.execute("NEEDS_INPUT", arguments={"pattern": "x"})

        with pytest.raises(ToolInputRequiredError):
            asyncio.run(run())

    def test_multi_execute_routes_local_and_remote_and_merges_in_order(self):
        backend = Backend()
        backend.tools = [_tool_json("COMPOSIO_MULTI_EXECUTE_TOOL")]
        backend.execute_response = {
            "data": {
                "results": [
                    {
                        "response": {"successful": True, "data": {"sent": True}},
                        "tool_slug": "GMAIL_SEND_EMAIL",
                        "index": 0,
                    }
                ],
                "total_count": 1,
                "success_count": 1,
                "error_count": 0,
            },
            "error": None,
            "log_id": "log_multi",
            "result_type": "completed",
        }
        provider = CallableProvider()

        async def run() -> t.Any:
            session = await self._session(backend, [_grep_tool()], provider)
            tools = await session.tools()
            multi = tools[provider.slugs.index("COMPOSIO_MULTI_EXECUTE_TOOL")]
            return await asyncio.to_thread(
                multi,
                {
                    "tools": [
                        {"tool_slug": "LOCAL_GREP", "arguments": {"pattern": "a"}},
                        {"tool_slug": "GMAIL_SEND_EMAIL", "arguments": {"to": "x"}},
                    ]
                },
            )

        result = asyncio.run(run())
        assert result["successful"] is True
        assert [
            (entry["index"], entry["tool_slug"]) for entry in result["data"]["results"]
        ] == [(0, "LOCAL_GREP"), (1, "GMAIL_SEND_EMAIL")]
        assert result["data"]["total_count"] == 2
        (body,) = backend.bodies("/execute")
        assert body["tool_slug"] == "COMPOSIO_MULTI_EXECUTE_TOOL"
        assert body["arguments"]["tools"] == [
            {"tool_slug": "GMAIL_SEND_EMAIL", "arguments": {"to": "x"}}
        ]

    def test_multi_execute_remote_failure_keeps_local_results(self):
        def failing(request: httpx.Request) -> t.Optional[httpx.Response]:
            if request.url.path.endswith("/execute"):
                return httpx.Response(500, json={"error": {"message": "boom"}})
            return None

        backend = Backend(failing)
        backend.tools = [_tool_json("COMPOSIO_MULTI_EXECUTE_TOOL")]
        provider = CallableProvider()

        async def run() -> t.Any:
            session = await self._session(backend, [_grep_tool()], provider)
            (multi,) = [
                tool
                for tool, slug in zip(await session.tools(), provider.slugs)
                if slug == "COMPOSIO_MULTI_EXECUTE_TOOL"
            ]
            return await asyncio.to_thread(
                multi,
                {
                    "tools": [
                        {"tool_slug": "GMAIL_SEND_EMAIL", "arguments": {}},
                        {"tool_slug": "LOCAL_GREP", "arguments": {"pattern": "a"}},
                    ]
                },
            )

        result = asyncio.run(run())
        assert result["successful"] is False
        remote, local = result["data"]["results"]
        assert remote["tool_slug"] == "GMAIL_SEND_EMAIL" and remote["error"]
        assert local["response"] == {
            "successful": True,
            "data": {"matches": ["a"], "user": "user_123"},
        }
        assert result["error"] == "1 out of 2 tools failed"

    def test_single_local_tool_through_the_agentic_callable(self):
        backend = Backend()
        backend.tools = []
        backend.session = _session_json()
        provider = CallableProvider()

        async def run() -> t.Any:
            backend.session = _session_json(
                experimental=_custom_experimental(),
                config={
                    "user_id": "user_123",
                    "execute": {},
                    "search": {},
                    "preload": {"tools": "all"},
                },
            )
            session = await _router(backend, provider).create(
                user_id="user_123",
                preload={"tools": "all"},
                experimental={"local_tools": [_grep_tool()]},
            )
            (tool,) = await session.tools()
            return await asyncio.to_thread(tool, {"pattern": "z"})

        result = asyncio.run(run())
        assert result == {
            "data": {"matches": ["z"], "user": "user_123"},
            "error": None,
            "successful": True,
        }
        assert backend.bodies("/execute") == []


class TestSessionOperations:
    def _run(self, backend: Backend, call: t.Callable[[t.Any], t.Any]) -> t.Any:
        async def run() -> t.Any:
            session = await _router(backend).create(user_id="user_123")
            return session, await call(session)

        return asyncio.run(run())

    def test_authorize_returns_an_async_connection_request(self):
        backend = Backend()
        _, request = self._run(
            backend, lambda s: s.authorize("github", callback_url="https://cb")
        )
        assert isinstance(request, AsyncConnectionRequest)
        assert request.id == "ca_1"
        assert request.redirect_url == "https://connect.example/redirect"
        assert request.status == "INITIATED"
        assert backend.bodies("/link") == [
            {"toolkit": "github", "callback_url": "https://cb"}
        ]

    def test_toolkits_maps_connection_states(self):
        backend = Backend()
        _, details = self._run(backend, lambda s: s.toolkits(limit=5, search="git"))
        (state,) = details.items
        assert state.slug == "github"
        assert state.connection.is_active is True
        assert state.connection.auth_config.id == "ac_1"
        assert details.total_pages == 1
        request = next(r for r in backend.requests if r.url.path.endswith("/toolkits"))
        assert request.url.params["limit"] == "5"
        assert request.url.params["search"] == "git"

    def test_search_sends_the_use_case(self):
        backend = Backend(
            lambda request: (
                httpx.Response(
                    200,
                    json={
                        "success": True,
                        "error": None,
                        "results": [],
                        "tool_schemas": {},
                        "toolkit_connection_statuses": [],
                        "session": {"id": "session_123", "generate_id": False},
                        "next_steps_guidance": [],
                        "time_info": {},
                    },
                )
                if request.url.path.endswith("/search")
                else None
            )
        )
        self._run(backend, lambda s: s.search(query="send an email"))
        assert backend.bodies("/search") == [
            {"queries": [{"use_case": "send an email"}]}
        ]

    def test_update_refreshes_config_in_place(self):
        backend = Backend()
        session, config = self._run(
            backend,
            lambda s: s.update(
                preload={"tools": "all"}, proxy_execute=True, expected_config_version=3
            ),
        )
        assert config is session.config
        assert session.config_version == 4
        assert session.preload.tools == "all"
        patch = next(r for r in backend.requests if r.method == "PATCH")
        assert json.loads(patch.content) == {
            "preload": {"tools": "all"},
            "proxy_execute": {"enable": True},
            "expected_config_version": 3,
        }

    def test_update_conflict_raises_once_and_leaves_the_session_unchanged(self):
        attempts: t.List[httpx.Request] = []

        def conflict(request: httpx.Request) -> t.Optional[httpx.Response]:
            if request.method == "PATCH":
                attempts.append(request)
                return httpx.Response(409, json={"error": {"message": "stale"}})
            return None

        backend = Backend(conflict)

        async def run() -> t.Any:
            session = await _router(backend).create(user_id="user_123")
            try:
                await session.update(toolkits=None, expected_config_version=2)
            except SessionConfigConflictError as error:
                return session, error
            return session, None

        session, error = asyncio.run(run())
        assert error is not None
        assert error.expected_config_version == 2
        assert session.config_version == 3
        assert len(attempts) == 1

    def test_update_validates_before_any_request(self):
        backend = Backend()

        async def run() -> None:
            session = await _router(backend).create(user_id="user_123")
            await session.update(instant=None)  # type: ignore[arg-type]

        with pytest.raises(InvalidParams):
            asyncio.run(run())
        assert not any(r.method == "PATCH" for r in backend.requests)

    def test_list_config_history_and_delete(self):
        backend = Backend()
        _, history = self._run(backend, lambda s: s.list_config_history(limit=10))
        assert history.items == []
        _, deleted = self._run(backend, lambda s: s.delete())
        assert deleted == {"session_id": "session_123", "deleted": True}
