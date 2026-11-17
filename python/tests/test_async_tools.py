"""Tests for the async tools resource, its file helper and provider hooks."""

import asyncio
import json
import typing as t
from pathlib import Path
from unittest.mock import patch

import httpx
import pytest
from composio_client import ProjectAuth
from openai.types.chat.chat_completion import ChatCompletion
from openai.types.responses.response import Response
from openai.types.responses.response_output_item import ResponseFunctionToolCall

from composio import after_execute, before_execute, schema_modifier
from composio.client import AsyncHttpClient
from composio.client.types import Tool, tool_list_response
from composio.core.models._async.tools import AsyncTools
from composio.core.models.base import allow_tracking
from composio.core.models.tools import ToolRouterSessionExecuteResponse
from composio.core.provider import AgenticProvider
from composio.core.provider._openai import OpenAIProvider
from composio.core.provider._openai_responses import OpenAIResponsesProvider
from composio.core.provider.agentic import get_execution_target
from composio.exceptions import (
    ComposioError,
    ToolNotFoundError,
    ToolVersionRequiredError,
)
from composio.utils.async_bridge import SyncCallOnEventLoopError

Handler = t.Callable[[httpx.Request], httpx.Response]


@pytest.fixture(autouse=True)
def disable_telemetry():
    token = allow_tracking.set(False)
    yield
    allow_tracking.reset(token)


def make_tool(
    slug: str = "GITHUB_STAR",
    toolkit: str = "github",
    input_parameters: t.Optional[dict] = None,
    output_parameters: t.Optional[dict] = None,
) -> Tool:
    return Tool(
        name=slug,
        slug=slug,
        description="Test tool",
        input_parameters=input_parameters or {"type": "object", "properties": {}},
        output_parameters=output_parameters or {"type": "object", "properties": {}},
        available_versions=["v1.0.0"],
        version="v1.0.0",
        scopes=[],
        toolkit=tool_list_response.ItemToolkit(name=toolkit, slug=toolkit, logo=""),
        deprecated=tool_list_response.ItemDeprecated(
            available_versions=["v1.0.0"],
            displayName=slug,
            version="v1.0.0",
            toolkit=tool_list_response.ItemDeprecatedToolkit(logo=""),
            is_deprecated=False,
        ),
        is_deprecated=False,
        no_auth=False,
        tags=[],
    )


EXECUTED = {"data": {"ok": True}, "error": None, "successful": True}


class Backend:
    """A mock Composio API that serves ``tools`` and records every request."""

    def __init__(self, *tools: Tool, execute: t.Optional[Handler] = None) -> None:
        self.tools = {tool.slug: tool for tool in tools}
        self.requests: t.List[httpx.Request] = []
        self._execute = execute

    def bodies(self, suffix: str) -> t.List[dict]:
        return [
            json.loads(request.content)
            for request in self.requests
            if request.url.path.endswith(suffix)
        ]

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        path = request.url.path
        if request.method == "GET" and path.endswith("/tools"):
            return httpx.Response(
                200,
                json={
                    "items": [
                        tool.model_dump(mode="json") for tool in self.tools.values()
                    ],
                    "total_pages": 1,
                    "current_page": 1,
                    "total_items": len(self.tools),
                    "next_cursor": None,
                },
            )
        if request.method == "GET":
            tool = self.tools.get(path.rsplit("/", 1)[-1])
            if tool is None:
                return httpx.Response(404, json={"error": {"message": "not found"}})
            return httpx.Response(200, json=tool.model_dump(mode="json"))
        if self._execute is not None:
            return self._execute(request)
        return httpx.Response(200, json=EXECUTED)


def run_with_tools(
    backend: Handler,
    body: t.Callable[[AsyncTools[t.Any, t.Any]], t.Awaitable[t.Any]],
    provider: t.Any = None,
    max_retries: int = 2,
    **options: t.Any,
) -> t.Any:
    async def main() -> t.Any:
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(backend)
        ) as http_client:
            client = AsyncHttpClient(
                provider="openai",
                auth=ProjectAuth("ak"),
                base_url="https://composio.invalid",
                http_client=http_client,
                max_retries=max_retries,
            )
            tools: AsyncTools[t.Any, t.Any] = AsyncTools(
                client=client,
                provider=provider if provider is not None else OpenAIProvider(),
                **options,
            )
            return await body(tools)

    return asyncio.run(main())


class FakeAgenticProvider(AgenticProvider[t.Callable, t.List[t.Callable]], name="fake"):
    def wrap_tool(self, tool, execute_tool):
        def call(**arguments):
            return execute_tool(tool.slug, arguments)

        call.execute_tool = execute_tool  # type: ignore[attr-defined]
        return call

    def wrap_tools(self, tools, execute_tool):
        return [self.wrap_tool(tool, execute_tool) for tool in tools]


class TestGet:
    def test_non_agentic_provider_wraps_schemas(self):
        backend = Backend(make_tool())

        @schema_modifier
        def rename(tool, toolkit, schema):
            schema.description = f"{toolkit}:{schema.description}"
            return schema

        wrapped = run_with_tools(
            backend,
            lambda tools: tools.get("user_1", toolkits=["github"], modifiers=[rename]),
        )

        assert [tool["function"]["name"] for tool in wrapped] == ["GITHUB_STAR"]
        assert wrapped[0]["function"]["description"] == "github:Test tool"
        assert backend.requests[0].url.params["toolkit_slug"] == "github"

    def test_filters_are_required(self):
        with pytest.raises(ComposioError, match="must be provided"):
            run_with_tools(Backend(), lambda tools: tools.get("user_1"))

    def test_agentic_tool_executes_from_a_worker_thread(self):
        backend = Backend(make_tool())

        async def body(tools):
            (tool,) = await tools.get("user_1", slug="GITHUB_STAR")
            assert get_execution_target(tool.execute_tool) == "user_1"
            return await asyncio.to_thread(tool, repo="composio")

        result = run_with_tools(backend, body, provider=FakeAgenticProvider())

        assert result["successful"] is True
        (body_sent,) = backend.bodies("/execute/GITHUB_STAR")
        assert body_sent["arguments"] == {"repo": "composio"}
        assert body_sent["user_id"] == "user_1"

    def test_agentic_tool_refuses_to_block_its_own_loop(self):
        async def body(tools):
            (tool,) = await tools.get("user_1", slug="GITHUB_STAR")
            return tool(repo="composio")

        with pytest.raises(SyncCallOnEventLoopError):
            run_with_tools(Backend(make_tool()), body, provider=FakeAgenticProvider())

    def test_tool_router_execute_bridges_to_the_session_endpoint(self):
        def execute(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                json={
                    "data": {"ok": True},
                    "error": None,
                    "log_id": "log_1",
                    "result_type": "completed",
                },
            )

        backend = Backend(make_tool(), execute=execute)

        @after_execute
        def tag(tool, toolkit, response):
            response["data"]["toolkit"] = toolkit
            return response

        async def body(tools):
            execute_tool = tools._wrap_execute_tool_for_tool_router(
                "session_1", modifiers=[tag]
            )
            return await asyncio.to_thread(execute_tool, "GITHUB_STAR", {"a": 1})

        result = run_with_tools(backend, body, provider=FakeAgenticProvider())

        assert result == {
            "data": {"ok": True, "toolkit": "github"},
            "error": None,
            "successful": True,
            "log_id": "log_1",
            "result_type": "completed",
        }
        (body_sent,) = backend.bodies("/session/session_1/execute")
        assert body_sent["arguments"] == {"a": 1}
        assert body_sent["tool_slug"] == "GITHUB_STAR"


class TestExecute:
    def test_execute_applies_modifiers_around_the_call(self):
        backend = Backend(make_tool())

        @before_execute
        def add_argument(tool, toolkit, params):
            params["arguments"]["added"] = toolkit
            params["connected_account_id"] = "ca_modified"
            return params

        @after_execute
        def mark(tool, toolkit, response):
            response["data"]["marked"] = tool
            return response

        result = run_with_tools(
            backend,
            lambda tools: tools.execute(
                "GITHUB_STAR",
                {"repo": "composio"},
                user_id="user_1",
                version="v1.0.0",
                modifiers=[add_argument, mark],
            ),
        )

        assert result == {
            "data": {"ok": True, "marked": "GITHUB_STAR"},
            "error": None,
            "successful": True,
        }
        (body_sent,) = backend.bodies("/execute/GITHUB_STAR")
        assert body_sent["arguments"] == {"repo": "composio", "added": "github"}
        assert body_sent["connected_account_id"] == "ca_modified"
        assert body_sent["user_id"] == "user_1"
        assert body_sent["version"] == "v1.0.0"

    def test_schema_is_fetched_once(self):
        backend = Backend(make_tool())

        async def body(tools):
            for _ in range(2):
                await tools.execute("GITHUB_STAR", {}, version="v1.0.0")

        run_with_tools(backend, body)

        assert [request.method for request in backend.requests] == [
            "GET",
            "POST",
            "POST",
        ]

    def test_latest_version_requires_an_explicit_opt_in(self):
        backend = Backend(make_tool())

        with pytest.raises(ToolVersionRequiredError):
            run_with_tools(backend, lambda tools: tools.execute("GITHUB_STAR", {}))
        assert backend.bodies("/execute/GITHUB_STAR") == []

        run_with_tools(
            backend,
            lambda tools: tools.execute(
                "GITHUB_STAR", {}, dangerously_skip_version_check=True
            ),
        )
        assert len(backend.bodies("/execute/GITHUB_STAR")) == 1

    def test_unknown_slug_is_a_tool_not_found_error(self):
        with pytest.raises(ToolNotFoundError, match="MISSING"):
            run_with_tools(Backend(), lambda tools: tools.execute("MISSING", {}))

    def test_other_status_errors_are_not_remapped(self):
        def unauthorized(request: httpx.Request) -> httpx.Response:
            return httpx.Response(401, json={"error": {"message": "bad key"}})

        with pytest.raises(ComposioError) as raised:
            run_with_tools(unauthorized, lambda tools: tools.execute("ANY", {}))
        assert not isinstance(raised.value, ToolNotFoundError)

    def test_execute_is_not_retried(self):
        def fail(request: httpx.Request) -> httpx.Response:
            return httpx.Response(503, json={"error": {"message": "unavailable"}})

        backend = Backend(make_tool(), execute=fail)

        with pytest.raises(ComposioError):
            run_with_tools(
                backend,
                lambda tools: tools.execute("GITHUB_STAR", {}, version="v1.0.0"),
            )
        assert len(backend.bodies("/execute/GITHUB_STAR")) == 1

    def test_proxy_is_not_retried_and_omits_unset_fields(self):
        def fail(request: httpx.Request) -> httpx.Response:
            return httpx.Response(503, json={"error": {"message": "unavailable"}})

        backend = Backend(execute=fail)

        with pytest.raises(ComposioError):
            run_with_tools(
                backend,
                lambda tools: tools.proxy(
                    endpoint="/user", method="GET", connected_account_id="ca_1"
                ),
            )
        (body_sent,) = backend.bodies("/tools/execute/proxy")
        assert body_sent == {
            "endpoint": "/user",
            "method": "GET",
            "connected_account_id": "ca_1",
        }


class TestFiles:
    INPUT = {
        "type": "object",
        "properties": {
            "attachment": {"type": "string", "file_uploadable": True},
            "note": {"type": "string"},
        },
    }
    OUTPUT = {
        "type": "object",
        "properties": {
            "data": {
                "type": "object",
                "properties": {
                    "file": {"type": "object", "file_downloadable": True},
                },
            }
        },
    }

    def test_empty_file_value_is_dropped_without_auto_upload(self):
        backend = Backend(make_tool(input_parameters=self.INPUT))

        run_with_tools(
            backend,
            lambda tools: tools.execute(
                "GITHUB_STAR", {"attachment": "", "note": "hi"}, version="v1.0.0"
            ),
        )

        (body_sent,) = backend.bodies("/execute/GITHUB_STAR")
        assert body_sent["arguments"] == {"note": "hi"}

    def test_upload_presigns_on_the_loop_and_transfers_off_it(self, tmp_path: Path):
        source = tmp_path / "report.txt"
        source.write_text("hello")
        loop_threads: t.List[bool] = []

        def execute(request: httpx.Request) -> httpx.Response:
            if request.url.path.endswith("/files/upload/request"):
                return httpx.Response(
                    200,
                    json={
                        "id": "file_1",
                        "key": "s3/key/report.txt",
                        "new_presigned_url": "https://bucket.invalid/put",
                        "type": "new",
                        "metadata": {"storage_backend": "s3"},
                    },
                )
            return httpx.Response(200, json=EXECUTED)

        backend = Backend(make_tool(input_parameters=self.INPUT), execute=execute)
        uploads: t.List[t.Tuple[str, str]] = []

        async def body(tools):
            loop = asyncio.get_running_loop()

            def fake_upload(url, data, mimetype):
                try:
                    on_loop = asyncio.get_running_loop() is loop
                except RuntimeError:
                    on_loop = False
                loop_threads.append(on_loop)
                uploads.append((url, mimetype))

            with patch(
                "composio.core.models._files._upload_to_presigned_url",
                side_effect=fake_upload,
            ):
                return await tools.execute(
                    "GITHUB_STAR",
                    {"attachment": str(source), "note": "hi"},
                    version="v1.0.0",
                )

        run_with_tools(
            backend,
            body,
            dangerously_allow_auto_upload_download_files=True,
            file_upload_dirs=[str(tmp_path)],
        )

        assert uploads == [("https://bucket.invalid/put", "text/plain")]
        assert loop_threads == [False]
        (presign,) = backend.bodies("/files/upload/request")
        assert presign["filename"] == "report.txt"
        assert presign["tool_slug"] == "GITHUB_STAR"
        (body_sent,) = backend.bodies("/execute/GITHUB_STAR")
        assert body_sent["arguments"] == {
            "attachment": {
                "name": "report.txt",
                "mimetype": "text/plain",
                "s3key": "s3/key/report.txt",
            },
            "note": "hi",
        }

    def test_upload_outside_the_allowlist_is_rejected(self, tmp_path: Path):
        source = tmp_path / "report.txt"
        source.write_text("hello")
        allowed = tmp_path / "allowed"
        allowed.mkdir()
        backend = Backend(make_tool(input_parameters=self.INPUT))

        with pytest.raises(ComposioError):
            run_with_tools(
                backend,
                lambda tools: tools.execute(
                    "GITHUB_STAR", {"attachment": str(source)}, version="v1.0.0"
                ),
                dangerously_allow_auto_upload_download_files=True,
                file_upload_dirs=[str(allowed)],
            )
        assert backend.bodies("/files/upload/request") == []
        assert backend.bodies("/execute/GITHUB_STAR") == []

    def test_download_substitutes_a_local_path(self, tmp_path: Path):
        def execute(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                json={
                    "data": {
                        "file": {
                            "name": "out.txt",
                            "mimetype": "text/plain",
                            "s3url": "https://bucket.invalid/out.txt",
                        }
                    },
                    "error": None,
                    "successful": True,
                },
            )

        backend = Backend(make_tool(output_parameters=self.OUTPUT), execute=execute)
        downloaded: t.List[t.Tuple[Path, Path]] = []

        def fake_download(self, outdir, chunk_size=0, *, root, max_size=0):
            downloaded.append((outdir, root))
            return outdir / self.name

        with patch(
            "composio.core.models._files.FileDownloadable.download", fake_download
        ):
            result = run_with_tools(
                backend,
                lambda tools: tools.execute("GITHUB_STAR", {}, version="v1.0.0"),
                dangerously_allow_auto_upload_download_files=True,
                file_download_dir=str(tmp_path),
            )

        assert downloaded == [(tmp_path / "github" / "GITHUB_STAR", tmp_path)]
        assert result["data"]["file"] == str(
            tmp_path / "github" / "GITHUB_STAR" / "out.txt"
        )


def chat_completion(*calls: t.Tuple[str, str]) -> ChatCompletion:
    return ChatCompletion.model_validate(
        {
            "id": "chatcmpl_1",
            "object": "chat.completion",
            "created": 0,
            "model": "gpt",
            "choices": [
                {
                    "index": 0,
                    "finish_reason": "tool_calls",
                    "message": {
                        "role": "assistant",
                        "tool_calls": [
                            {
                                "id": f"call_{index}",
                                "type": "function",
                                "function": {"name": name, "arguments": arguments},
                            }
                            for index, (name, arguments) in enumerate(calls)
                        ],
                    },
                }
            ],
        }
    )


class TestProviderHooks:
    def test_sync_helpers_point_at_the_async_ones(self):
        provider = OpenAIProvider()

        async def body(tools):
            return provider.handle_tool_calls(
                user_id="user_1", response=chat_completion(("GITHUB_STAR", "{}"))
            )

        with pytest.raises(ComposioError, match="ahandle_tool_calls"):
            run_with_tools(Backend(make_tool()), body, provider=provider)

    def test_aexecute_tool_skips_the_version_check(self):
        backend = Backend(make_tool())
        provider = OpenAIProvider()

        async def body(tools):
            return await provider.aexecute_tool(
                slug="GITHUB_STAR", arguments={"a": 1}, user_id="user_1"
            )

        assert run_with_tools(backend, body, provider=provider)["successful"] is True
        (body_sent,) = backend.bodies("/execute/GITHUB_STAR")
        assert body_sent["arguments"] == {"a": 1}

    def test_ahandle_tool_calls_runs_every_call_in_order(self):
        backend = Backend(make_tool(), make_tool("GITHUB_FORK"))
        provider = OpenAIProvider()

        async def body(tools):
            return await provider.ahandle_tool_calls(
                user_id="user_1",
                response=chat_completion(
                    ("GITHUB_STAR", '{"repo": "a"}'), ("GITHUB_FORK", "")
                ),
            )

        results = run_with_tools(backend, body, provider=provider)

        assert [result["successful"] for result in results] == [True, True]
        assert backend.bodies("/execute/GITHUB_STAR")[0]["arguments"] == {"repo": "a"}
        assert backend.bodies("/execute/GITHUB_FORK")[0]["arguments"] == {}

    def test_ahandle_tool_calls_awaits_an_async_session(self):
        class Session:
            def __init__(self) -> None:
                self.calls: t.List[t.Tuple[str, t.Optional[dict]]] = []

            async def execute(self, tool_slug, *, arguments=None):
                self.calls.append((tool_slug, arguments))
                return ToolRouterSessionExecuteResponse.model_construct(
                    data={"ok": True},
                    error=None,
                    log_id="log_1",
                    result_type="completed",
                )

        session = Session()
        provider = OpenAIProvider()

        async def body(tools):
            return await provider.ahandle_tool_calls(
                session=session, response=chat_completion(("GITHUB_STAR", '{"a": 1}'))
            )

        (result,) = run_with_tools(Backend(), body, provider=provider)

        assert session.calls == [("GITHUB_STAR", {"a": 1})]
        assert result["successful"] is True
        assert result["log_id"] == "log_1"

    def test_session_and_modifiers_are_mutually_exclusive(self):
        provider = OpenAIProvider()

        async def body(tools):
            return await provider.ahandle_tool_calls(
                session=object(),  # type: ignore[arg-type]
                response=chat_completion(("GITHUB_STAR", "{}")),
                modifiers=[],
            )

        with pytest.raises(ValueError, match="Tool Router session"):
            run_with_tools(Backend(), body, provider=provider)

    def test_responses_provider_ahandle_tool_calls(self):
        backend = Backend(make_tool())
        provider = OpenAIResponsesProvider()
        response = Response.model_construct(
            output=[
                ResponseFunctionToolCall(
                    type="function_call",
                    call_id="call_1",
                    name="GITHUB_STAR",
                    arguments='{"repo": "a"}',
                )
            ]
        )

        async def body(tools):
            return await provider.ahandle_tool_calls(
                user_id="user_1", response=response
            )

        (result,) = run_with_tools(backend, body, provider=provider)

        assert result["successful"] is True
        assert backend.bodies("/execute/GITHUB_STAR")[0]["arguments"] == {"repo": "a"}
