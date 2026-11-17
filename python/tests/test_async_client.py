"""AsyncHttpClient, the async resource base, and the sync-callback bridge."""

import asyncio
import logging
import threading
import typing as t
import warnings

import httpx
import pytest
from composio_client import APIStatusError, ProjectAuth

from composio.client import AsyncHttpClient, HttpClient
from composio.core.models._async.auth_configs import AsyncAuthConfigs
from composio.core.models._async.internal import AsyncInternal
from composio.core.models._async.keyring import AsyncKeyring
from composio.core.models._async.logs import AsyncLogs
from composio.core.models.base import allow_tracking
from composio.exceptions import ComposioDeprecationWarning, ComposioError
from composio.utils import toolkit_deprecation
from composio.utils.async_bridge import (
    EventLoopNotRunningError,
    SyncCallOnEventLoopError,
    run_callback,
    run_io,
    run_on_loop,
    sync_bridge,
)

Handler = t.Callable[[httpx.Request], httpx.Response]


def _client(handler: Handler, **kwargs: t.Any) -> AsyncHttpClient:
    return AsyncHttpClient(
        provider="openai",
        auth=ProjectAuth("ak_test"),
        base_url="https://backend.composio.dev",
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        **kwargs,
    )


def _recording(
    payload: t.Any = None, status: int = 200, headers: t.Optional[dict] = None
) -> t.Tuple[Handler, t.List[httpx.Request]]:
    requests: t.List[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(status, json=payload or {}, headers=headers)

    return handler, requests


@pytest.fixture(autouse=True)
def _no_tracking() -> t.Iterator[None]:
    token = allow_tracking.set(False)
    yield
    allow_tracking.reset(token)


class TestAsyncHttpClient:
    def test_sends_the_same_identity_and_credential_as_the_sync_client(self) -> None:
        handler, requests = _recording({"keys": [], "active_kid": "k"})
        sync_requests: t.List[httpx.Request] = []

        def sync_handler(request: httpx.Request) -> httpx.Response:
            sync_requests.append(request)
            return httpx.Response(200, json={"keys": [], "active_kid": "k"})

        async def run() -> None:
            await _client(handler).keyring.list_transfer_keys()

        asyncio.run(run())
        HttpClient(
            provider="openai",
            auth=ProjectAuth("ak_test"),
            base_url="https://backend.composio.dev",
            http_client=httpx.Client(transport=httpx.MockTransport(sync_handler)),
        ).keyring.list_transfer_keys()

        sent, expected = requests[0].headers, sync_requests[0].headers
        assert sent["x-api-key"] == "ak_test"
        assert len(sent["x-request-id"]) == 32
        varying = {"x-request-id", "user-agent"}
        for name, value in expected.items():
            if name in varying or name.startswith("x-stainless"):
                continue
            assert sent.get(name) == value, name
        assert requests[0].url == sync_requests[0].url

    def test_request_id_from_request_context_is_sent(self) -> None:
        handler, requests = _recording({"keys": [], "active_kid": "k"})

        async def run() -> None:
            client = _client(handler)
            client.request_ctx.set({"id": "req_pinned", "provider": "openai"})
            await client.keyring.list_transfer_keys()

        asyncio.run(run())
        assert requests[0].headers["x-request-id"] == "req_pinned"

    def test_status_errors_are_composio_errors(self) -> None:
        handler, _ = _recording({"error": {"message": "bad key"}}, status=401)

        async def run() -> None:
            await _client(handler, max_retries=0).keyring.list_transfer_keys()

        with pytest.raises(ComposioError) as raised:
            asyncio.run(run())
        assert isinstance(raised.value, APIStatusError)
        assert raised.value.status_code == 401

    def test_without_retries_is_a_cached_async_sibling(self) -> None:
        attempts = 0

        def handler(request: httpx.Request) -> httpx.Response:
            nonlocal attempts
            attempts += 1
            return httpx.Response(500, json={})

        async def run() -> None:
            client = _client(handler, max_retries=2)
            sibling = client.without_retries
            assert isinstance(sibling, AsyncHttpClient)
            assert sibling is client.without_retries
            assert sibling.max_retries == 0
            assert sibling.provider == "openai"
            with pytest.raises(APIStatusError):
                await sibling.keyring.list_transfer_keys()

        asyncio.run(run())
        assert attempts == 1

    def test_lifecycle_records_reach_the_instance_logger(self) -> None:
        handler, _ = _recording({"keys": [], "active_kid": "k"})
        records: t.List[logging.LogRecord] = []

        class Capture(logging.Handler):
            def emit(self, record: logging.LogRecord) -> None:
                records.append(record)

        logger = logging.getLogger("test_async_client.instance")
        logger.setLevel(logging.DEBUG)
        logger.propagate = False
        capture = Capture()
        logger.addHandler(capture)
        try:

            async def run() -> None:
                await _client(handler, logger=logger).keyring.list_transfer_keys()

            asyncio.run(run())
        finally:
            logger.removeHandler(capture)
        assert any("/keyring" in record.getMessage() for record in records)

    def test_deprecated_toolkit_header_warns(self) -> None:
        toolkit_deprecation._reset_for_tests()
        handler, _ = _recording(
            {"keys": [], "active_kid": "k"},
            headers={toolkit_deprecation.HEADER: '"GITHUB";version="20250101_00"'},
        )

        async def run() -> None:
            await _client(handler).keyring.list_transfer_keys()

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            asyncio.run(run())
        toolkit_deprecation._reset_for_tests()
        assert any(
            issubclass(warning.category, ComposioDeprecationWarning)
            and "GITHUB" in str(warning.message)
            for warning in caught
        )

    def test_user_supplied_client_keeps_its_hooks_and_gets_ours_once(self) -> None:
        async def mine(response: httpx.Response) -> None: ...

        handler, _ = _recording()
        http_client = httpx.AsyncClient(
            transport=httpx.MockTransport(handler), event_hooks={"response": [mine]}
        )
        for _ in range(2):
            AsyncHttpClient(
                provider="openai", auth=ProjectAuth("ak"), http_client=http_client
            )
        hooks = http_client.event_hooks["response"]
        assert hooks[0] is mine
        assert hooks.count(toolkit_deprecation.async_response_hook) == 1


class TestAsyncResources:
    def test_keyring_logs_and_internal(self) -> None:
        paths: t.List[t.Tuple[str, str]] = []

        def handler(request: httpx.Request) -> httpx.Response:
            paths.append((request.method, request.url.path))
            if request.url.path.endswith("/realtime/credentials"):
                return httpx.Response(
                    200,
                    json={
                        "pusher_key": "pk",
                        "project_id": "pr",
                        "pusher_cluster": "mt1",
                    },
                )
            return httpx.Response(200, json={})

        async def run() -> str:
            client = _client(handler)
            await AsyncKeyring(client).list_transfer_keys()
            await AsyncLogs(client).search(limit=5)
            await AsyncLogs(client).get("log_1")
            credentials = await AsyncInternal(client).get_sdk_realtime_credentials()
            return credentials.pusher_cluster

        assert asyncio.run(run()) == "mt1"
        assert [method for method, _ in paths] == ["GET", "POST", "GET", "GET"]
        assert paths[2][1].endswith("/log_1")

    def test_auth_configs(self) -> None:
        seen: t.List[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(request)
            if request.method == "POST":
                return httpx.Response(
                    200,
                    json={
                        "toolkit": {"slug": "github"},
                        "auth_config": {
                            "id": "ac_1",
                            "auth_scheme": "OAUTH2",
                            "is_composio_managed": True,
                        },
                    },
                )
            return httpx.Response(200, json={"success": True, "message": "ok"})

        async def run() -> str:
            auth_configs = AsyncAuthConfigs(_client(handler))
            created = await auth_configs.create(
                "github", {"type": "use_composio_managed_auth"}
            )
            await auth_configs.enable("ac_1")
            await auth_configs.disable("ac_1")
            await auth_configs.delete("ac_1")
            return created.id

        assert asyncio.run(run()) == "ac_1"
        assert [request.method for request in seen] == [
            "POST",
            "PATCH",
            "PATCH",
            "DELETE",
        ]
        assert seen[1].url.path.endswith("/ac_1/ENABLED")
        assert seen[2].url.path.endswith("/ac_1/DISABLED")

    def test_tracing_wraps_coroutine_methods_and_reports_errors(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        events: t.List[t.Any] = []
        monkeypatch.setattr(
            "composio.core.models.base.push_event",
            lambda event: events.append(event),
        )
        handler, _ = _recording(status=404)

        async def run() -> None:
            allow_tracking.set(True)
            await AsyncLogs(_client(handler, max_retries=0)).get("missing")

        with pytest.raises(APIStatusError):
            asyncio.run(run())
        kind, payload = events[0]
        assert kind == "error"
        assert payload["functionName"] == "AsyncLogs.get"
        assert payload["durationMs"] >= 0


def test_tracking_opt_out_holds_in_tasks_that_do_not_inherit_the_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from composio import AsyncComposio

    events: t.List[t.Any] = []
    monkeypatch.setattr(
        "composio.core.models.base.push_event", lambda event: events.append(event)
    )
    handler, _ = _recording({"items": [], "next_cursor": None})
    allow_tracking.set(True)
    sdk = AsyncComposio(
        api_key="ak",
        allow_tracking=False,
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )
    allow_tracking.set(True)  # what a task outside the constructor's context sees

    asyncio.run(sdk.logs.search(limit=1))
    assert events == []


class TestSyncBridge:
    def test_runs_the_coroutine_on_the_sdk_loop_from_a_worker_thread(self) -> None:
        async def run() -> t.Tuple[int, bool]:
            loop_thread = threading.get_ident()
            ran_on: t.List[int] = []

            async def double(value: int) -> int:
                ran_on.append(threading.get_ident())
                await asyncio.sleep(0)
                return value * 2

            call = sync_bridge(double)
            result = await asyncio.to_thread(call, 21)
            return result, ran_on == [loop_thread]

        assert asyncio.run(run()) == (42, True)

    def test_propagates_the_coroutine_exception(self) -> None:
        async def run() -> None:
            async def boom() -> None:
                raise ValueError("nope")

            await asyncio.to_thread(sync_bridge(boom))

        with pytest.raises(ValueError, match="nope"):
            asyncio.run(run())

    def test_refuses_to_block_the_loop_it_would_wait_on(self) -> None:
        async def run() -> None:
            async def work() -> int:
                return 1

            with warnings.catch_warnings():
                warnings.simplefilter("error")  # the coroutine must be closed
                with pytest.raises(SyncCallOnEventLoopError):
                    sync_bridge(work)()
                with pytest.raises(SyncCallOnEventLoopError):
                    run_on_loop(asyncio.get_running_loop(), work())

        asyncio.run(run())

    def test_refuses_a_loop_that_is_not_running(self) -> None:
        async def work() -> int:
            return 1

        async def make() -> t.Callable[..., int]:
            return sync_bridge(work)

        with warnings.catch_warnings():
            warnings.simplefilter("error")
            idle = asyncio.new_event_loop()
            try:
                call = idle.run_until_complete(make())
                with pytest.raises(EventLoopNotRunningError):
                    call()
            finally:
                idle.close()
            with pytest.raises(EventLoopNotRunningError):
                call()

    def test_sdk_threads_do_not_depend_on_the_default_executor(self) -> None:
        """Bridged callbacks filling the default executor must not starve the
        worker threads the loop needs to finish their calls."""
        from concurrent.futures import ThreadPoolExecutor

        async def run() -> t.List[int]:
            loop = asyncio.get_running_loop()
            loop.set_default_executor(ThreadPoolExecutor(max_workers=2))

            async def nested(value: int) -> int:
                # callback thread -> loop -> io thread, as a custom tool that
                # executes a tool with a file upload does
                inner = sync_bridge(lambda: run_io(lambda: value))
                return await run_callback(inner)

            calls = [asyncio.to_thread(sync_bridge(nested), i) for i in range(6)]
            return await asyncio.wait_for(asyncio.gather(*calls), timeout=10)

        assert asyncio.run(run()) == list(range(6))

    def test_worker_threads_see_the_callers_context(self) -> None:
        async def run() -> bool:
            token = allow_tracking.set(False)
            try:
                return await run_io(allow_tracking.get)
            finally:
                allow_tracking.reset(token)

        assert asyncio.run(run()) is False
