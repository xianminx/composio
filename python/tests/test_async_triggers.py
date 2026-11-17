"""Tests for the async triggers resource and its realtime subscription."""

import asyncio
import base64
import hashlib
import hmac
import json
import threading
import time
import typing as t
from unittest.mock import AsyncMock, Mock, patch

import httpx
import pytest
from composio_client import NotFoundError

from composio import exceptions
from composio.core.models._async import triggers as async_triggers
from composio.core.models._async.triggers import (
    AsyncTriggers,
    AsyncTriggerSubscription,
    _AsyncSubscriptionBuilder,
)
from composio.core.models.triggers import Triggers, WebhookVersion
from composio.exceptions import ComposioSDKTimeoutError

SECRET = "test-webhook-secret-12345"


def _client() -> Mock:
    client = Mock()
    client.base_url = "https://api.example.com"
    client.triggers_types.retrieve = AsyncMock(return_value=Mock(slug="T"))
    client.triggers_types.list = AsyncMock(return_value=Mock(items=[]))
    client.triggers_types.retrieve_enum = AsyncMock(return_value=["A", "B"])
    client.trigger_instances.list_active = AsyncMock(return_value=Mock(items=[]))
    client.trigger_instances.upsert = AsyncMock(return_value=Mock(trigger_id="ti_1"))
    client.trigger_instances.manage.update = AsyncMock(return_value=Mock())
    client.trigger_instances.manage.delete = AsyncMock(return_value=Mock())
    return client


def _frame(trigger_slug: str = "GITHUB_PUSH_EVENT", user_id: str = "user-1") -> str:
    return json.dumps(
        {
            "id": "evt-1",
            "type": "composio.trigger.message",
            "metadata": {
                "log_id": "log-1",
                "trigger_slug": trigger_slug,
                "trigger_id": "ti_abc",
                "connected_account_id": "ca_abc",
                "auth_config_id": "ac_abc",
                "user_id": user_id,
            },
            "data": {"subject": "hello"},
        }
    )


class FakeConnection:
    def __init__(self) -> None:
        self.bindings: t.Dict[str, t.Callable[[str], None]] = {}
        self.alive = True
        self.disconnect_calls = 0
        self.reconnect_calls: t.List[t.Dict[str, t.Any]] = []
        self.logger: t.Any = None

    def bind(self, event_name: str, callback: t.Callable[[str], None]) -> None:
        self.bindings[event_name] = callback

    def is_alive(self) -> bool:
        return self.alive

    def disconnect(self) -> None:
        self.disconnect_calls += 1
        self.alive = False

    def reconnect(self, **kwargs: t.Any) -> None:
        self.reconnect_calls.append(kwargs)


class FakeChannel:
    def __init__(self, connection: FakeConnection) -> None:
        self.connection = connection
        self.bindings: t.Dict[str, t.Callable[[str], None]] = {}

    def bind(self, event_name: str, callback: t.Callable[[str], None]) -> None:
        self.bindings[event_name] = callback

    def emit(self, event_name: str, data: str) -> None:
        """Deliver a frame from a non-loop thread, as pysher does."""
        thread = threading.Thread(target=self.bindings[event_name], args=(data,))
        thread.start()
        thread.join()


class FakePusher:
    """Stands in for ``pysher.Pusher``: ``connect()`` fires the established
    event on a separate thread unless ``establish`` is False."""

    def __init__(
        self,
        establish: bool = True,
        subscribe_error: t.Optional[Exception] = None,
    ) -> None:
        self.connection = FakeConnection()
        self.channel = FakeChannel(self.connection)
        self.establish = establish
        self.subscribe_error = subscribe_error
        self.subscribed: t.List[str] = []
        self.disconnect_calls = 0

    def subscribe(self, channel_name: str) -> FakeChannel:
        if self.subscribe_error is not None:
            raise self.subscribe_error
        self.subscribed.append(channel_name)
        return self.channel

    def connect(self) -> None:
        if not self.establish:
            return
        handler = self.connection.bindings["pusher:connection_established"]
        threading.Thread(target=handler, args=("{}",)).start()

    def disconnect(self) -> None:
        self.disconnect_calls += 1
        self.connection.disconnect()


async def _subscribe(pusher: FakePusher, **kwargs: t.Any) -> AsyncTriggerSubscription:
    client = _client()
    client.get = AsyncMock(
        return_value=Mock(project_id="proj", pusher_key="k", pusher_cluster="mt1")
    )
    with patch.object(
        _AsyncSubscriptionBuilder, "_get_pusher_instance", return_value=pusher
    ):
        return await AsyncTriggers(client=client).subscribe(**kwargs)


async def _settle() -> None:
    """Let `call_soon_threadsafe` callbacks and the tasks they start finish."""
    for _ in range(5):
        await asyncio.sleep(0)


class TestRest:
    def test_get_type_and_lists(self):
        client = _client()
        triggers = AsyncTriggers(client=client, toolkit_versions={"github": "v1"})

        async def run():
            await triggers.get_type("GITHUB_PUSH")
            await triggers.list(limit=5, toolkit_slugs=["github"])
            await triggers.list_active(trigger_ids=["ti_1"], limit=2)
            return await triggers.list_enum()

        assert asyncio.run(run()) == ["A", "B"]
        client.triggers_types.retrieve.assert_awaited_once_with(
            slug="GITHUB_PUSH", toolkit_versions={"github": "v1"}
        )
        list_kwargs = client.triggers_types.list.await_args.kwargs
        assert list_kwargs["limit"] == 5
        assert list_kwargs["toolkit_slugs"] == ["github"]
        assert list_kwargs["toolkit_versions"] == {"github": "v1"}
        active_kwargs = client.trigger_instances.list_active.await_args.kwargs
        assert active_kwargs["query_trigger_ids_1"] == ["ti_1"]
        assert active_kwargs["limit"] == 2

    def test_enable_disable_delete(self):
        client = _client()
        triggers = AsyncTriggers(client=client)

        async def run():
            await triggers.enable("ti_1")
            await triggers.disable("ti_1")
            await triggers.delete("ti_1")

        asyncio.run(run())
        calls = client.trigger_instances.manage.update.await_args_list
        assert [c.kwargs for c in calls] == [
            {"status": "enable"},
            {"status": "disable"},
        ]
        client.trigger_instances.manage.delete.assert_awaited_once_with("ti_1")

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"connected_account_id": "ca_1"},
            {"user_id": "user_1"},
            {"user_id": "user_1", "connected_account_id": "ca_1"},
        ],
    )
    def test_create_variants(self, kwargs):
        client = _client()
        triggers = AsyncTriggers(client=client)

        result = asyncio.run(
            triggers.create(
                "GITHUB_PUSH",
                trigger_config={"repo": "x"},
                type="poll",
                polling_interval=5,
                **kwargs,
            )
        )

        assert result.trigger_id == "ti_1"
        sent = client.trigger_instances.upsert.await_args.kwargs
        assert sent["slug"] == "GITHUB_PUSH"
        assert sent["body_trigger_config_1"] == {"repo": "x"}
        assert sent["type"] == "poll"
        assert sent["polling_interval"] == 5
        for name in ("user_id", "connected_account_id"):
            if name in kwargs:
                assert sent[name] == kwargs[name]
            else:
                assert not isinstance(sent[name], str)

    def test_create_requires_a_target(self):
        triggers = AsyncTriggers(client=_client())
        with pytest.raises(exceptions.InvalidParams):
            asyncio.run(triggers.create("GITHUB_PUSH", user_id="  "))

    def test_create_maps_unknown_slug(self):
        client = _client()
        response = httpx.Response(
            404, request=httpx.Request("GET", "https://api.example.com")
        )
        client.triggers_types.retrieve.side_effect = NotFoundError(
            "nope", response=response, body=None
        )
        triggers = AsyncTriggers(client=client)

        with pytest.raises(exceptions.TriggerTypeNotFound):
            asyncio.run(triggers.create("NOPE", user_id="u"))
        client.trigger_instances.upsert.assert_not_awaited()

    def test_set_webhook_subscription_delegates(self):
        client = _client()
        subscriptions = Mock()
        subscriptions.set = AsyncMock(return_value={"id": "sub_1"})

        with patch.object(
            async_triggers, "_async_webhook_subscriptions", return_value=subscriptions
        ) as factory:
            result = asyncio.run(
                AsyncTriggers(client=client).set_webhook_subscription(
                    webhook_url="https://app.example.com/hook"
                )
            )

        assert result == {"id": "sub_1"}
        factory.assert_called_once_with(client)
        subscriptions.set.assert_awaited_once_with(
            webhook_url="https://app.example.com/hook",
            enabled_events=None,
            version=WebhookVersion.V3,
        )


class TestSubscribe:
    def test_frames_reach_sync_and_async_handlers_on_the_loop(self):
        pusher = FakePusher()

        async def run():
            subscription = await _subscribe(pusher)
            loop_thread = threading.get_ident()
            seen: t.List[t.Tuple[str, str, bool]] = []

            @subscription.handle()
            def sync_handler(event):
                seen.append(
                    (
                        "sync",
                        event["trigger_slug"],
                        threading.get_ident() == loop_thread,
                    )
                )

            @subscription.handle()
            async def async_handler(event):
                await asyncio.sleep(0)
                seen.append(
                    (
                        "async",
                        event["trigger_slug"],
                        threading.get_ident() == loop_thread,
                    )
                )

            pusher.channel.emit("trigger_to_client", _frame())
            await _settle()
            return subscription, seen

        subscription, seen = asyncio.run(run())

        assert pusher.subscribed == ["private-proj_triggers"]
        assert subscription.is_alive()
        assert sorted(seen) == [
            ("async", "GITHUB_PUSH_EVENT", True),
            ("sync", "GITHUB_PUSH_EVENT", True),
        ]

    def test_filters(self):
        pusher = FakePusher()

        async def run():
            subscription = await _subscribe(pusher)
            seen: t.List[str] = []

            @subscription.handle(toolkit="github")
            async def github(event):
                seen.append("github")

            @subscription.handle(toolkit="slack")
            async def slack(event):
                seen.append("slack")

            @subscription.handle(user_id="someone-else")
            def other_user(event):
                seen.append("other_user")

            pusher.channel.emit("trigger_to_client", _frame())
            await _settle()
            return seen

        assert asyncio.run(run()) == ["github"]

    def test_chunked_events(self):
        pusher = FakePusher()
        frame = _frame()
        half = len(frame) // 2

        async def run():
            subscription = await _subscribe(pusher)
            seen: t.List[t.Any] = []
            subscription.handle()(lambda event: seen.append(event["payload"]))

            pusher.channel.emit(
                "chunked-trigger_to_client",
                json.dumps(
                    {"id": "c1", "index": 1, "chunk": frame[half:], "final": False}
                ),
            )
            await _settle()
            assert seen == []
            pusher.channel.emit(
                "chunked-trigger_to_client",
                json.dumps(
                    {"id": "c1", "index": 0, "chunk": frame[:half], "final": True}
                ),
            )
            await _settle()
            return seen

        assert asyncio.run(run()) == [{"subject": "hello"}]

    def test_handler_errors_are_contained(self):
        pusher = FakePusher()

        async def run():
            subscription = await _subscribe(pusher)
            seen: t.List[str] = []

            @subscription.handle()
            def sync_boom(event):
                raise RuntimeError("sync boom")

            @subscription.handle()
            async def async_boom(event):
                raise RuntimeError("async boom")

            @subscription.handle()
            async def survivor(event):
                seen.append("ok")

            pusher.channel.emit("trigger_to_client", _frame())
            pusher.channel.emit("trigger_to_client", "not json {")
            pusher.channel.emit("trigger_to_client", _frame())
            await _settle()
            return subscription, seen

        subscription, seen = asyncio.run(run())
        assert seen == ["ok", "ok"]
        assert subscription.is_alive()

    def test_subscription_error_frame_reaches_async_callback(self):
        pusher = FakePusher()

        async def run():
            seen: t.List[t.Dict[str, t.Any]] = []

            async def on_error(payload):
                seen.append(payload)

            await _subscribe(pusher, on_subscription_error=on_error)
            pusher.channel.emit("pusher:subscription_error", '{"status": 401}')
            pusher.channel.emit("pusher:subscription_error", "not json {")
            await _settle()
            return seen

        assert asyncio.run(run()) == [{"status": 401}, {"raw": "not json {"}]

    def test_establish_failure_raises_fast_and_disconnects(self):
        failure = exceptions.TriggerSubscriptionAuthError("auth rejected")
        pusher = FakePusher(subscribe_error=failure)
        seen: t.List[t.Dict[str, t.Any]] = []

        async def run():
            await _subscribe(pusher, timeout=15.0, on_subscription_error=seen.append)

        started = time.monotonic()
        with pytest.raises(exceptions.TriggerSubscriptionAuthError):
            asyncio.run(run())

        assert time.monotonic() - started < 5.0
        assert pusher.disconnect_calls == 1
        assert seen == [{"error": "auth rejected"}]

    def test_timeout_disconnects(self):
        pusher = FakePusher(establish=False)

        with pytest.raises(ComposioSDKTimeoutError):
            asyncio.run(_subscribe(pusher, timeout=0.2))

        assert pusher.disconnect_calls == 1

    def test_cancelled_subscribe_disconnects(self):
        pusher = FakePusher(establish=False)

        async def run():
            task = asyncio.ensure_future(_subscribe(pusher, timeout=30.0))
            await asyncio.sleep(0.05)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task

        asyncio.run(run())
        assert pusher.disconnect_calls == 1


class TestLifecycle:
    def test_stop_from_a_handler_ends_wait_forever(self):
        pusher = FakePusher()

        async def run():
            subscription = await _subscribe(pusher)

            @subscription.handle()
            async def stop_on_first(event):
                await subscription.stop()

            pusher.channel.emit("trigger_to_client", _frame())
            await asyncio.wait_for(subscription.wait_forever(), timeout=5)
            return subscription

        subscription = asyncio.run(run())
        assert not subscription.is_alive()
        assert pusher.connection.disconnect_calls == 1

    def test_wait_forever_returns_when_the_connection_dies(self):
        pusher = FakePusher()

        async def run():
            subscription = await _subscribe(pusher)
            with patch.object(async_triggers, "_WAIT_POLL_SECONDS", 0.01):
                waiter = asyncio.ensure_future(subscription.wait_forever())
                await asyncio.sleep(0.03)
                assert not waiter.done()
                pusher.connection.alive = False
                await asyncio.wait_for(waiter, timeout=5)
            return subscription

        assert asyncio.run(run()).has_errored()

    def test_wait_forever_is_cancellable(self):
        pusher = FakePusher()

        async def run():
            subscription = await _subscribe(pusher)
            waiter = asyncio.ensure_future(subscription.wait_forever())
            await asyncio.sleep(0.01)
            waiter.cancel()
            with pytest.raises(asyncio.CancelledError):
                await waiter

        asyncio.run(run())

    def test_restart_requests_a_reconnect(self):
        pusher = FakePusher()

        async def run():
            subscription = await _subscribe(pusher)
            await subscription.restart()

        asyncio.run(run())
        assert pusher.connection.reconnect_calls == [{"reconnect_interval": 0}]

    def test_frame_after_loop_closed_is_dropped(self):
        pusher = FakePusher()
        seen: t.List[t.Any] = []

        async def run():
            subscription = await _subscribe(pusher)
            subscription.handle()(seen.append)

        asyncio.run(run())
        pusher.channel.emit("trigger_to_client", _frame())
        assert seen == []


class TestWebhookParity:
    """`verify_webhook` / `parse` are the sync implementation, not a copy."""

    @staticmethod
    def _signed(payload: str, webhook_id: str, timestamp: str) -> str:
        digest = hmac.new(
            SECRET.encode(),
            f"{webhook_id}.{timestamp}.{payload}".encode(),
            hashlib.sha256,
        ).digest()
        return f"v1,{base64.b64encode(digest).decode()}"

    @pytest.mark.parametrize(
        "body",
        [
            {
                "id": "evt-123",
                "timestamp": "2026-01-01T00:00:00Z",
                "type": "composio.trigger.message",
                "metadata": {
                    "log_id": "log-123",
                    "trigger_slug": "GITHUB_PUSH_EVENT",
                    "trigger_id": "trigger-nano-123",
                    "connected_account_id": "conn-nano-123",
                    "auth_config_id": "auth-nano-123",
                    "user_id": "user-456",
                },
                "data": {"action": "push"},
            },
            {
                "type": "github_push_event",
                "timestamp": "2026-01-01T00:00:00Z",
                "log_id": "log-123",
                "data": {
                    "connection_id": "conn-123",
                    "connection_nano_id": "conn-nano-123",
                    "trigger_nano_id": "trigger-nano-123",
                    "trigger_id": "trigger-123",
                    "user_id": "user-456",
                    "action": "push",
                },
            },
            {
                "trigger_name": "GITHUB_PUSH_EVENT",
                "connection_id": "conn-123",
                "trigger_id": "trigger-123",
                "payload": {"action": "push"},
                "log_id": "log-123",
            },
        ],
        ids=["v3", "v2", "v1"],
    )
    def test_same_result_as_sync(self, body):
        sync, asynchronous = Triggers(client=Mock()), AsyncTriggers(client=_client())
        payload = json.dumps(body)
        webhook_id, timestamp = "msg_test123", str(int(time.time()))
        signature = self._signed(payload, webhook_id, timestamp)
        kwargs = dict(
            id=webhook_id,
            payload=payload,
            secret=SECRET,
            signature=signature,
            timestamp=timestamp,
        )
        headers = {
            "webhook-id": webhook_id,
            "webhook-timestamp": timestamp,
            "webhook-signature": signature,
        }

        assert asynchronous.verify_webhook(**kwargs) == sync.verify_webhook(**kwargs)
        assert asynchronous.parse(
            body=payload, headers=headers, verify_secret=SECRET
        ) == sync.parse(body=payload, headers=headers, verify_secret=SECRET)

    def test_bad_signature_raises(self):
        with pytest.raises(exceptions.WebhookSignatureVerificationError):
            AsyncTriggers(client=_client()).verify_webhook(
                id="msg_1",
                payload="{}",
                secret=SECRET,
                signature="v1,AAAA",
                timestamp=str(int(time.time())),
            )

    def test_shared_implementation(self):
        assert AsyncTriggers._parse_webhook_payload is Triggers._parse_webhook_payload
