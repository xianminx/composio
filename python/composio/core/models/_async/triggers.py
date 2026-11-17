from __future__ import annotations

import asyncio
import functools
import inspect
import time
import traceback
import typing as t

import typing_extensions as te
from composio_client import APIStatusError, omit
from composio_client.types import TriggersTypeRetrieveResponse

from composio import exceptions
from composio.client import AsyncHttpClient, HttpClient
from composio.client.types import trigger_instance_upsert_response
from composio.core.models._async.internal import AsyncInternal
from composio.core.models.base import AsyncResource
from composio.core.models.triggers import (
    TriggerEvent,
    TriggerEventFilters,
    TriggerSubscription,
    _SubcriptionBuilder,
    _WebhookVerification,
)
from composio.core.models.webhooks import WebhookSubscription, WebhookVersion
from composio.core.types import ToolkitVersionParam
from composio.exceptions import ComposioSDKTimeoutError
from composio.utils.async_bridge import run_io
from composio.utils.logging import WithLogger
from composio.utils.pydantic import none_to_omit

AsyncTriggerCallback = t.Callable[[TriggerEvent], t.Union[None, t.Awaitable[None]]]
"""A trigger handler: a plain function or a coroutine function."""

AsyncSubscriptionErrorCallback = t.Callable[
    [t.Dict[str, t.Any]], t.Union[None, t.Awaitable[None]]
]

_TCallback = t.TypeVar("_TCallback", bound=AsyncTriggerCallback)

_WAIT_POLL_SECONDS = 1.0
_CONNECT_POLL_SECONDS = 0.1


def _async_webhook_subscriptions(client: AsyncHttpClient) -> t.Any:
    # Imported here: the webhooks module is a sibling that imports nothing
    # from this one, but keeping it lazy lets either be loaded alone.
    from composio.core.models._async.webhooks import AsyncWebhookSubscriptions

    return AsyncWebhookSubscriptions(client=client)


class AsyncTriggerSubscription(TriggerSubscription):
    """Async counterpart of :class:`~composio.core.models.triggers.TriggerSubscription`.

    The realtime transport is the same pysher websocket thread. Frames are
    parsed there and handed to the event loop ``subscribe()`` was awaited on,
    so handlers always run on that loop: a coroutine function as a task, a
    plain function inline (keep it non-blocking). A handler that raises is
    logged and does not end the subscription.
    """

    def __init__(
        self,
        client: AsyncHttpClient,
        loop: t.Optional[asyncio.AbstractEventLoop] = None,
    ) -> None:
        # The subscription only stores the client; it never issues requests.
        super().__init__(client=t.cast(HttpClient, client))
        self._loop = loop if loop is not None else asyncio.get_running_loop()
        self._tasks: t.Set[asyncio.Future[t.Any]] = set()
        self._stopped = asyncio.Event()

    def handle(  # type: ignore[override]
        self, **filters: te.Unpack[TriggerEventFilters]
    ) -> t.Callable[[_TCallback], _TCallback]:
        """See :meth:`TriggerSubscription.handle`; coroutine functions are accepted."""
        return t.cast(t.Callable[[_TCallback], _TCallback], super().handle(**filters))

    def _call_soon(self, fn: t.Callable[..., None], *args: t.Any) -> None:
        """Hand ``fn`` to the event loop from the websocket thread."""
        try:
            self._loop.call_soon_threadsafe(fn, *args)
        except RuntimeError:
            self.logger.warning(
                "Dropping a trigger frame: the event loop the subscription "
                "was created on is closed"
            )

    def _run_callback(
        self,
        callback: t.Callable[[t.Any], t.Any],
        argument: t.Any,
        describe_failure: t.Callable[[], str],
    ) -> None:
        """Run a handler on the loop, containing whatever it raises."""
        try:
            result = callback(argument)
        except Exception:
            self.logger.error(f"{describe_failure()}:\n {traceback.format_exc()}")
            return
        if not inspect.isawaitable(result):
            return

        async def _contain() -> None:
            try:
                await result
            except Exception:
                self.logger.error(f"{describe_failure()}:\n {traceback.format_exc()}")

        task = self._loop.create_task(_contain())
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    def _handle_event(self, event: str) -> None:
        # Websocket thread. `_handle_chunked_events` also lands here once a
        # chunked frame is complete.
        self._call_soon(self._dispatch_event, event)

    def _dispatch_event(self, event: str) -> None:
        data = self._parse_event(event)
        if data is None:
            return
        for callback, filters in list(self._callbacks):
            name = getattr(callback, "__name__", repr(callback))
            if not self._filters_match(data, filters, name):
                continue
            self._run_callback(
                callback,
                data,
                lambda name=name: (  # type: ignore[misc]
                    f"Error executing `{name}` for "
                    f"event `{data['metadata']['trigger_slug']}` with error"
                ),
            )

    def _notify_subscription_error(self, payload: t.Dict[str, t.Any]) -> None:
        callback = self._on_subscription_error
        if callback is None:
            return
        self._call_soon(
            self._run_callback,
            callback,
            payload,
            lambda: "Error in subscription error callback",
        )

    async def wait_forever(self) -> None:  # type: ignore[override]
        """See :meth:`TriggerSubscription.wait_forever`. Cancellable."""
        while self.is_alive() and not self.has_errored():
            try:
                await asyncio.wait_for(self._stopped.wait(), _WAIT_POLL_SECONDS)
            except asyncio.TimeoutError:
                continue

    async def stop(self) -> None:  # type: ignore[override]
        """Stop the listener and wait for the websocket to close.

        Safe to await from a handler: handlers run on the event loop, not on
        the websocket thread the disconnect joins.
        """
        self._alive = False
        self._stopped.set()
        await run_io(self._connection.disconnect)

    async def restart(self) -> None:  # type: ignore[override]
        """See :meth:`TriggerSubscription.restart`."""
        await run_io(self._connection.reconnect, reconnect_interval=0)


class _AsyncSubscriptionBuilder(_SubcriptionBuilder):
    """``_SubcriptionBuilder`` whose credentials request and wait are awaited."""

    subscription: AsyncTriggerSubscription  # type: ignore[assignment]

    def __init__(self, client: AsyncHttpClient) -> None:
        WithLogger.__init__(self)
        # The inherited helpers only read `base_url` and the credentials.
        self._client = t.cast(HttpClient, client)
        self.base_url = client.base_url
        self.async_internal = AsyncInternal(client=client)
        self.subscription = AsyncTriggerSubscription(client=client)

    async def connect(  # type: ignore[override]
        self,
        timeout: float = 15.0,
        on_subscription_error: t.Optional[AsyncSubscriptionErrorCallback] = None,
    ) -> AsyncTriggerSubscription:
        self.logger.debug("Creating trigger subscription")
        project_info = await self.async_internal.get_sdk_realtime_credentials()
        pusher = self._start(project_info, t.cast(t.Any, on_subscription_error))

        # See `_SubcriptionBuilder.connect` for why every exit that is not a
        # success disconnects; cancellation is one more such exit here.
        deadline = time.monotonic() + timeout
        try:
            while time.monotonic() < deadline:
                self.subscription._raise_on_connection_error()
                if not self.subscription.is_alive():
                    await asyncio.sleep(_CONNECT_POLL_SECONDS)
                    continue

                self.subscription._pusher = pusher
                return self.subscription
            raise ComposioSDKTimeoutError(
                "Timed out while waiting for trigger listener to be established"
            )
        except BaseException:
            await asyncio.shield(run_io(pusher.disconnect))
            raise


class AsyncTriggers(AsyncResource, _WebhookVerification):
    """Async counterpart of :class:`~composio.core.models.triggers.Triggers`.

    ``verify_webhook`` and ``parse`` do no IO and stay synchronous.
    """

    enable: t.Callable[..., t.Awaitable[t.Any]]
    """Enables a trigger given its id"""

    disable: t.Callable[..., t.Awaitable[t.Any]]
    """Disables a trigger given its id"""

    verify_webhook = _WebhookVerification.verify_webhook
    parse = _WebhookVerification.parse

    def __init__(
        self,
        client: AsyncHttpClient,
        toolkit_versions: t.Optional[ToolkitVersionParam] = None,
    ):
        self._client = client
        self._toolkit_versions = toolkit_versions
        self.list_enum = self._client.triggers_types.retrieve_enum
        self.delete = self._client.trigger_instances.manage.delete
        self.enable = functools.partial(
            self._client.trigger_instances.manage.update,
            status="enable",
        )
        self.disable = functools.partial(
            self._client.trigger_instances.manage.update,
            status="disable",
        )

    async def set_webhook_subscription(
        self,
        *,
        webhook_url: str,
        enabled_events: t.Optional[t.Sequence[str]] = None,
        version: t.Union[WebhookVersion, str] = WebhookVersion.V3,
    ) -> WebhookSubscription:
        """See :meth:`Triggers.set_webhook_subscription`."""
        return await _async_webhook_subscriptions(self._client).set(
            webhook_url=webhook_url,
            enabled_events=enabled_events,
            version=version,
        )

    async def get_type(self, slug: str) -> TriggersTypeRetrieveResponse:
        """See :meth:`Triggers.get_type`."""
        return await self._client.triggers_types.retrieve(
            slug=slug, toolkit_versions=none_to_omit(self._toolkit_versions)
        )

    async def list_active(
        self,
        trigger_ids: t.Optional[list[str]] = None,
        trigger_names: t.Optional[list[str]] = None,
        auth_config_ids: t.Optional[list[str]] = None,
        connected_account_ids: t.Optional[list[str]] = None,
        show_disabled: t.Optional[bool] = None,
        limit: t.Optional[int] = None,
        cursor: t.Optional[str] = None,
    ):
        """See :meth:`Triggers.list_active`."""
        return await self._client.trigger_instances.list_active(
            query_trigger_ids_1=trigger_ids,
            query_trigger_names_1=trigger_names,
            query_auth_config_ids_1=auth_config_ids,
            query_connected_account_ids_1=connected_account_ids,
            query_show_disabled_1=show_disabled,
            limit=limit if limit is not None else omit,
            cursor=cursor if cursor is not None else omit,
        )

    async def list(
        self,
        *,
        cursor: t.Optional[str] = None,
        limit: t.Optional[int] = None,
        toolkit_slugs: t.Optional[list[str]] = None,
    ):
        """See :meth:`Triggers.list`."""
        return await self._client.triggers_types.list(
            cursor=none_to_omit(cursor),
            limit=none_to_omit(limit),
            toolkit_slugs=none_to_omit(toolkit_slugs),
            toolkit_versions=none_to_omit(self._toolkit_versions),
        )

    @t.overload
    async def create(
        self,
        slug: str,
        *,
        connected_account_id: str,
        trigger_config: t.Optional[t.Dict[str, t.Any]] = None,
        type: t.Optional[t.Literal["webhook", "poll"]] = None,
        polling_interval: t.Optional[int] = None,
    ) -> trigger_instance_upsert_response.TriggerInstanceUpsertResponse: ...

    @t.overload
    async def create(
        self,
        slug: str,
        *,
        user_id: str,
        trigger_config: t.Optional[t.Dict[str, t.Any]] = None,
        type: t.Optional[t.Literal["webhook", "poll"]] = None,
        polling_interval: t.Optional[int] = None,
    ) -> trigger_instance_upsert_response.TriggerInstanceUpsertResponse: ...

    async def create(
        self,
        slug: str,
        *,
        user_id: t.Optional[str] = None,
        connected_account_id: t.Optional[str] = None,
        trigger_config: t.Optional[t.Dict[str, t.Any]] = None,
        type: t.Optional[t.Literal["webhook", "poll"]] = None,
        polling_interval: t.Optional[int] = None,
    ) -> trigger_instance_upsert_response.TriggerInstanceUpsertResponse:
        """See :meth:`Triggers.create`."""
        if isinstance(user_id, str) and not user_id.strip():
            user_id = None
        if isinstance(connected_account_id, str) and not connected_account_id.strip():
            connected_account_id = None

        if user_id is None and connected_account_id is None:
            raise exceptions.InvalidParams(
                "please provide valid `connected_account_id` or `user_id`"
            )

        try:
            await self.get_type(slug=slug)
        except APIStatusError as error:
            if error.status_code in (400, 404):
                raise exceptions.TriggerTypeNotFound(
                    f"Trigger type {slug} not found"
                ) from error
            raise

        return await self._client.trigger_instances.upsert(
            slug=slug,
            connected_account_id=none_to_omit(connected_account_id),
            toolkit_versions=self._toolkit_versions,
            body_trigger_config_1=none_to_omit(trigger_config),
            type=none_to_omit(type),
            polling_interval=none_to_omit(polling_interval),
            user_id=none_to_omit(user_id),
        )

    async def subscribe(
        self,
        timeout: float = 15.0,
        on_subscription_error: t.Optional[AsyncSubscriptionErrorCallback] = None,
    ) -> AsyncTriggerSubscription:
        """See :meth:`Triggers.subscribe`.

        Handlers registered on the returned subscription, and
        ``on_subscription_error``, may be plain or coroutine functions; both
        run on the event loop this was awaited on.
        """
        return await _AsyncSubscriptionBuilder(client=self._client).connect(
            timeout=timeout,
            on_subscription_error=on_subscription_error,
        )


__all__ = [
    "AsyncSubscriptionErrorCallback",
    "AsyncTriggerCallback",
    "AsyncTriggerSubscription",
    "AsyncTriggers",
]
