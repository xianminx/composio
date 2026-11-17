from __future__ import annotations

import typing as t

from composio.client import AsyncHttpClient
from composio.client.types import (
    webhook_endpoint_create_response,
    webhook_endpoint_list_response,
    webhook_endpoint_replace_response,
    webhook_endpoint_retrieve_response,
    webhook_endpoint_update_response,
    webhook_subscription_delete_response,
    webhook_subscription_list_event_types_response,
    webhook_subscription_list_response,
    webhook_subscription_retrieve_response,
    webhook_subscription_rotate_secret_response,
    webhook_subscription_update_response,
)
from composio.core.models.base import AsyncResource
from composio.core.models.webhooks import (
    WebhookSubscription,
    WebhookVersion,
    _subscription_events,
    _webhook_version,
    first_webhook_subscription_id,
    normalize_webhook_subscription,
)
from composio.utils.pydantic import none_to_omit


class AsyncWebhookSubscriptions(AsyncResource):
    """Async counterpart of :class:`~composio.core.models.webhooks.WebhookSubscriptions`."""

    async def list(
        self,
        *,
        limit: t.Optional[float] = None,
        cursor: t.Optional[str] = None,
    ) -> webhook_subscription_list_response.WebhookSubscriptionListResponse:
        """See :meth:`WebhookSubscriptions.list`."""
        return await self._client.webhook_subscriptions.list(
            limit=none_to_omit(limit),
            cursor=none_to_omit(cursor),
        )

    async def get(
        self, id: str
    ) -> webhook_subscription_retrieve_response.WebhookSubscriptionRetrieveResponse:
        """See :meth:`WebhookSubscriptions.get`."""
        return await self._client.webhook_subscriptions.retrieve(id)

    async def set(
        self,
        *,
        webhook_url: str,
        enabled_events: t.Optional[t.Sequence[str]] = None,
        version: t.Union[WebhookVersion, str] = WebhookVersion.V3,
    ) -> WebhookSubscription:
        """See :meth:`WebhookSubscriptions.set`."""
        events = _subscription_events(webhook_url, enabled_events)
        version_value = _webhook_version(version)

        existing = await self._client.webhook_subscriptions.list(limit=1)
        subscription_id = first_webhook_subscription_id(existing)

        if subscription_id:
            return normalize_webhook_subscription(
                await self._client.webhook_subscriptions.update(
                    subscription_id,
                    webhook_url=webhook_url,
                    enabled_events=events,
                    version=version_value,
                )
            )

        return normalize_webhook_subscription(
            await self._client.webhook_subscriptions.create(
                webhook_url=webhook_url,
                enabled_events=events,
                version=version_value,
            )
        )

    async def update(
        self,
        id: str,
        *,
        webhook_url: t.Optional[str] = None,
        enabled_events: t.Optional[t.Sequence[str]] = None,
        version: t.Union[WebhookVersion, str, None] = None,
    ) -> webhook_subscription_update_response.WebhookSubscriptionUpdateResponse:
        """See :meth:`WebhookSubscriptions.update`."""
        version_value = None if version is None else _webhook_version(version)
        return await self._client.webhook_subscriptions.update(
            id,
            webhook_url=none_to_omit(webhook_url),
            enabled_events=none_to_omit(
                list(enabled_events) if enabled_events is not None else None
            ),
            version=none_to_omit(version_value),
        )

    async def delete(
        self, id: str
    ) -> webhook_subscription_delete_response.WebhookSubscriptionDeleteResponse:
        """See :meth:`WebhookSubscriptions.delete`."""
        return await self._client.webhook_subscriptions.delete(id)

    async def rotate_secret(
        self, id: str
    ) -> webhook_subscription_rotate_secret_response.WebhookSubscriptionRotateSecretResponse:
        """See :meth:`WebhookSubscriptions.rotate_secret`."""
        return await self._client.webhook_subscriptions.rotate_secret(id)

    async def list_event_types(
        self,
    ) -> webhook_subscription_list_event_types_response.WebhookSubscriptionListEventTypesResponse:
        """See :meth:`WebhookSubscriptions.list_event_types`."""
        return await self._client.webhook_subscriptions.list_event_types()


class AsyncWebhookEndpoints(AsyncResource):
    """Async counterpart of :class:`~composio.core.models.webhooks.WebhookEndpoints`."""

    async def list(
        self,
        *,
        toolkit_slug: t.Optional[str] = None,
    ) -> webhook_endpoint_list_response.WebhookEndpointListResponse:
        """See :meth:`WebhookEndpoints.list`."""
        return await self._client.webhook_endpoints.list(
            toolkit_slug=none_to_omit(toolkit_slug),
        )

    async def get(
        self, nano_id: str
    ) -> webhook_endpoint_retrieve_response.WebhookEndpointRetrieveResponse:
        """See :meth:`WebhookEndpoints.get`."""
        return await self._client.webhook_endpoints.retrieve(nano_id)

    async def create(
        self,
        *,
        toolkit_slug: str,
        client_id: str,
    ) -> webhook_endpoint_create_response.WebhookEndpointCreateResponse:
        """See :meth:`WebhookEndpoints.create`."""
        return await self._client.webhook_endpoints.create(
            toolkit_slug=toolkit_slug,
            client_id=client_id,
        )

    async def replace(
        self,
        nano_id: str,
        *,
        data: t.Dict[str, t.Any],
    ) -> webhook_endpoint_replace_response.WebhookEndpointReplaceResponse:
        """See :meth:`WebhookEndpoints.replace`."""
        return await self._client.webhook_endpoints.replace(nano_id, data=data)

    async def update(
        self,
        nano_id: str,
        *,
        data: t.Dict[str, t.Any],
    ) -> webhook_endpoint_update_response.WebhookEndpointUpdateResponse:
        """See :meth:`WebhookEndpoints.update`."""
        return await self._client.webhook_endpoints.update(nano_id, data=data)


class AsyncWebhooks(AsyncResource):
    """Async webhooks namespace, exposed as ``composio.webhooks`` on ``AsyncComposio``."""

    subscriptions: AsyncWebhookSubscriptions
    endpoints: AsyncWebhookEndpoints

    def __init__(self, client: AsyncHttpClient):
        super().__init__(client)
        self.subscriptions = AsyncWebhookSubscriptions(client)
        self.endpoints = AsyncWebhookEndpoints(client)
