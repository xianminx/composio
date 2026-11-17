"""Tests for the async ``composio.webhooks`` namespace."""

import asyncio
from unittest.mock import AsyncMock, Mock

import pytest
from composio_client import omit

from composio import exceptions
from composio.core.models._async.webhooks import (
    AsyncWebhookEndpoints,
    AsyncWebhooks,
    AsyncWebhookSubscriptions,
)
from composio.core.models.webhooks import WebhookVersion

WEBHOOK_URL = "https://example.com/webhooks/composio"


@pytest.fixture
def client() -> Mock:
    client = Mock()
    client.webhook_subscriptions = Mock()
    for name in (
        "list",
        "retrieve",
        "create",
        "update",
        "delete",
        "rotate_secret",
        "list_event_types",
    ):
        setattr(client.webhook_subscriptions, name, AsyncMock())
    client.webhook_endpoints = Mock()
    for name in ("list", "retrieve", "create", "replace", "update"):
        setattr(client.webhook_endpoints, name, AsyncMock())
    return client


@pytest.fixture
def subscriptions(client: Mock) -> AsyncWebhookSubscriptions:
    return AsyncWebhookSubscriptions(client=client)


@pytest.fixture
def endpoints(client: Mock) -> AsyncWebhookEndpoints:
    return AsyncWebhookEndpoints(client=client)


def _raw(**overrides):
    return {
        "id": "sub_123",
        "webhook_url": WEBHOOK_URL,
        "version": "V3",
        "enabled_events": ["composio.trigger.message"],
        "secret": "whsec_abc",
        **overrides,
    }


def test_mounts_async_sub_namespaces(client):
    webhooks = AsyncWebhooks(client=client)

    assert isinstance(webhooks.subscriptions, AsyncWebhookSubscriptions)
    assert isinstance(webhooks.endpoints, AsyncWebhookEndpoints)
    assert webhooks.subscriptions._client is client
    assert webhooks.endpoints._client is client


class TestAsyncWebhookSubscriptions:
    def test_list_omits_absent_pagination(self, subscriptions, client):
        client.webhook_subscriptions.list.return_value = "listed"

        assert asyncio.run(subscriptions.list()) == "listed"
        client.webhook_subscriptions.list.assert_awaited_once_with(
            limit=omit, cursor=omit
        )

    def test_get_delete_rotate_and_event_types_delegate(self, subscriptions, client):
        async def run():
            await subscriptions.get("sub_123")
            await subscriptions.delete("sub_123")
            await subscriptions.rotate_secret("sub_123")
            await subscriptions.list_event_types()

        asyncio.run(run())

        client.webhook_subscriptions.retrieve.assert_awaited_once_with("sub_123")
        client.webhook_subscriptions.delete.assert_awaited_once_with("sub_123")
        client.webhook_subscriptions.rotate_secret.assert_awaited_once_with("sub_123")
        client.webhook_subscriptions.list_event_types.assert_awaited_once_with()

    def test_update_normalizes_events_and_version(self, subscriptions, client):
        asyncio.run(
            subscriptions.update(
                "sub_123",
                enabled_events=("composio.trigger.message",),
                version=WebhookVersion.V2,
            )
        )

        client.webhook_subscriptions.update.assert_awaited_once_with(
            "sub_123",
            webhook_url=omit,
            enabled_events=["composio.trigger.message"],
            version="V2",
        )

    def test_set_creates_when_none_exists(self, subscriptions, client):
        client.webhook_subscriptions.list.return_value = Mock(items=[])
        client.webhook_subscriptions.create.return_value = _raw()

        result = asyncio.run(subscriptions.set(webhook_url=WEBHOOK_URL))

        client.webhook_subscriptions.list.assert_awaited_once_with(limit=1)
        client.webhook_subscriptions.create.assert_awaited_once_with(
            webhook_url=WEBHOOK_URL,
            enabled_events=["composio.trigger.message"],
            version="V3",
        )
        client.webhook_subscriptions.update.assert_not_awaited()
        assert result["id"] == "sub_123"
        assert result["secret"] == "whsec_abc"

    def test_set_updates_the_first_existing_subscription(self, subscriptions, client):
        client.webhook_subscriptions.list.return_value = Mock(
            items=[Mock(id="sub_existing")]
        )
        client.webhook_subscriptions.update.return_value = _raw(id="sub_existing")

        result = asyncio.run(
            subscriptions.set(
                webhook_url=WEBHOOK_URL,
                enabled_events=["composio.connected_account.expired"],
                version="V2",
            )
        )

        client.webhook_subscriptions.update.assert_awaited_once_with(
            "sub_existing",
            webhook_url=WEBHOOK_URL,
            enabled_events=["composio.connected_account.expired"],
            version="V2",
        )
        client.webhook_subscriptions.create.assert_not_awaited()
        assert result["id"] == "sub_existing"

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"webhook_url": ""},
            {"webhook_url": WEBHOOK_URL, "enabled_events": []},
        ],
    )
    def test_set_validates_before_any_request(self, subscriptions, client, kwargs):
        with pytest.raises(exceptions.ValidationError):
            asyncio.run(subscriptions.set(**kwargs))

        client.webhook_subscriptions.list.assert_not_awaited()


class TestAsyncWebhookEndpoints:
    def test_list_get_and_create(self, endpoints, client):
        async def run():
            await endpoints.list()
            await endpoints.list(toolkit_slug="github")
            await endpoints.get("we_123")
            await endpoints.create(toolkit_slug="github", client_id="Iv1.abc")

        asyncio.run(run())

        assert client.webhook_endpoints.list.await_args_list[0].kwargs == {
            "toolkit_slug": omit
        }
        assert client.webhook_endpoints.list.await_args_list[1].kwargs == {
            "toolkit_slug": "github"
        }
        client.webhook_endpoints.retrieve.assert_awaited_once_with("we_123")
        client.webhook_endpoints.create.assert_awaited_once_with(
            toolkit_slug="github", client_id="Iv1.abc"
        )

    def test_replace_and_update_forward_data(self, endpoints, client):
        async def run():
            await endpoints.replace("we_123", data={"secret": "a"})
            await endpoints.update("we_123", data={"secret": "b"})

        asyncio.run(run())

        client.webhook_endpoints.replace.assert_awaited_once_with(
            "we_123", data={"secret": "a"}
        )
        client.webhook_endpoints.update.assert_awaited_once_with(
            "we_123", data={"secret": "b"}
        )
