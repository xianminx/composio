"""Tests for the async toolkits resource."""

import asyncio
from unittest.mock import AsyncMock, Mock

import pytest
from composio_client import omit

from composio import exceptions
from composio.core.models._async.connected_accounts import (
    AsyncConnectedAccounts,
    AsyncConnectionRequest,
)
from composio.core.models._async.toolkits import AsyncToolkits


@pytest.fixture
def client() -> Mock:
    client = Mock()
    for name in (
        "list",
        "retrieve",
        "retrieve_multi",
        "retrieve_changelog",
        "recommend_scopes",
        "retrieve_scopes_grant_context",
        "retrieve_categories",
    ):
        setattr(client.toolkits, name, AsyncMock())
    client.auth_configs.list = AsyncMock()
    client.auth_configs.create = AsyncMock()
    client.connected_accounts.list = AsyncMock(return_value=Mock(items=[]))
    client.connected_accounts.with_raw_response.create = AsyncMock()
    return client


@pytest.fixture
def toolkits(client: Mock) -> AsyncToolkits:
    return AsyncToolkits(client)


def _auth_detail(mode: str, group: str, required, optional):
    detail = Mock(mode=mode)
    setattr(detail.fields, group, Mock(required=required, optional=optional))
    return detail


def test_mounts_async_connected_accounts(toolkits, client):
    assert isinstance(toolkits.connected_accounts, AsyncConnectedAccounts)
    assert toolkits.connected_accounts._client is client


def test_list_omits_absent_filters(toolkits, client):
    client.toolkits.list.return_value = "page"

    assert asyncio.run(toolkits.list(limit=5)) == "page"
    client.toolkits.list.assert_awaited_once_with(
        category=omit, cursor=omit, limit=5, managed_by=omit, sort_by=omit
    )


def test_get_by_slug_retrieves_one(toolkits, client):
    client.toolkits.retrieve.return_value = "github"

    assert asyncio.run(toolkits.get("github")) == "github"
    client.toolkits.retrieve.assert_awaited_once_with(slug="github")


def test_get_without_slug_returns_list_items(toolkits, client):
    client.toolkits.list.return_value = Mock(items=["a", "b"])

    assert asyncio.run(toolkits.get(query={"category": "dev"})) == ["a", "b"]
    client.toolkits.list.assert_awaited_once_with(category="dev")


def test_get_many_forwards_filters(toolkits, client):
    client.toolkits.retrieve_multi.return_value = Mock(items=["x"])

    assert asyncio.run(toolkits.get_many(("github", "slack"), limit=2)) == ["x"]
    client.toolkits.retrieve_multi.assert_awaited_once_with(
        toolkits=["github", "slack"],
        category=omit,
        managed_by=omit,
        sort_by=omit,
        limit=2,
        cursor=omit,
    )


def test_changelog_scopes_and_categories_delegate(toolkits, client):
    client.toolkits.retrieve_changelog.return_value = "changelog"
    client.toolkits.recommend_scopes.return_value = "scopes"
    client.toolkits.retrieve_scopes_grant_context.return_value = "contexts"
    client.toolkits.retrieve_categories.return_value = Mock(items=["crm"])

    async def run():
        return (
            await toolkits.changelog(),
            await toolkits.recommend_scopes("gmail", tools=["GMAIL_SEND_EMAIL"]),
            await toolkits.list_grant_contexts("gmail"),
            await toolkits.list_categories(),
        )

    assert asyncio.run(run()) == ("changelog", "scopes", "contexts", ["crm"])
    client.toolkits.recommend_scopes.assert_awaited_once_with(
        "gmail", tools=["GMAIL_SEND_EMAIL"]
    )
    client.toolkits.retrieve_scopes_grant_context.assert_awaited_once_with("gmail")


def _initiate_response(client: Mock):
    body = Mock()
    body.id = "conn-1"
    body.connection_data.val.status = "INITIATED"
    body.connection_data.val.redirect_url = "https://redirect"
    raw = Mock(headers={})
    raw.parse = AsyncMock(return_value=body)
    client.connected_accounts.with_raw_response.create.return_value = raw


def test_authorize_uses_the_newest_auth_config(toolkits, client):
    client.auth_configs.list.return_value = Mock(
        items=[
            Mock(id="ac_old", created_at="2025-01-01"),
            Mock(id="ac_new", created_at="2026-01-01"),
        ]
    )
    _initiate_response(client)

    request = asyncio.run(toolkits.authorize(user_id="user-1", toolkit="github"))

    assert isinstance(request, AsyncConnectionRequest)
    assert request.redirect_url == "https://redirect"
    client.auth_configs.list.assert_awaited_once_with(toolkit_slug="github")
    client.auth_configs.create.assert_not_awaited()
    client.connected_accounts.with_raw_response.create.assert_awaited_once_with(
        auth_config={"id": "ac_new"}, connection={"user_id": "user-1"}
    )


def test_authorize_creates_a_managed_auth_config_when_none_exists(toolkits, client):
    client.auth_configs.list.return_value = Mock(items=[])
    client.auth_configs.create.return_value = Mock(auth_config=Mock(id="ac_created"))
    _initiate_response(client)

    asyncio.run(toolkits.authorize(user_id="user-1", toolkit="github"))

    client.auth_configs.create.assert_awaited_once_with(
        toolkit={"slug": "github"},
        auth_config={
            "type": "use_composio_managed_auth",
            "tool_access_config": {"tools_for_connected_account_creation": []},
        },
    )
    assert client.connected_accounts.with_raw_response.create.await_args.kwargs[
        "auth_config"
    ] == {"id": "ac_created"}


@pytest.mark.parametrize(
    "method,group",
    [
        ("get_connected_account_initiation_fields", "connected_account_initiation"),
        ("get_auth_config_creation_fields", "auth_config_creation"),
    ],
)
def test_auth_fields_for_a_scheme(toolkits, client, method, group):
    client.toolkits.retrieve.return_value = Mock(
        auth_config_details=[
            _auth_detail("API_KEY", group, ["other"], []),
            _auth_detail("OAUTH2", group, ["client_id"], ["scopes"]),
        ]
    )

    async def run():
        fetch = getattr(toolkits, method)
        return (
            await fetch("github", "OAUTH2"),
            await fetch("github", "OAUTH2", required_only=True),
        )

    assert asyncio.run(run()) == (["client_id", "scopes"], ["client_id"])
    client.toolkits.retrieve.assert_awaited_with(slug="github")


def test_auth_fields_raise_for_an_unknown_scheme(toolkits, client):
    client.toolkits.retrieve.return_value = Mock(auth_config_details=None)

    with pytest.raises(exceptions.InvalidParams):
        asyncio.run(toolkits.get_auth_config_creation_fields("github", "OAUTH2"))
