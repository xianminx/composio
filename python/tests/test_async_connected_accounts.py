"""Tests for the async connected accounts resource."""

import asyncio
import time
import warnings
from unittest.mock import AsyncMock, Mock

import httpx
import pytest
from composio_client import (
    BadRequestError,
    ConflictError,
    ProjectAuth,
    RateLimitError,
    omit,
)

import composio.core.models.connected_accounts as ca_module
from composio import exceptions
from composio.client import AsyncHttpClient
from composio.core.models._async.connected_accounts import (
    AsyncConnectedAccounts,
    AsyncConnectionRequest,
)


def _status_error(error_type, status: int, message: str):
    request = httpx.Request("POST", "https://backend.invalid")
    response = httpx.Response(status, request=request)
    return error_type(message, response=response, body=None)


def _rate_limited(retry_after=None):
    headers = {} if retry_after is None else {"retry-after": retry_after}
    request = httpx.Request("GET", "https://backend.invalid")
    response = httpx.Response(429, headers=headers, request=request)
    return RateLimitError("slow down", response=response, body=None)


def _client() -> Mock:
    client = Mock()
    client.connected_accounts.retrieve = AsyncMock()
    client.connected_accounts.list = AsyncMock(return_value=Mock(items=[]))
    client.connected_accounts.delete = AsyncMock()
    client.connected_accounts.update_status = AsyncMock()
    client.connected_accounts.refresh = AsyncMock()
    client.connected_accounts.revoke = AsyncMock()
    client.connected_accounts.complete_auth = AsyncMock()
    client.connected_accounts.create = AsyncMock()
    client.connected_accounts.patch = AsyncMock()
    client.connected_accounts.with_raw_response.create = AsyncMock()
    client.link.create = AsyncMock()
    return client


def _set_initiate_response(client: Mock, body, headers=None):
    raw = Mock()
    raw.parse = AsyncMock(return_value=body)
    raw.headers = headers or {}
    client.connected_accounts.with_raw_response.create.return_value = raw


def _initiate_body():
    body = Mock()
    body.id = "conn-1"
    body.connection_data.val.status = "INITIATED"
    body.connection_data.val.redirect_url = "https://redirect"
    return body


@pytest.fixture
def clock(monkeypatch):
    """A fake clock that only ``asyncio.sleep`` moves."""
    state = {"now": 0.0}
    monkeypatch.setattr(time, "time", lambda: state["now"])

    async def sleep(seconds):
        state["now"] += seconds

    monkeypatch.setattr(asyncio, "sleep", sleep)
    return state


def _request(client) -> AsyncConnectionRequest:
    return AsyncConnectionRequest(
        id="conn-1", status="PENDING", redirect_url=None, client=client
    )


class TestAsyncConnectionRequestPolling:
    def _poll_times(self, clock, timeout):
        times = []

        async def retrieve(nanoid):
            times.append(clock["now"])
            return Mock(status="PENDING")

        client = _client()
        client.connected_accounts.retrieve.side_effect = retrieve
        with pytest.raises(exceptions.ComposioSDKTimeoutError):
            asyncio.run(_request(client).wait_for_connection(timeout=timeout))
        return times

    def test_backs_off_from_1s_to_a_5s_cap(self, clock):
        times = self._poll_times(clock, 30.0)
        assert times[:7] == [0.0, 1.0, 2.5, 4.75, 8.125, 13.125, 18.125]

    def test_sends_about_16_requests_in_the_default_wait(self, clock):
        assert len(self._poll_times(clock, 60.0)) == 16

    def test_polls_one_last_time_at_the_deadline(self, clock):
        assert self._poll_times(clock, 3.0) == [0.0, 1.0, 2.5, 3.0]

    def test_returns_when_active_and_tracks_status(self, clock):
        active = Mock(status="ACTIVE")
        client = _client()
        client.connected_accounts.retrieve.side_effect = [
            Mock(status="INITIATED"),
            active,
        ]
        request = _request(client)

        assert asyncio.run(request.wait_for_connection(timeout=60.0)) is active
        assert request.status == "ACTIVE"
        client.connected_accounts.retrieve.assert_awaited_with(nanoid="conn-1")

    @pytest.mark.parametrize("status", ["FAILED", "EXPIRED", "REVOKED"])
    def test_fails_fast_on_terminal_status(self, clock, status):
        client = _client()
        client.connected_accounts.retrieve.return_value = Mock(status=status)

        with pytest.raises(exceptions.SDKError, match="terminal state"):
            asyncio.run(_request(client).wait_for_connection(timeout=60.0))

        assert client.connected_accounts.retrieve.await_count == 1
        assert clock["now"] == 0.0

    def test_does_not_treat_inactive_as_terminal(self, clock):
        active = Mock(status="ACTIVE")
        client = _client()
        client.connected_accounts.retrieve.side_effect = [
            Mock(status="INACTIVE"),
            active,
        ]

        assert asyncio.run(_request(client).wait_for_connection(timeout=60.0)) is active

    def test_waits_as_long_as_a_429_asks_then_keeps_polling(self, clock):
        active = Mock(status="ACTIVE")
        client = _client()
        client.connected_accounts.retrieve.side_effect = [_rate_limited("10"), active]

        assert asyncio.run(_request(client).wait_for_connection(timeout=60.0)) is active
        assert clock["now"] == 10.0

    @pytest.mark.parametrize("retry_after", ["5", "60"])
    def test_does_not_repoll_at_the_deadline_after_a_429(self, clock, retry_after):
        client = _client()
        client.connected_accounts.retrieve.side_effect = _rate_limited(retry_after)

        with pytest.raises(exceptions.ComposioSDKTimeoutError):
            asyncio.run(_request(client).wait_for_connection(timeout=5.0))

        assert clock["now"] == 5.0
        assert client.connected_accounts.retrieve.await_count == 1

    @pytest.mark.parametrize("retry_after", [None, "soon", "nan", "inf"])
    def test_keeps_its_usual_delay_without_a_usable_retry_after(
        self, clock, retry_after
    ):
        client = _client()
        client.connected_accounts.retrieve.side_effect = [
            _rate_limited(retry_after),
            Mock(status="ACTIVE"),
        ]

        asyncio.run(_request(client).wait_for_connection(timeout=60.0))

        assert clock["now"] == 1.0

    def test_a_nan_timeout_times_out_after_one_poll(self, clock):
        client = _client()
        client.connected_accounts.retrieve.return_value = Mock(status="PENDING")

        with pytest.raises(exceptions.ComposioSDKTimeoutError):
            asyncio.run(_request(client).wait_for_connection(timeout=float("nan")))

        assert clock["now"] == 0.0
        assert client.connected_accounts.retrieve.await_count == 1

    def test_waiting_by_id_survives_a_429_on_its_first_request(self, clock):
        active = Mock(status="ACTIVE")
        client = _client()
        client.connected_accounts.retrieve.side_effect = [_rate_limited("10"), active]

        result = asyncio.run(
            AsyncConnectedAccounts(client).wait_for_connection(
                id="conn-1", timeout=60.0
            )
        )

        assert result is active
        assert clock["now"] == 10.0

    def test_from_id_reads_the_current_status(self):
        client = _client()
        client.connected_accounts.retrieve.return_value = Mock(status="ACTIVE")

        request = asyncio.run(AsyncConnectionRequest.from_id("conn-9", client))

        assert (request.id, request.status, request.redirect_url) == (
            "conn-9",
            "ACTIVE",
            None,
        )
        client.connected_accounts.retrieve.assert_awaited_once_with(nanoid="conn-9")

    def test_polls_over_the_async_transport_without_real_sleeping(self, clock):
        requests = []

        def handler(request: httpx.Request) -> httpx.Response:
            requests.append(request)
            return httpx.Response(
                429, headers={"retry-after": "3"}, json={"error": {"message": "x"}}
            )

        async def run():
            client = AsyncHttpClient(
                provider="test",
                auth=ProjectAuth("test-api-key"),
                base_url="https://backend.invalid",
                max_retries=0,
                http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
            )
            await _request(client).wait_for_connection(timeout=5.0)

        with pytest.raises(exceptions.ComposioSDKTimeoutError):
            asyncio.run(run())

        assert [r.url.path for r in requests] == [
            "/api/v3.1/connected_accounts/conn-1",
            "/api/v3.1/connected_accounts/conn-1",
        ]
        assert clock["now"] == 5.0


class TestAsyncConnectedAccounts:
    def test_constructor_binds_the_async_client_methods(self):
        client = _client()
        accounts = AsyncConnectedAccounts(client)

        assert accounts.get is client.connected_accounts.retrieve
        assert accounts.list is client.connected_accounts.list
        assert accounts.delete is client.connected_accounts.delete
        assert accounts.update_status is client.connected_accounts.update_status

    def test_enable_and_disable_are_awaitable_partials(self):
        client = _client()
        accounts = AsyncConnectedAccounts(client)

        async def run():
            await accounts.enable("ca_1")
            await accounts.disable("ca_1")

        asyncio.run(run())

        assert client.connected_accounts.update_status.await_args_list[0].kwargs == {
            "enabled": True
        }
        assert client.connected_accounts.update_status.await_args_list[1].kwargs == {
            "enabled": False
        }

    def test_refresh_is_deprecated_and_drops_validate_credentials(self):
        client = _client()
        client.connected_accounts.refresh.return_value = "refreshed"
        accounts = AsyncConnectedAccounts(client)

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            result = asyncio.run(
                accounts.refresh("ca_1", validate_credentials=True, timeout=5)  # type: ignore[call-arg]
            )

        assert result == "refreshed"
        client.connected_accounts.refresh.assert_awaited_once_with("ca_1", timeout=5)
        messages = [
            str(w.message)
            for w in caught
            if issubclass(w.category, exceptions.ComposioDeprecationWarning)
        ]
        assert any("refresh() is deprecated" in m for m in messages)
        assert any("validate_credentials" in m for m in messages)

    def test_revoke_delegates(self):
        client = _client()
        client.connected_accounts.revoke.return_value = "revoked"

        assert asyncio.run(AsyncConnectedAccounts(client).revoke("ca_1")) == "revoked"
        client.connected_accounts.revoke.assert_awaited_once_with("ca_1")

    @pytest.mark.parametrize(
        "error_type,status,expected",
        [
            (
                BadRequestError,
                400,
                exceptions.ComposioConnectedAccountRevocationNotSupportedError,
            ),
            (ConflictError, 409, exceptions.ComposioConnectedAccountNotRevokableError),
        ],
    )
    def test_revoke_maps_api_errors(self, error_type, status, expected):
        client = _client()
        client.connected_accounts.revoke.side_effect = _status_error(
            error_type, status, "nope"
        )

        with pytest.raises(expected):
            asyncio.run(AsyncConnectedAccounts(client).revoke("ca_1"))

    def test_complete_auth_delegates(self):
        client = _client()
        asyncio.run(
            AsyncConnectedAccounts(client).complete_auth(user_id="u", session_uri="uri")
        )
        client.connected_accounts.complete_auth.assert_awaited_once_with(
            user_id="u", session_uri="uri"
        )

    def test_update_omits_absent_fields(self):
        client = _client()
        asyncio.run(AsyncConnectedAccounts(client).update("ca_1", alias="work"))
        client.connected_accounts.patch.assert_awaited_once_with(
            "ca_1", alias="work", connection=omit
        )

    def test_update_acl_serializes_only_given_fields(self):
        client = _client()
        asyncio.run(
            AsyncConnectedAccounts(client).update_acl(
                "ca_1", allow_all_users=True, not_allowed_user_ids=[]
            )
        )
        client.connected_accounts.patch.assert_awaited_once_with(
            "ca_1",
            experimental={
                "acl_config_for_shared": {
                    "allow_all_users": True,
                    "not_allowed_user_ids": [],
                }
            },
        )

    def test_update_acl_rejects_all_none_without_a_request(self):
        client = _client()
        with pytest.raises(exceptions.ValidationError):
            asyncio.run(AsyncConnectedAccounts(client).update_acl("ca_1"))
        client.connected_accounts.patch.assert_not_awaited()

    def test_update_acl_maps_acl_only_for_shared(self):
        client = _client()
        client.connected_accounts.patch.side_effect = _status_error(
            BadRequestError, 400, ca_module.ACL_ONLY_FOR_SHARED_ERROR_FRAGMENT
        )
        with pytest.raises(exceptions.ComposioAclOnlyForSharedError):
            asyncio.run(
                AsyncConnectedAccounts(client).update_acl("ca_1", allow_all_users=True)
            )

    def test_update_acl_rethrows_other_bad_requests(self):
        client = _client()
        client.connected_accounts.patch.side_effect = _status_error(
            BadRequestError, 400, "something else"
        )
        with pytest.raises(BadRequestError):
            asyncio.run(
                AsyncConnectedAccounts(client).update_acl("ca_1", allow_all_users=True)
            )

    def test_link_builds_payload_and_returns_a_request(self):
        client = _client()
        client.link.create.return_value = Mock(
            connected_account_id="ca_new", redirect_url="https://connect"
        )

        request = asyncio.run(
            AsyncConnectedAccounts(client).link("user-1", "auth-1", alias="work")
        )

        assert isinstance(request, AsyncConnectionRequest)
        assert (request.id, request.status, request.redirect_url) == (
            "ca_new",
            "INITIATED",
            "https://connect",
        )
        client.connected_accounts.list.assert_awaited_once_with(
            user_ids=["user-1"], auth_config_ids=["auth-1"], statuses=["ACTIVE"]
        )
        client.link.create.assert_awaited_once_with(
            auth_config_id="auth-1",
            user_id="user-1",
            callback_url=omit,
            alias="work",
            experimental=omit,
        )

    def test_link_raises_when_an_active_connection_exists(self):
        client = _client()
        client.connected_accounts.list.return_value = Mock(items=[Mock()])

        with pytest.raises(exceptions.ComposioMultipleConnectedAccountsError):
            asyncio.run(AsyncConnectedAccounts(client).link("user-1", "auth-1"))
        client.link.create.assert_not_awaited()

    def test_link_allows_multiple_when_asked(self):
        client = _client()
        client.connected_accounts.list.return_value = Mock(items=[Mock()])
        client.link.create.return_value = Mock(
            connected_account_id="ca_2", redirect_url=None
        )

        request = asyncio.run(
            AsyncConnectedAccounts(client).link("user-1", "auth-1", allow_multiple=True)
        )
        assert request.id == "ca_2"

    def test_link_maps_acl_only_for_shared(self):
        client = _client()
        client.link.create.side_effect = _status_error(
            BadRequestError, 400, ca_module.ACL_ONLY_FOR_SHARED_ERROR_FRAGMENT
        )
        with pytest.raises(exceptions.ComposioAclOnlyForSharedError):
            asyncio.run(AsyncConnectedAccounts(client).link("user-1", "auth-1"))


class TestAsyncInitiate:
    @pytest.fixture(autouse=True)
    def _reset_warning_flag(self, monkeypatch):
        monkeypatch.setattr(ca_module, "_legacy_initiate_warning_emitted", False)

    def test_creates_through_the_raw_response_and_returns_a_request(self):
        client = _client()
        _set_initiate_response(client, _initiate_body())

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            request = asyncio.run(
                AsyncConnectedAccounts(client).initiate(
                    "user-1", "auth-1", callback_url="https://cb", alias="work"
                )
            )

        assert isinstance(request, AsyncConnectionRequest)
        assert (request.id, request.status, request.redirect_url) == (
            "conn-1",
            "INITIATED",
            "https://redirect",
        )
        client.connected_accounts.with_raw_response.create.assert_awaited_once_with(
            auth_config={"id": "auth-1"},
            connection={
                "user_id": "user-1",
                "callback_url": "https://cb",
                "alias": "work",
            },
        )
        assert caught == []

    def test_warns_once_per_process_on_the_deprecation_header(self):
        client = _client()
        _set_initiate_response(
            client, _initiate_body(), headers={"Deprecation": "@1776988800"}
        )
        accounts = AsyncConnectedAccounts(client)

        async def run():
            await accounts.initiate("user-1", "auth-1")
            await accounts.initiate("user-1", "auth-1")

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            asyncio.run(run())

        deprecations = [
            w
            for w in caught
            if issubclass(w.category, exceptions.ComposioDeprecationWarning)
        ]
        assert len(deprecations) == 1
        assert "composio.connected_accounts.link()" in str(deprecations[0].message)

    def test_raises_when_an_active_connection_exists(self):
        client = _client()
        client.connected_accounts.list.return_value = Mock(items=[Mock()])

        with pytest.raises(exceptions.ComposioMultipleConnectedAccountsError):
            asyncio.run(AsyncConnectedAccounts(client).initiate("user-1", "auth-1"))
        client.connected_accounts.with_raw_response.create.assert_not_awaited()

    def test_maps_the_retired_legacy_endpoint(self):
        client = _client()
        client.connected_accounts.with_raw_response.create.side_effect = _status_error(
            BadRequestError,
            400,
            "This endpoint is no longer supported; use /api/v3/connected_accounts/link",
        )

        with pytest.raises(
            exceptions.ComposioLegacyConnectedAccountsEndpointRetiredError
        ):
            asyncio.run(AsyncConnectedAccounts(client).initiate("user-1", "auth-1"))

    def test_reads_the_deprecation_header_off_a_real_response(self):
        def handler(request: httpx.Request) -> httpx.Response:
            if request.method == "GET":
                return httpx.Response(
                    200,
                    json={
                        "items": [],
                        "next_cursor": None,
                        "total_pages": 1,
                        "current_page": 1,
                        "total_items": 0,
                    },
                )
            return httpx.Response(
                200,
                headers={"Deprecation": "@1776988800"},
                json={
                    "id": "ca_http",
                    "connection_data": {
                        "auth_scheme": "OAUTH2",
                        "val": {
                            "status": "INITIATED",
                            "redirect_url": "https://redirect",
                        },
                    },
                    "status": "INITIATED",
                    "redirect_url": "https://redirect",
                },
            )

        async def run():
            client = AsyncHttpClient(
                provider="test",
                auth=ProjectAuth("ak"),
                base_url="https://backend.invalid",
                http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
            )
            return await AsyncConnectedAccounts(client).initiate("user-1", "auth-1")

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            request = asyncio.run(run())

        assert request.id == "ca_http"
        assert request.redirect_url == "https://redirect"
        assert any(
            issubclass(w.category, exceptions.ComposioDeprecationWarning)
            and "link()" in str(w.message)
            for w in caught
        )
