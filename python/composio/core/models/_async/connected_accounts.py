from __future__ import annotations

import asyncio
import functools
import inspect
import time
import typing as t

import typing_extensions as te
from composio_client import BadRequestError, ConflictError, RateLimitError, omit

from composio.client import AsyncHttpClient
from composio.client.types import (
    connected_account_complete_auth_response,
    connected_account_create_params,
    connected_account_patch_params,
    connected_account_patch_response,
    connected_account_refresh_params,
    connected_account_refresh_response,
    connected_account_retrieve_response,
    connected_account_revoke_response,
    connected_account_update_status_response,
    link_create_params,
)
from composio.core.models.base import AsyncResource
from composio.core.models.connected_accounts import (
    _INITIAL_POLL_DELAY_SECONDS,
    _REFRESH_DEPRECATION,
    _acl_payload,
    _deprecation_header,
    _guard_multiple_accounts,
    _initiate_connection,
    _next_poll_delay,
    _raise_acl_only_for_shared,
    _raise_if_terminal,
    _raise_legacy_endpoint_retired,
    _refresh_params,
    _retry_after_seconds,
    _revoke_error,
    _wait_timeout_error,
    _warn_legacy_initiate,
)


class AsyncConnectionRequest(AsyncResource):
    """Async counterpart of :class:`~composio.core.models.connected_accounts.ConnectionRequest`."""

    DEFAULT_WAIT_TIMEOUT = 60.0  # Seconds

    def __init__(
        self,
        id: str,
        status: str,
        redirect_url: t.Optional[str],
        client: AsyncHttpClient,
    ):
        super().__init__(client)
        self.id = id
        self.status = status
        self.redirect_url = redirect_url

    async def wait_for_connection(
        self,
        timeout: t.Optional[float] = None,
    ) -> connected_account_retrieve_response.ConnectedAccountRetrieveResponse:
        """See :meth:`ConnectionRequest.wait_for_connection`. Polls without
        blocking the event loop; cancelling the task ends the wait."""
        timeout = self.DEFAULT_WAIT_TIMEOUT if timeout is None else timeout
        deadline = time.time() + timeout
        poll_delay = _INITIAL_POLL_DELAY_SECONDS
        while True:
            delay = poll_delay
            rate_limited = False
            try:
                connection = await self._client.connected_accounts.retrieve(
                    nanoid=self.id
                )
            except RateLimitError as error:
                rate_limited = True
                delay = max(delay, _retry_after_seconds(error))
            else:
                self.status = connection.status
                if self.status == "ACTIVE":
                    return connection
                _raise_if_terminal(self.id, self.status)

            remaining = deadline - time.time()
            # Written so a NaN timeout times out rather than polling forever.
            if not remaining > 0:
                break
            await asyncio.sleep(min(delay, remaining))
            # Don't start a fresh client retry cycle after a rate-limited wait expires.
            if rate_limited and time.time() >= deadline:
                break
            poll_delay = _next_poll_delay(poll_delay)

        raise _wait_timeout_error(self.id)

    @classmethod
    async def from_id(cls, id: str, client: AsyncHttpClient) -> te.Self:
        connection = await client.connected_accounts.retrieve(nanoid=id)
        return cls(
            id=id,
            status=connection.status,
            redirect_url=None,
            client=client,
        )


class AsyncConnectedAccounts:
    """Async counterpart of :class:`~composio.core.models.connected_accounts.ConnectedAccounts`.

    ``get``, ``list``, ``delete``, ``update_status``, ``enable`` and ``disable``
    are the async client's methods and must be awaited.
    """

    enable: t.Callable[
        [str],
        t.Awaitable[
            connected_account_update_status_response.ConnectedAccountUpdateStatusResponse
        ],
    ]
    """Enable a connected account."""

    disable: t.Callable[
        [str],
        t.Awaitable[
            connected_account_update_status_response.ConnectedAccountUpdateStatusResponse
        ],
    ]
    """Disable a connected account."""

    def __init__(self, client: AsyncHttpClient):
        self._client = client
        self.get = self._client.connected_accounts.retrieve
        self.list = self._client.connected_accounts.list
        self.delete = self._client.connected_accounts.delete
        self.update_status = self._client.connected_accounts.update_status
        self.enable = functools.partial(
            self._client.connected_accounts.update_status,
            enabled=True,
        )
        self.disable = functools.partial(
            self._client.connected_accounts.update_status,
            enabled=False,
        )

    @te.deprecated(_REFRESH_DEPRECATION, category=None)
    async def refresh(
        self,
        nanoid: str,
        **params: te.Unpack[
            connected_account_refresh_params.ConnectedAccountRefreshParams
        ],
    ) -> connected_account_refresh_response.ConnectedAccountRefreshResponse:
        """See :meth:`ConnectedAccounts.refresh` (deprecated)."""
        return await self._client.connected_accounts.refresh(
            nanoid, **_refresh_params(params)
        )

    async def revoke(
        self, nanoid: str
    ) -> connected_account_revoke_response.ConnectedAccountRevokeResponse:
        """See :meth:`ConnectedAccounts.revoke`."""
        try:
            return await self._client.connected_accounts.revoke(nanoid)
        except (BadRequestError, ConflictError) as error:
            raise _revoke_error(nanoid, error) from error

    async def complete_auth(
        self, *, user_id: str, session_uri: str
    ) -> connected_account_complete_auth_response.ConnectedAccountCompleteAuthResponse:
        """See :meth:`ConnectedAccounts.complete_auth`."""
        return await self._client.connected_accounts.complete_auth(
            user_id=user_id, session_uri=session_uri
        )

    async def update(
        self,
        nanoid: str,
        *,
        alias: t.Optional[str] = None,
        connection: t.Optional[connected_account_patch_params.Connection] = None,
    ) -> connected_account_patch_response.ConnectedAccountPatchResponse:
        """See :meth:`ConnectedAccounts.update`."""
        return await self._client.connected_accounts.patch(
            nanoid,
            alias=alias if alias is not None else omit,
            connection=connection if connection is not None else omit,
        )

    async def update_acl(
        self,
        nanoid: str,
        *,
        allow_all_users: t.Optional[bool] = None,
        allowed_user_ids: t.Optional[t.List[str]] = None,
        not_allowed_user_ids: t.Optional[t.List[str]] = None,
    ) -> connected_account_patch_response.ConnectedAccountPatchResponse:
        """See :meth:`ConnectedAccounts.update_acl`."""
        experimental = _acl_payload(
            allow_all_users, allowed_user_ids, not_allowed_user_ids
        )
        try:
            return await self._client.connected_accounts.patch(
                nanoid, experimental=experimental
            )
        except BadRequestError as error:
            _raise_acl_only_for_shared(error)
            raise

    async def _guard_multiple(
        self, user_id: str, auth_config_id: str, allow_multiple: bool
    ) -> None:
        connected_accounts = await self.list(
            user_ids=[user_id], auth_config_ids=[auth_config_id], statuses=["ACTIVE"]
        )
        _guard_multiple_accounts(
            connected_accounts.items,
            user_id=user_id,
            auth_config_id=auth_config_id,
            allow_multiple=allow_multiple,
        )

    async def initiate(
        self,
        user_id: str,
        auth_config_id: str,
        *,
        callback_url: t.Optional[str] = None,
        allow_multiple: bool = False,
        config: t.Optional[connected_account_create_params.ConnectionState] = None,
        alias: t.Optional[str] = None,
    ) -> AsyncConnectionRequest:
        """See :meth:`ConnectedAccounts.initiate`, including its deprecation
        for Composio-managed OAuth; prefer :meth:`link` there."""
        await self._guard_multiple(user_id, auth_config_id, allow_multiple)
        connection = _initiate_connection(user_id, callback_url, config, alias)

        # The raw response carries the `Deprecation` header that marks the
        # retiring path; see the sync twin.
        deprecation_header: t.Optional[str] = None
        try:
            ca_client = self._client.connected_accounts
            raw_create = getattr(
                getattr(ca_client, "with_raw_response", None), "create", None
            )
            if callable(raw_create):
                raw = await raw_create(
                    auth_config={"id": auth_config_id},
                    connection=connection,
                )
                response = raw
                if callable(getattr(raw, "parse", None)):
                    response = raw.parse()
                    if inspect.isawaitable(response):
                        response = await response
                deprecation_header = _deprecation_header(raw)
            else:
                response = await ca_client.create(
                    auth_config={"id": auth_config_id},
                    connection=connection,
                )
        except BadRequestError as error:
            _raise_legacy_endpoint_retired(error)
            raise

        _warn_legacy_initiate(deprecation_header)

        return AsyncConnectionRequest(
            id=response.id,
            status=response.connection_data.val.status,
            redirect_url=getattr(response.connection_data.val, "redirect_url", None),
            client=self._client,
        )

    async def link(
        self,
        user_id: str,
        auth_config_id: str,
        *,
        callback_url: t.Optional[str] = None,
        alias: t.Optional[str] = None,
        allow_multiple: bool = False,
        experimental: t.Optional[link_create_params.Experimental] = None,
    ) -> AsyncConnectionRequest:
        """See :meth:`ConnectedAccounts.link`."""
        await self._guard_multiple(user_id, auth_config_id, allow_multiple)

        try:
            response = await self._client.link.create(
                auth_config_id=auth_config_id,
                user_id=user_id,
                callback_url=callback_url if callback_url is not None else omit,
                alias=alias if alias is not None else omit,
                experimental=experimental if experimental is not None else omit,
            )
        except BadRequestError as error:
            _raise_acl_only_for_shared(error)
            raise

        return AsyncConnectionRequest(
            id=response.connected_account_id,
            status="INITIATED",
            redirect_url=getattr(response, "redirect_url", None),
            client=self._client,
        )

    async def wait_for_connection(
        self,
        id: str,
        timeout: t.Optional[float] = None,
    ) -> connected_account_retrieve_response.ConnectedAccountRetrieveResponse:
        """See :meth:`ConnectedAccounts.wait_for_connection`."""
        # No fetch first: a 429 on it would end the wait before polling starts.
        return await AsyncConnectionRequest(
            id=id,
            status="INITIATED",
            redirect_url=None,
            client=self._client,
        ).wait_for_connection(
            timeout=timeout,
        )
