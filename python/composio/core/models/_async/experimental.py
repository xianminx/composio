"""The ``composio.experimental`` namespace of :class:`~composio.AsyncComposio`."""

from __future__ import annotations

import typing as t
import warnings

import typing_extensions as te

from composio import exceptions
from composio.client import AsyncHttpClient
from composio.client.types import (
    connected_account_patch_response,
    custom_create_toolkit_params,
    custom_create_toolkit_response,
    custom_delete_toolkit_response,
    custom_sync_response,
    custom_update_toolkits_params,
    custom_update_toolkits_response,
    custom_upsert_params,
    custom_upsert_response,
    usage_retrieve_params,
    usage_retrieve_response,
    usage_retrieve_summary_params,
)
from composio.core.models import experimental as _sync
from composio.core.models.experimental import (
    _UPSERT_DEPRECATION,
    UsageSummaryResponse,
    _ExperimentalDecorators,
)
from composio.utils.pydantic import none_to_omit


class AsyncExperimentalUsage:
    """Async counterpart of :class:`~composio.core.models.experimental.ExperimentalUsage`."""

    def __init__(self, client: t.Optional[AsyncHttpClient] = None) -> None:
        self._client = client

    def _require_client(self) -> AsyncHttpClient:
        if self._client is None:
            raise exceptions.ValidationError(
                "experimental.usage requires a Composio client. Access it via "
                "composio.experimental.usage.summary(...)."
            )
        return self._client

    async def summary(
        self,
        **params: te.Unpack[usage_retrieve_summary_params.UsageRetrieveSummaryParams],
    ) -> UsageSummaryResponse:
        """See :meth:`ExperimentalUsage.summary`."""
        response = await self._require_client().project.usage.retrieve_summary(**params)
        return UsageSummaryResponse.model_validate(response.model_dump())

    async def breakdown(
        self,
        entity_type: str,
        **params: te.Unpack[usage_retrieve_params.UsageRetrieveParams],
    ) -> usage_retrieve_response.UsageRetrieveResponse:
        """See :meth:`ExperimentalUsage.breakdown`."""
        return await self._require_client().project.usage.retrieve(
            entity_type, **params
        )


class AsyncCustomToolkits:
    """Async counterpart of :class:`~composio.core.models.experimental.CustomToolkits`."""

    def __init__(self, client: t.Optional[AsyncHttpClient] = None) -> None:
        self._client = client

    def _require_client(self) -> AsyncHttpClient:
        if self._client is None:
            raise exceptions.ValidationError(
                "custom_toolkits requires a Composio client. Access it via "
                "composio.custom_toolkits.create(...)."
            )
        return self._client

    async def create(
        self,
        **params: te.Unpack[custom_create_toolkit_params.CustomCreateToolkitParams],
    ) -> custom_create_toolkit_response.CustomCreateToolkitResponse:
        """See :meth:`CustomToolkits.create`."""
        return await self._require_client().custom.create_toolkit(**params)

    async def update(
        self,
        slug: str,
        **params: te.Unpack[custom_update_toolkits_params.CustomUpdateToolkitsParams],
    ) -> custom_update_toolkits_response.CustomUpdateToolkitsResponse:
        """See :meth:`CustomToolkits.update`."""
        return await self._require_client().custom.update_toolkits(slug, **params)

    @te.deprecated(_UPSERT_DEPRECATION, category=None)
    async def upsert(
        self,
        **params: te.Unpack[custom_upsert_params.CustomUpsertParams],
    ) -> custom_upsert_response.CustomUpsertResponse:
        """See :meth:`CustomToolkits.upsert`."""
        # The once-per-process flag is shared with the sync class.
        if not _sync._upsert_warning_emitted:
            _sync._upsert_warning_emitted = True
            warnings.warn(
                _UPSERT_DEPRECATION,
                exceptions.ComposioDeprecationWarning,
                stacklevel=2,
            )
        return await self._require_client().custom.upsert(**params)

    async def sync(
        self, slug: str, *, connected_account_id: t.Optional[str] = None
    ) -> custom_sync_response.CustomSyncResponse:
        """See :meth:`CustomToolkits.sync`."""
        return await self._require_client().custom.sync(
            slug=slug, connected_account_id=none_to_omit(connected_account_id)
        )

    async def delete(
        self, slug: str
    ) -> custom_delete_toolkit_response.CustomDeleteToolkitResponse:
        """See :meth:`CustomToolkits.delete`."""
        return await self._require_client().custom.delete_toolkit(slug)


class AsyncExperimentalAPI(_ExperimentalDecorators):
    """Async counterpart of :class:`~composio.core.models.experimental.ExperimentalAPI`.

    ``tool`` and ``Toolkit`` are the same builders as on the sync namespace.
    """

    usage: AsyncExperimentalUsage
    custom_toolkits: AsyncCustomToolkits

    def __init__(self, client: t.Optional[AsyncHttpClient] = None) -> None:
        self._client = client
        self.usage = AsyncExperimentalUsage(client=client)
        self.custom_toolkits = AsyncCustomToolkits(client=client)

    async def update_acl(
        self,
        nanoid: str,
        *,
        allow_all_users: t.Optional[bool] = None,
        allowed_user_ids: t.Optional[t.List[str]] = None,
        not_allowed_user_ids: t.Optional[t.List[str]] = None,
    ) -> connected_account_patch_response.ConnectedAccountPatchResponse:
        """See :meth:`ExperimentalAPI.update_acl`; prefer
        ``composio.connected_accounts.update_acl``."""
        from .connected_accounts import AsyncConnectedAccounts

        if self._client is None:
            raise exceptions.ValidationError(
                "update_acl requires a Composio client. Access it via "
                "composio.connected_accounts.update_acl(...)."
            )

        return await AsyncConnectedAccounts(client=self._client).update_acl(
            nanoid,
            allow_all_users=allow_all_users,
            allowed_user_ids=allowed_user_ids,
            not_allowed_user_ids=not_allowed_user_ids,
        )
