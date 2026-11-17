from __future__ import annotations

import typing as t

import typing_extensions as te

from composio.client import AsyncHttpClient
from composio.client.types import (
    AuthSchemeL,
    toolkit_list_params,
    toolkit_list_response,
    toolkit_recommend_scopes_params,
    toolkit_recommend_scopes_response,
    toolkit_retrieve_changelog_response,
    toolkit_retrieve_response,
    toolkit_retrieve_scopes_grant_context_params,
    toolkit_retrieve_scopes_grant_context_response,
)
from composio.core.models.base import AsyncResource
from composio.core.models.toolkits import (
    AuthFieldsT,
    _auth_fields,
    _latest_auth_config_id,
)
from composio.utils.pydantic import none_to_omit

from .connected_accounts import AsyncConnectedAccounts, AsyncConnectionRequest


class AsyncToolkits(AsyncResource):
    """Async counterpart of :class:`~composio.core.models.toolkits.Toolkits`."""

    connected_accounts: AsyncConnectedAccounts

    def __init__(self, client: AsyncHttpClient):
        super().__init__(client)
        self.connected_accounts = AsyncConnectedAccounts(client)

    async def list(
        self,
        *,
        category: t.Optional[str] = None,
        cursor: t.Optional[str] = None,
        limit: t.Optional[float] = None,
        sort_by: t.Optional[t.Literal["usage", "alphabetically"]] = None,
        managed_by: t.Optional[t.Literal["composio", "all", "project"]] = None,
    ) -> toolkit_list_response.ToolkitListResponse:
        """See :meth:`Toolkits.list`."""
        return await self._client.toolkits.list(
            category=none_to_omit(category),
            cursor=none_to_omit(cursor),
            limit=none_to_omit(limit),
            managed_by=none_to_omit(managed_by),
            sort_by=none_to_omit(sort_by),
        )

    @t.overload
    async def get(self) -> t.List[toolkit_list_response.Item]:
        """Get all toolkits."""

    @t.overload
    async def get(self, slug: str) -> toolkit_retrieve_response.ToolkitRetrieveResponse:
        """Get a toolkit by slug."""

    @t.overload
    async def get(
        self,
        *,
        query: toolkit_list_params.ToolkitListParams,
    ) -> t.List[toolkit_list_response.Item]:
        """Get a list of toolkits by query."""

    async def get(
        self,
        slug: t.Optional[str] = None,
        *,
        query: t.Optional[toolkit_list_params.ToolkitListParams] = None,
    ) -> t.Union[
        toolkit_retrieve_response.ToolkitRetrieveResponse,
        t.List[toolkit_list_response.Item],
    ]:
        if slug is not None:
            return await self._client.toolkits.retrieve(slug=slug)
        return (await self._client.toolkits.list(**(query or {}))).items

    async def get_many(
        self,
        slugs: t.Sequence[str],
        *,
        category: t.Optional[str] = None,
        managed_by: t.Optional[t.Literal["composio", "all", "project"]] = None,
        sort_by: t.Optional[t.Literal["usage", "alphabetically"]] = None,
        limit: t.Optional[float] = None,
        cursor: t.Optional[str] = None,
    ) -> t.List[t.Any]:
        """See :meth:`Toolkits.get_many`."""
        response = await self._client.toolkits.retrieve_multi(
            toolkits=list(slugs),
            category=none_to_omit(category),
            managed_by=none_to_omit(managed_by),
            sort_by=none_to_omit(sort_by),
            limit=none_to_omit(limit),
            cursor=none_to_omit(cursor),
        )
        return response.items

    async def changelog(
        self,
    ) -> toolkit_retrieve_changelog_response.ToolkitRetrieveChangelogResponse:
        """See :meth:`Toolkits.changelog`."""
        return await self._client.toolkits.retrieve_changelog()

    async def recommend_scopes(
        self,
        toolkit_slug: str,
        **params: te.Unpack[
            toolkit_recommend_scopes_params.ToolkitRecommendScopesParams
        ],
    ) -> toolkit_recommend_scopes_response.ToolkitRecommendScopesResponse:
        """See :meth:`Toolkits.recommend_scopes`."""
        return await self._client.toolkits.recommend_scopes(toolkit_slug, **params)

    async def list_grant_contexts(
        self,
        toolkit_slug: str,
        **params: te.Unpack[
            toolkit_retrieve_scopes_grant_context_params.ToolkitRetrieveScopesGrantContextParams
        ],
    ) -> toolkit_retrieve_scopes_grant_context_response.ToolkitRetrieveScopesGrantContextResponse:
        """See :meth:`Toolkits.list_grant_contexts`."""
        return await self._client.toolkits.retrieve_scopes_grant_context(
            toolkit_slug, **params
        )

    async def list_categories(self):
        """See :meth:`Toolkits.list_categories`."""
        return (await self._client.toolkits.retrieve_categories()).items

    async def _get_auth_config_id(self, toolkit: str) -> str:
        auth_configs = await self._client.auth_configs.list(toolkit_slug=toolkit)
        existing = _latest_auth_config_id(auth_configs.items)
        if existing is not None:
            return existing

        created = await self._client.auth_configs.create(
            toolkit={"slug": toolkit},
            auth_config={
                "type": "use_composio_managed_auth",
                "tool_access_config": {
                    "tools_for_connected_account_creation": [],
                },
            },
        )
        return created.auth_config.id

    async def authorize(self, *, user_id: str, toolkit: str) -> AsyncConnectionRequest:
        """See :meth:`Toolkits.authorize`."""
        return await self.connected_accounts.initiate(
            user_id=user_id,
            auth_config_id=await self._get_auth_config_id(toolkit=toolkit),
        )

    async def get_connected_account_initiation_fields(
        self,
        toolkit: str,
        auth_scheme: AuthSchemeL,
        required_only: bool = False,
    ) -> AuthFieldsT:
        """See :meth:`Toolkits.get_connected_account_initiation_fields`."""
        info = await self._client.toolkits.retrieve(slug=toolkit)
        return _auth_fields(
            info,
            "connected_account_initiation",
            toolkit=toolkit,
            auth_scheme=auth_scheme,
            required_only=required_only,
        )

    async def get_auth_config_creation_fields(
        self,
        toolkit: str,
        auth_scheme: AuthSchemeL,
        required_only: bool = False,
    ) -> AuthFieldsT:
        """See :meth:`Toolkits.get_auth_config_creation_fields`."""
        info = await self._client.toolkits.retrieve(slug=toolkit)
        return _auth_fields(
            info,
            "auth_config_creation",
            toolkit=toolkit,
            auth_scheme=auth_scheme,
            required_only=required_only,
        )
