from __future__ import annotations

import typing as t

import typing_extensions as te
from composio_client import omit
from composio_client.types.mcp.custom_create_response import CustomCreateResponse

from composio.core.models.base import AsyncResource
from composio.core.models.mcp import (
    ConfigToolkit,
    MCPCreateResponse,
    MCPListResponse,
    MCPServerInstance,
    _list_response,
    _managed_auth_by_composio,
    _server_instance,
    _toolkit_names_and_auth_config_ids,
    _update_params,
)
from composio.exceptions import ValidationError
from composio.utils.pydantic import none_to_omit


def _add_generate_method(
    response: CustomCreateResponse, mcp_instance: "AsyncMCP"
) -> MCPCreateResponse:
    async def generate(
        user_id: str, manually_manage_connections: t.Optional[bool] = None
    ) -> MCPServerInstance:
        """Awaitable: generate a server instance for this MCP configuration."""
        return await mcp_instance.generate(
            user_id, response.id, manually_manage_connections
        )

    response.generate = generate  # type: ignore
    return t.cast(MCPCreateResponse, response)


class AsyncMCP(AsyncResource):
    """Async counterpart of :class:`~composio.core.models.mcp.MCP`.

    .. deprecated::
        Prefer a session MCP endpoint: ``await composio.create(user_id, mcp=True)``.
    """

    async def create(
        self,
        name: str,
        toolkits: t.List[t.Union[ConfigToolkit, str]],
        manually_manage_connections: bool = False,
        allowed_tools: t.Optional[t.List[str]] = None,
    ) -> MCPCreateResponse:
        """See :meth:`MCP.create`. The returned ``generate`` must be awaited:
        ``await server.generate(user_id)``."""
        if not toolkits:
            raise ValidationError("At least one toolkit configuration is required")

        try:
            toolkit_names, auth_config_ids = _toolkit_names_and_auth_config_ids(
                toolkits
            )
            response = await self._client.mcp.custom.create(
                name=name,
                toolkits=toolkit_names,
                auth_config_ids=auth_config_ids,
                allowed_tools=none_to_omit(allowed_tools),
                managed_auth_via_composio=not manually_manage_connections,
            )
            return _add_generate_method(response, self)
        except Exception as e:
            raise ValidationError("Failed to create MCP server") from e

    async def list(
        self,
        page_no: t.Optional[int] = None,
        limit: t.Optional[int] = None,
        toolkits: t.Optional[str] = None,
        auth_config_ids: t.Optional[str] = None,
        name: t.Optional[str] = None,
        order_by: t.Optional[te.Literal["created_at", "updated_at"]] = None,
        order_direction: t.Optional[te.Literal["asc", "desc"]] = None,
    ) -> MCPListResponse:
        """See :meth:`MCP.list`."""
        try:
            response = await self._client.mcp.list(
                page_no=page_no,
                limit=limit,
                toolkits=none_to_omit(toolkits),
                auth_config_ids=none_to_omit(auth_config_ids),
                name=none_to_omit(name),
                order_by=none_to_omit(order_by),
                order_direction=none_to_omit(order_direction),
            )
            return _list_response(response, page_no)
        except Exception as e:
            raise ValidationError("Failed to list MCP servers") from e

    async def get(self, server_id: str):
        """See :meth:`MCP.get`."""
        try:
            return await self._client.mcp.retrieve(server_id)
        except Exception as e:
            raise ValidationError(f"Failed to retrieve MCP server {server_id}") from e

    async def update(
        self,
        server_id: str,
        name: t.Optional[str] = None,
        toolkits: t.Optional[t.List[t.Union[ConfigToolkit, str]]] = None,
        manually_manage_connections: t.Optional[bool] = None,
        allowed_tools: t.Optional[t.List[str]] = None,
    ):
        """See :meth:`MCP.update`."""
        try:
            update_params = _update_params(
                name, toolkits, manually_manage_connections, allowed_tools
            )
            return await self._client.mcp.update(server_id, **update_params)
        except Exception as e:
            raise ValidationError(f"Failed to update MCP server {server_id}") from e

    async def delete(self, server_id: str) -> t.Dict[str, t.Any]:
        """See :meth:`MCP.delete`."""
        try:
            response = await self._client.mcp.delete(server_id)
            return {
                "id": server_id,
                "deleted": getattr(response, "deleted", True),
            }
        except Exception as e:
            raise ValidationError(f"Failed to delete MCP server {server_id}") from e

    async def generate(
        self,
        user_id: str,
        mcp_config_id: str,
        manually_manage_connections: t.Optional[bool] = None,
    ) -> MCPServerInstance:
        """See :meth:`MCP.generate`."""
        try:
            server_details = await self._client.mcp.retrieve(mcp_config_id)
            managed_auth_by_composio = _managed_auth_by_composio(
                manually_manage_connections
            )
            url_response = await self._client.mcp.generate.url(
                mcp_server_id=mcp_config_id,
                user_ids=[user_id],
                managed_auth_by_composio=True if managed_auth_by_composio else omit,
            )
            return _server_instance(server_details, url_response, user_id)
        except Exception as e:
            if isinstance(e, ValidationError):
                raise
            raise ValidationError("Failed to parse MCP server instance") from e
