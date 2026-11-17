"""Session context for local tools running inside an async session.

Local tool functions are synchronous and run on a worker thread, so the
context they receive is synchronous too: its remote calls are handed to the
event loop the session runs on.
"""

from __future__ import annotations

import asyncio
import typing as t

from composio_client.types.tool_router.session_execute_response import (
    SessionExecuteResponse,
)

from composio.client import AsyncHttpClient
from composio.core.models.custom_tool_types import (
    CustomToolsMap,
    InlineCustomToolsWirePayload,
    ToolRouterSessionProxyExecuteResponse,
)
from composio.core.models.session_context import (
    SessionContextImpl,
    proxy_execute_request,
    proxy_execute_result,
)
from composio.core.models.tools import require_execute_result
from composio.exceptions import ComposioError
from composio.utils.async_bridge import run_on_loop


async def async_proxy_execute_impl(
    client: AsyncHttpClient,
    session_id: str,
    *,
    toolkit: str,
    endpoint: str,
    method: t.Literal["GET", "POST", "PUT", "DELETE", "PATCH"],
    body: t.Any = None,
    parameters: t.Optional[t.List[t.Dict[str, t.Any]]] = None,
) -> ToolRouterSessionProxyExecuteResponse:
    """Async counterpart of :func:`~composio.core.models.session_context.proxy_execute_impl`."""
    request, subject = proxy_execute_request(
        session_id,
        toolkit=toolkit,
        endpoint=endpoint,
        method=method,
        body=body,
        parameters=parameters,
    )
    # Disable retries: a proxied call is a non-idempotent write, and a silent
    # retry after a read timeout can duplicate the side effect.
    return proxy_execute_result(
        await client.without_retries.tool_router.session.proxy_execute(**request),
        subject,
    )


class AsyncSessionContextImpl(SessionContextImpl):
    """:class:`SessionContextImpl` over an async client.

    ``execute`` and ``proxy_execute`` block the calling worker thread while the
    request runs on the session's event loop.
    """

    def __init__(
        self,
        client: AsyncHttpClient,
        user_id: str,
        session_id: str,
        custom_tools_map: t.Optional[CustomToolsMap] = None,
        inline_custom_tools_payload: t.Optional[InlineCustomToolsWirePayload] = None,
    ) -> None:
        super().__init__(
            client=t.cast(t.Any, client),
            user_id=user_id,
            session_id=session_id,
            custom_tools_map=custom_tools_map,
            inline_custom_tools_payload=inline_custom_tools_payload,
        )
        self._async_client = client
        self._loop: t.Optional[asyncio.AbstractEventLoop] = None

    def bind_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        self._loop = loop

    def _require_loop(self) -> asyncio.AbstractEventLoop:
        if self._loop is None:
            raise ComposioError(
                "This session context is not bound to an event loop; local tools "
                "of an async session must be run through the session"
            )
        return self._loop

    async def _remote_execute_async(
        self, tool_slug: str, serialized: t.Dict[str, t.Any]
    ) -> SessionExecuteResponse:
        # Disable retries: a session execution is a non-idempotent write, and a
        # silent retry after a read timeout can duplicate the side effect.
        return require_execute_result(
            await self._async_client.without_retries.tool_router.session.execute(
                **self._remote_execute_request(tool_slug, serialized)
            ),
            tool_slug,
        )

    def _remote_execute(
        self, tool_slug: str, serialized: t.Dict[str, t.Any]
    ) -> SessionExecuteResponse:
        return run_on_loop(
            self._require_loop(), self._remote_execute_async(tool_slug, serialized)
        )

    def proxy_execute(
        self,
        *,
        toolkit: str,
        endpoint: str,
        method: t.Literal["GET", "POST", "PUT", "DELETE", "PATCH"],
        body: t.Any = None,
        parameters: t.Optional[t.List[t.Dict[str, t.Any]]] = None,
    ) -> ToolRouterSessionProxyExecuteResponse:
        return run_on_loop(
            self._require_loop(),
            async_proxy_execute_impl(
                self._async_client,
                self._session_id,
                toolkit=toolkit,
                endpoint=endpoint,
                method=method,
                body=body,
                parameters=parameters,
            ),
        )
