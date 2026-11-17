from __future__ import annotations

import functools
import typing as t
from pathlib import Path

from composio_client import APIStatusError, omit

from composio.client import AsyncHttpClient
from composio.client.types import (
    Tool,
    tool_execute_params,
    tool_proxy_params,
    tool_proxy_response,
)
from composio.core.models._async._files import AsyncFileHelper
from composio.core.models._modifiers import (
    Modifiers,
    apply_modifier_by_type,
    merge_before_file_upload,
)
from composio.core.models.base import AsyncResource
from composio.core.models.custom_tool_types import InlineCustomToolsWirePayload
from composio.core.models.tools import (
    TOOL_ROUTER_SESSION_TOOLS_PAGE_LIMIT,
    ToolExecutionResponse,
    _apply_before_execute,
    _apply_schema_modifiers,
    _cache_tool_schemas,
    _ExecuteCall,
    _normalize_tool,
    _prepare_tools_for_provider,
    _resolve_execute_version,
    _serialize_arguments,
    _session_after_execute,
    _session_before_execute,
    _toolkit_slug,
    require_execute_result,
)
from composio.core.provider import TTool, TToolCollection
from composio.core.provider.agentic import (
    AgenticProvider,
    AgenticProviderExecuteFn,
    bind_execution_target,
)
from composio.core.provider.base import (
    AsyncExecuteToolFn,
    BaseProvider,
    ExecuteToolFn,
)
from composio.core.provider.none_agentic import NonAgenticProvider
from composio.core.types import ToolkitVersionParam
from composio.exceptions import ComposioError, InvalidParams, ToolNotFoundError
from composio.utils.async_bridge import sync_bridge
from composio.utils.pydantic import none_to_omit
from composio.utils.upload_dir_allowlist import resolve_effective_upload_allowlist


class AsyncAgenticProviderExecuteFn(t.Protocol):
    """:class:`AgenticProviderExecuteFn` as a coroutine function."""

    def __call__(
        self, slug: str, arguments: t.Dict
    ) -> t.Coroutine[t.Any, t.Any, t.Dict]: ...


def _sync_execute_unavailable(*_: t.Any, **__: t.Any) -> t.NoReturn:
    raise ComposioError(
        "This provider is bound to AsyncComposio, so its synchronous tool "
        "execution helpers are unavailable. Await `provider.aexecute_tool(...)` "
        "or `composio.tools.execute(...)`; the OpenAI providers also have "
        "`ahandle_tool_calls`."
    )


class AsyncTools(AsyncResource, t.Generic[TTool, TToolCollection]):
    """Async counterpart of :class:`~composio.core.models.tools.Tools`.

    Agentic providers still receive a blocking ``execute_tool`` callable, as
    their frameworks call tools synchronously. It runs the execution on the
    event loop ``get`` was awaited on, so it must be called from another thread
    (frameworks that offload sync tools to an executor already do).
    """

    provider: BaseProvider[TTool, TToolCollection]

    def __init__(
        self,
        client: AsyncHttpClient,
        provider: BaseProvider[TTool, TToolCollection],
        file_download_dir: t.Optional[str] = None,
        toolkit_versions: t.Optional[ToolkitVersionParam] = None,
        dangerously_allow_auto_upload_download_files: bool = False,
        sensitive_file_upload_protection: bool = True,
        file_upload_path_deny_segments: t.Optional[t.Sequence[str]] = None,
        file_upload_dirs: t.Union[t.Sequence[str], t.Literal[False], None] = None,
    ):
        """See :meth:`Tools.__init__`."""
        self._auto_upload_download_files = bool(
            dangerously_allow_auto_upload_download_files
        )
        resolved_allowlist: t.Optional[t.List[Path]] = (
            resolve_effective_upload_allowlist(file_upload_dirs)
            if self._auto_upload_download_files
            else None
        )

        self._client = client
        self._tool_schemas: t.Dict[str, Tool] = {}
        self._file_helper = AsyncFileHelper(
            client=self._client,
            outdir=file_download_dir,
            sensitive_file_upload_protection=sensitive_file_upload_protection,
            file_upload_path_deny_segments=file_upload_path_deny_segments,
            file_upload_allowlist=resolved_allowlist,
        )
        self._toolkit_versions = toolkit_versions

        self.provider = provider
        self.provider.set_async_execute_tool_fn(
            t.cast(
                AsyncExecuteToolFn,
                functools.partial(
                    self.execute,
                    # Dangerously skip version check for provider controlled tool execution
                    dangerously_skip_version_check=True,
                ),
            ),
        )
        self.provider.set_execute_tool_fn(
            t.cast(ExecuteToolFn, _sync_execute_unavailable)
        )

    async def _cached_tool(self, slug: str) -> Tool:
        tool = self._tool_schemas.get(slug)
        if tool is None:
            tool = await self.get_raw_composio_tool_by_slug(slug)
            self._tool_schemas[slug] = tool
        return tool

    async def get_raw_composio_tool_by_slug(self, slug: str) -> Tool:
        """See :meth:`Tools.get_raw_composio_tool_by_slug`."""
        try:
            response = await self._client.tools.retrieve(
                tool_slug=slug,
                toolkit_versions=none_to_omit(self._toolkit_versions),
            )
        except APIStatusError as error:
            if error.status_code in (400, 404):
                raise ToolNotFoundError(f"Tool with slug {slug} not found") from error
            raise
        return _normalize_tool(response)

    async def get_raw_composio_tools(
        self,
        tools: t.Optional[list[str]] = None,
        search: t.Optional[str] = None,
        toolkits: t.Optional[list[str]] = None,
        scopes: t.Optional[t.List[str]] = None,
        limit: t.Optional[int] = None,
    ) -> list[Tool]:
        """See :meth:`Tools.get_raw_composio_tools`."""
        if tools is None and search is None and toolkits is None:
            raise InvalidParams(
                "Either `tools`, `search`, or `toolkits` must be provided"
            )

        tools_list: t.List[t.Any] = []
        if tools is not None:
            if len(tools):
                by_slug = await self._client.tools.list(
                    tool_slugs=",".join(tools),
                    toolkit_versions=none_to_omit(self._toolkit_versions),
                )
                tools_list.extend(by_slug.items)

        if toolkits is not None or search is not None:
            by_filter = await self._client.tools.list(
                toolkit_slug=none_to_omit(",".join(toolkits) if toolkits else None),
                query=none_to_omit(search),
                scopes=scopes,
                limit=limit,
                toolkit_versions=none_to_omit(self._toolkit_versions),
            )
            tools_list.extend(by_filter.items)
        return [_normalize_tool(tool) for tool in tools_list]

    async def get_raw_tool_router_meta_tools(
        self,
        session_id: str,
        modifiers: t.Optional[Modifiers] = None,
    ) -> list[Tool]:
        """See :meth:`Tools.get_raw_tool_router_meta_tools`."""
        tools_list: t.List[Tool] = []
        cursor: t.Optional[str] = None

        while True:
            tools_response = await self._client.tool_router.session.tools(
                session_id=session_id,
                cursor=none_to_omit(cursor),
                limit=TOOL_ROUTER_SESSION_TOOLS_PAGE_LIMIT,
            )
            tools_list.extend(_normalize_tool(item) for item in tools_response.items)

            cursor = getattr(tools_response, "next_cursor", None)
            if not cursor:
                break

        return _apply_schema_modifiers(
            _cache_tool_schemas(self._tool_schemas, tools_list), modifiers
        )

    async def _get(
        self,
        user_id: str,
        tools: t.Optional[list[str]] = None,
        search: t.Optional[str] = None,
        toolkits: t.Optional[list[str]] = None,
        scopes: t.Optional[t.List[str]] = None,
        modifiers: t.Optional[Modifiers] = None,
        limit: t.Optional[int] = None,
    ) -> TToolCollection:
        raw_tools = await self.get_raw_composio_tools(
            tools=tools,
            search=search,
            toolkits=toolkits,
            scopes=scopes,
            limit=limit,
        )
        tools_list = _prepare_tools_for_provider(
            raw_tools,
            tool_schemas=self._tool_schemas,
            modifiers=modifiers,
            file_helper=self._file_helper,
            auto_upload_download_files=self._auto_upload_download_files,
        )

        if issubclass(type(self.provider), NonAgenticProvider):
            return t.cast(
                TToolCollection,
                t.cast(
                    NonAgenticProvider[TTool, TToolCollection], self.provider
                ).wrap_tools(tools=tools_list),
            )

        return t.cast(
            TToolCollection,
            t.cast(AgenticProvider[TTool, TToolCollection], self.provider).wrap_tools(
                tools=tools_list,
                execute_tool=self._wrap_execute_tool(
                    user_id=user_id,
                    modifiers=modifiers,
                ),
            ),
        )

    async def get(
        self,
        user_id: str,
        *,
        slug: t.Optional[str] = None,
        tools: t.Optional[list[str]] = None,
        search: t.Optional[str] = None,
        toolkits: t.Optional[list[str]] = None,
        scopes: t.Optional[t.List[str]] = None,
        modifiers: t.Optional[Modifiers] = None,
        limit: t.Optional[int] = None,
    ) -> TToolCollection:
        """See :meth:`Tools.get`."""
        if slug is not None:
            return await self._get(
                user_id=user_id,
                tools=[slug],
                modifiers=modifiers,
            )
        return await self._get(
            user_id=user_id,
            tools=tools,
            search=search,
            toolkits=toolkits,
            scopes=scopes,
            modifiers=modifiers,
            limit=limit,
        )

    def _wrap_execute_tool(
        self,
        modifiers: t.Optional[Modifiers] = None,
        user_id: t.Optional[str] = None,
    ) -> AgenticProviderExecuteFn:
        """A blocking execute function for an agentic provider's tools.

        Bound to the running event loop; see :func:`sync_bridge`.
        """
        execute_tool = t.cast(
            AgenticProviderExecuteFn,
            sync_bridge(
                functools.partial(
                    self.execute,
                    modifiers=modifiers,
                    user_id=user_id,
                    # Dangerously skip version check for agentic tool execution via providers
                    dangerously_skip_version_check=True,
                )
            ),
        )
        if user_id is None:
            return execute_tool
        return bind_execution_target(execute_tool, user_id)

    def _async_execute_tool_for_tool_router(
        self,
        session_id: str,
        modifiers: t.Optional[Modifiers] = None,
        inline_custom_tools_payload: t.Optional[InlineCustomToolsWirePayload] = None,
    ) -> AsyncAgenticProviderExecuteFn:
        """The coroutine function behind :meth:`_wrap_execute_tool_for_tool_router`."""

        async def execute_tool_fn(slug: str, arguments: t.Dict) -> t.Dict:
            tool = await self._cached_tool(slug)
            toolkit_slug = _toolkit_slug(tool, "composio")

            if self._auto_upload_download_files:
                arguments = await self._file_helper.substitute_file_uploads(
                    tool=tool,
                    request=arguments,
                    before_file_upload=merge_before_file_upload(
                        modifiers,
                        tool=slug,
                        toolkit=toolkit_slug,
                    ),
                )
            else:
                arguments = self._file_helper.drop_empty_file_uploads(
                    tool=tool, request=arguments
                )

            processed_arguments = _session_before_execute(
                modifiers, toolkit_slug, slug, arguments
            )

            # Disable retries: a session execution is a non-idempotent write, and a
            # silent retry after a read timeout can duplicate the side effect.
            response = require_execute_result(
                await self._client.without_retries.tool_router.session.execute(
                    session_id=session_id,
                    tool_slug=slug,
                    arguments=processed_arguments,
                    # Provider-wrapped session tools are agentic calls, so they opt into
                    # direct tool offload when the backend session workbench allows it.
                    enable_auto_workbench_offload=True,
                    experimental=inline_custom_tools_payload
                    if inline_custom_tools_payload is not None
                    else omit,
                ),
                slug,
            )
            return _session_after_execute(modifiers, toolkit_slug, slug, response)

        return execute_tool_fn

    def _wrap_execute_tool_for_tool_router(
        self,
        session_id: str,
        modifiers: t.Optional[Modifiers] = None,
        inline_custom_tools_payload: t.Optional[InlineCustomToolsWirePayload] = None,
    ) -> AgenticProviderExecuteFn:
        """See :meth:`Tools._wrap_execute_tool_for_tool_router`.

        Returns a blocking function bound to the running event loop; the
        coroutine function is :meth:`_async_execute_tool_for_tool_router`.
        """
        return t.cast(
            AgenticProviderExecuteFn,
            sync_bridge(
                self._async_execute_tool_for_tool_router(
                    session_id=session_id,
                    modifiers=modifiers,
                    inline_custom_tools_payload=inline_custom_tools_payload,
                )
            ),
        )

    async def _execute_tool(
        self,
        slug: str,
        arguments: t.Dict,
        tool: Tool,
        connected_account_id: t.Optional[str] = None,
        custom_auth_params: t.Optional[tool_execute_params.CustomAuthParams] = None,
        custom_connection_data: t.Optional[
            tool_execute_params.CustomConnectionData
        ] = None,
        user_id: t.Optional[str] = None,
        text: t.Optional[str] = None,
        version: t.Optional[str] = None,
        dangerously_skip_version_check: t.Optional[bool] = None,
    ) -> ToolExecutionResponse:
        arguments = _serialize_arguments(arguments)
        version = _resolve_execute_version(
            tool, version, self._toolkit_versions, dangerously_skip_version_check
        )

        # Disable retries: tool execution is a non-idempotent write, and a
        # silent retry after a read timeout can duplicate the side effect.
        response = await self._client.without_retries.tools.execute(
            tool_slug=slug,
            arguments=arguments,
            connected_account_id=none_to_omit(connected_account_id),
            custom_auth_params=none_to_omit(custom_auth_params),
            custom_connection_data=none_to_omit(custom_connection_data),
            user_id=none_to_omit(user_id),
            text=none_to_omit(text),
            version=none_to_omit(version),
        )
        return t.cast(
            ToolExecutionResponse,
            response.model_dump(
                exclude={
                    "log_id",
                    "session_info",
                }
            ),
        )

    async def execute(
        self,
        slug: str,
        arguments: t.Dict,
        *,
        connected_account_id: t.Optional[str] = None,
        custom_auth_params: t.Optional[tool_execute_params.CustomAuthParams] = None,
        custom_connection_data: t.Optional[
            tool_execute_params.CustomConnectionData
        ] = None,
        user_id: t.Optional[str] = None,
        text: t.Optional[str] = None,
        version: t.Optional[str] = None,
        dangerously_skip_version_check: t.Optional[bool] = None,
        modifiers: t.Optional[Modifiers] = None,
    ) -> ToolExecutionResponse:
        """See :meth:`Tools.execute`.

        Modifiers are plain functions and run on the event loop.
        """
        tool = await self._cached_tool(slug)

        if self._auto_upload_download_files:
            arguments = await self._file_helper.substitute_file_uploads(
                tool=tool,
                request=arguments,
                before_file_upload=merge_before_file_upload(
                    modifiers,
                    tool=slug,
                    toolkit=_toolkit_slug(tool, "unknown"),
                ),
            )
        else:
            # "No file" must be sent as an omitted key rather than ``""`` (issue #4233).
            arguments = self._file_helper.drop_empty_file_uploads(
                tool=tool, request=arguments
            )

        if modifiers is not None:
            (
                arguments,
                connected_account_id,
                custom_auth_params,
                custom_connection_data,
                user_id,
                text,
                version,
                dangerously_skip_version_check,
            ) = _apply_before_execute(
                modifiers,
                tool,
                slug,
                _ExecuteCall(
                    arguments=arguments,
                    connected_account_id=connected_account_id,
                    custom_auth_params=custom_auth_params,
                    custom_connection_data=custom_connection_data,
                    user_id=user_id,
                    text=text,
                    version=version,
                    dangerously_skip_version_check=dangerously_skip_version_check,
                ),
            )
        response = await self._execute_tool(
            slug=slug,
            arguments=arguments,
            tool=tool,
            connected_account_id=connected_account_id,
            custom_auth_params=custom_auth_params,
            custom_connection_data=custom_connection_data,
            user_id=user_id,
            text=text,
            version=version,
            dangerously_skip_version_check=dangerously_skip_version_check,
        )
        if self._auto_upload_download_files:
            response = await self._file_helper.substitute_file_downloads(
                tool=tool,
                response=response,
            )
        if modifiers is not None:
            response = apply_modifier_by_type(
                modifiers=modifiers,
                toolkit=_toolkit_slug(tool, "unknown"),
                tool=slug,
                type="after_execute",
                response=response,
            )
        return response

    async def proxy(
        self,
        endpoint: str,
        method: t.Literal["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD"],
        body: t.Optional[object] = None,
        connected_account_id: t.Optional[str] = None,
        parameters: t.Optional[t.List[tool_proxy_params.Parameter]] = None,
        custom_connection_data: t.Optional[
            tool_proxy_params.CustomConnectionData
        ] = None,
        user_id: t.Optional[str] = None,
    ) -> tool_proxy_response.ToolProxyResponse:
        """See :meth:`Tools.proxy`."""
        # Disable retries: a proxied call is a non-idempotent write, and a silent
        # retry after a read timeout can duplicate the side effect.
        return await self._client.without_retries.tools.proxy(
            endpoint=endpoint,
            method=method,
            body=body if body is not None else omit,
            connected_account_id=connected_account_id
            if connected_account_id is not None
            else omit,
            parameters=parameters if parameters is not None else omit,
            custom_connection_data=custom_connection_data
            if custom_connection_data is not None
            else omit,
            user_id=user_id if user_id is not None else omit,
        )


__all__ = ["AsyncAgenticProviderExecuteFn", "AsyncTools"]
