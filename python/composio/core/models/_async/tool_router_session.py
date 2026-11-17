"""Async tool router session.

Mirrors :mod:`composio.core.models.tool_router_session`: API calls are awaited,
and local tools (synchronous functions) run on worker threads.
"""

from __future__ import annotations

import asyncio
import typing as t

import typing_extensions as te
from composio_client import BadRequestError, ConflictError, Omit, omit
from composio_client._types import SequenceNotStr
from composio_client.types.tool_router import session_link_params, session_patch_params

from composio import exceptions
from composio.client import AsyncHttpClient
from composio.client.types import (
    session_config_history_params,
    session_config_history_response,
)
from composio.core.models._modifiers import Modifiers, apply_modifier_by_type
from composio.core.models.custom_tool_execution import (
    assert_unambiguous_custom_tool_slug,
    execute_custom_tool,
    find_custom_tool,
)
from composio.core.models.custom_tool_types import (
    CustomToolsMap,
    CustomToolsMapEntry,
    InlineCustomToolsWirePayload,
    ToolRouterSessionProxyExecuteResponse,
)
from composio.core.models.experimental import ACL_ONLY_FOR_SHARED_ERROR_FRAGMENT
from composio.core.models.tool_router_session import (
    COMPOSIO_MULTI_EXECUTE_TOOL,
    MAX_PARALLEL_WORKERS,
    ToolRouterHook,
    ToolRouterInputResponse,
    ToolRouterInstantConfig,
    ToolRouterProxyExecuteConfig,
    ToolRouterSessionConfig,
    ToolRouterSessionMetaToolSlug,
    ToolRouterSessionPreloadConfig,
    ToolRouterSessionSearchResponse,
    ToolRouterUpdateExperimentalConfig,
    ToolRouterUpdateManageConnectionsConfig,
    ToolRouterUpdateMultiAccountConfig,
    _local_execute_response,
    _toolkit_connections_details,
    _toolkits_query,
    _ToolRouterSessionBase,
)
from composio.core.models.tool_router_session_delete import (
    ToolRouterSessionDeleteResponse,
    delete_response,
)
from composio.core.models.tools import (
    ToolExecuteParams,
    ToolExecutionResponse,
    ToolRouterSessionExecuteResponse,
    require_execute_result,
)
from composio.core.provider import TTool, TToolCollection
from composio.core.provider.base import BaseProvider
from composio.utils.async_bridge import run_callback, sync_bridge

from .connected_accounts import AsyncConnectionRequest
from .session_context import AsyncSessionContextImpl, async_proxy_execute_impl

if t.TYPE_CHECKING:
    from composio.core.models.tool_router import (
        ToolkitConnectionsDetails,
        ToolRouterMCPServerConfig,
    )

    from .tool_router import AsyncToolRouterSessionExperimental

_AsyncExecuteFn = t.Callable[[str, t.Dict], t.Awaitable[t.Dict]]


class AsyncToolRouterSession(_ToolRouterSessionBase[TTool, TToolCollection]):
    """Async counterpart of :class:`~composio.core.models.tool_router_session.ToolRouterSession`.

    Returned by ``await composio.create(...)`` / ``await composio.use(...)`` on
    :class:`~composio.AsyncComposio`. Local tools stay synchronous functions and
    run on a worker thread; the ``ctx`` they receive blocks that thread while its
    calls run on the session's event loop.
    """

    session_id: str
    config: ToolRouterSessionConfig
    experimental: "AsyncToolRouterSessionExperimental"
    config_version: t.Optional[int]

    def __init__(
        self,
        *,
        client: AsyncHttpClient,
        provider: t.Optional[BaseProvider[t.Any, t.Any]],
        dangerously_allow_auto_upload_download_files: bool,
        sensitive_file_upload_protection: bool = True,
        file_upload_path_deny_segments: t.Optional[t.Sequence[str]] = None,
        file_upload_dirs: t.Union[t.Sequence[str], t.Literal[False], None] = None,
        session_id: str,
        mcp: t.Any,
        experimental: "AsyncToolRouterSessionExperimental",
        config: t.Optional[ToolRouterSessionConfig] = None,
        config_version: t.Optional[int] = None,
        custom_tools_map: t.Optional[CustomToolsMap] = None,
        user_id: t.Optional[str] = None,
        preload: t.Optional[ToolRouterSessionPreloadConfig] = None,
        preloaded_custom_tool_slugs: t.Optional[t.List[str]] = None,
        inline_custom_tools_payload: t.Optional[InlineCustomToolsWirePayload] = None,
    ) -> None:
        self._init_session(
            client=client,
            provider=provider,
            dangerously_allow_auto_upload_download_files=dangerously_allow_auto_upload_download_files,
            sensitive_file_upload_protection=sensitive_file_upload_protection,
            file_upload_path_deny_segments=file_upload_path_deny_segments,
            file_upload_dirs=file_upload_dirs,
            session_id=session_id,
            mcp=mcp,
            experimental=experimental,
            config=config,
            config_version=config_version,
            custom_tools_map=custom_tools_map,
            user_id=user_id,
            preload=preload,
            preloaded_custom_tool_slugs=preloaded_custom_tool_slugs,
            inline_custom_tools_payload=inline_custom_tools_payload,
        )
        self._session_context: t.Optional[AsyncSessionContextImpl] = None
        if custom_tools_map and user_id:
            self._session_context = AsyncSessionContextImpl(
                client=client,
                user_id=user_id,
                session_id=session_id,
                custom_tools_map=custom_tools_map,
                inline_custom_tools_payload=inline_custom_tools_payload,
            )

    async def _run_custom_tool(
        self, entry: CustomToolsMapEntry, arguments: t.Dict[str, t.Any]
    ) -> ToolExecutionResponse:
        """Run a local tool on a worker thread, its context bound to this loop."""
        ctx = self._session_context
        assert ctx is not None
        ctx.bind_loop(asyncio.get_running_loop())
        return await run_callback(execute_custom_tool, entry, arguments, ctx)

    def _backend_execute(
        self,
        tools_model: t.Any,
        modifiers: t.Optional[Modifiers] = None,
    ) -> _AsyncExecuteFn:
        return t.cast(
            _AsyncExecuteFn,
            tools_model._async_execute_tool_for_tool_router(
                session_id=self.session_id,
                modifiers=modifiers,
                inline_custom_tools_payload=self._inline_custom_tools_payload,
            ),
        )

    async def tools(self, modifiers: t.Optional[Modifiers] = None) -> TToolCollection:
        """See :meth:`ToolRouterSession.tools`.

        Agentic providers receive a blocking ``execute_tool`` bound to the
        running event loop, to be called from another thread.
        """
        from composio.core.provider import (
            AgenticProvider,
            NonAgenticProvider,
            bind_execution_target,
        )

        from .tools import AsyncTools

        if self._provider is None:
            raise ValueError(
                "Provider is required for tool router. "
                "Please initialize ToolRouter with a provider."
            )

        tools_model: AsyncTools[t.Any, t.Any] = AsyncTools(
            client=self._client,
            provider=self._provider,
            dangerously_allow_auto_upload_download_files=self._auto_upload_download_files,
            sensitive_file_upload_protection=self._sensitive_file_upload_protection,
            file_upload_path_deny_segments=self._file_upload_path_deny_segments,
            file_upload_dirs=self._file_upload_dirs,
        )

        router_tools = await tools_model.get_raw_tool_router_meta_tools(
            session_id=self.session_id,
            modifiers=modifiers,
        )
        router_tools = self._add_preloaded_custom_tools(router_tools, modifiers)

        for tool in router_tools:
            tool.input_parameters = (
                tools_model._file_helper.enhance_schema_descriptions(
                    schema=tool.input_parameters,
                )
            )

        if issubclass(type(self._provider), NonAgenticProvider):
            return t.cast(
                TToolCollection,
                t.cast(
                    NonAgenticProvider[TTool, TToolCollection], self._provider
                ).wrap_tools(tools=router_tools),
            )

        execute_fn: t.Callable[..., t.Any]
        if self._has_custom_tools():
            execute_fn = sync_bridge(self._routing_execute(tools_model, modifiers))
        else:
            execute_fn = tools_model._wrap_execute_tool_for_tool_router(
                session_id=self.session_id,
                modifiers=modifiers,
                inline_custom_tools_payload=self._inline_custom_tools_payload,
            )

        return t.cast(
            TToolCollection,
            t.cast(AgenticProvider[TTool, TToolCollection], self._provider).wrap_tools(
                tools=router_tools,
                execute_tool=bind_execution_target(execute_fn, t.cast(t.Any, self)),
            ),
        )

    def _routing_execute(
        self,
        tools_model: t.Any,
        modifiers: t.Optional[Modifiers],
    ) -> t.Callable[[str, t.Dict], t.Coroutine[t.Any, t.Any, t.Dict]]:
        """See :meth:`ToolRouterSession._create_routing_execute_fn`."""
        backend_execute = self._backend_execute(tools_model, modifiers=modifiers)

        async def routing_execute(slug: str, arguments: t.Dict) -> t.Dict:
            if slug == COMPOSIO_MULTI_EXECUTE_TOOL:
                processed_arguments = arguments
                if modifiers is not None:
                    type_before: t.Literal["before_execute"] = "before_execute"
                    params: ToolExecuteParams = {"arguments": arguments}
                    modified = apply_modifier_by_type(
                        modifiers=modifiers,
                        toolkit="composio",
                        tool=slug,
                        type=type_before,
                        request=params,
                    )
                    processed_arguments = modified.get("arguments", arguments)

                result = await self._route_multi_execute(
                    processed_arguments, tools_model
                )

                if modifiers is not None:
                    type_after: t.Literal["after_execute"] = "after_execute"
                    result = t.cast(
                        t.Dict[str, t.Any],
                        apply_modifier_by_type(
                            modifiers=modifiers,
                            toolkit="composio",
                            tool=slug,
                            type=type_after,
                            response=t.cast(ToolExecutionResponse, result),
                        ),
                    )

                return result
            entry = find_custom_tool(self._custom_tools_map, slug)
            if entry:
                return t.cast(
                    t.Dict[str, t.Any], await self._run_custom_tool(entry, arguments)
                )
            assert_unambiguous_custom_tool_slug(self._custom_tools_map, slug)
            return await backend_execute(slug, arguments)

        return routing_execute

    async def _route_multi_execute(
        self,
        input_args: t.Dict[str, t.Any],
        tools_model: t.Any,
    ) -> t.Dict[str, t.Any]:
        """See :meth:`ToolRouterSession._route_multi_execute`."""
        # No modifiers on the backend call: the caller applies them once.
        backend_execute = self._backend_execute(tools_model)
        split = self._split_multi_execute(input_args)
        if split is None:
            return await backend_execute(COMPOSIO_MULTI_EXECUTE_TOOL, input_args)

        workers = asyncio.Semaphore(MAX_PARALLEL_WORKERS)

        async def run_local(
            idx: int, entry: CustomToolsMapEntry
        ) -> t.Tuple[int, ToolExecutionResponse]:
            async with workers:
                return idx, await self._run_custom_tool(
                    entry, split.parsed[idx]["arguments"]
                )

        async def run_remote() -> t.Dict[str, t.Any]:
            remote_input = {
                **input_args,
                "tools": [split.tool_items[i] for i in split.remote_indices],
            }
            async with workers:
                return await backend_execute(COMPOSIO_MULTI_EXECUTE_TOOL, remote_input)

        # Every call runs to completion before any failure is raised: a
        # remote execution must not be cancelled midway by a local one.
        outcomes = await asyncio.gather(
            *(run_local(idx, entry) for idx, entry in split.local_items),
            *([run_remote()] if split.remote_indices else []),
            return_exceptions=True,
        )
        local_outcomes = outcomes[: len(split.local_items)]
        local_results: t.List[t.Tuple[int, ToolExecutionResponse]] = []
        for local_outcome in local_outcomes:
            if isinstance(local_outcome, BaseException):
                raise local_outcome
            local_results.append(
                t.cast(t.Tuple[int, ToolExecutionResponse], local_outcome)
            )

        # A transport failure of the remote batch must not discard completed
        # local results, so it becomes per-tool failures (matches TS).
        remote_result: t.Optional[t.Dict[str, t.Any]] = None
        remote_error_message: t.Optional[str] = None
        if split.remote_indices:
            remote_outcome = outcomes[-1]
            if isinstance(remote_outcome, exceptions.ToolInputRequiredError):
                # Not a per-tool failure: the caller needs `input_requests` and
                # `request_state`. Local results of this batch are discarded.
                raise remote_outcome
            if isinstance(remote_outcome, Exception):
                remote_error_message = (
                    str(remote_outcome) or "Remote tool execution failed"
                )
            elif isinstance(remote_outcome, BaseException):
                raise remote_outcome
            else:
                remote_result = t.cast(t.Dict[str, t.Any], remote_outcome)

        return self._merge_multi_execute(
            split, local_results, remote_result, remote_error_message
        )

    async def authorize(
        self,
        toolkit: str,
        *,
        callback_url: t.Optional[str] = None,
        alias: t.Optional[str] = None,
        experimental: t.Optional[session_link_params.Experimental] = None,
    ) -> AsyncConnectionRequest:
        """See :meth:`ToolRouterSession.authorize`."""
        try:
            response = await self._client.tool_router.session.link(
                session_id=self.session_id,
                toolkit=toolkit,
                callback_url=callback_url if callback_url else omit,
                alias=alias if alias is not None else omit,
                experimental=experimental if experimental is not None else omit,
            )
        except BadRequestError as error:
            message = str(error)
            if ACL_ONLY_FOR_SHARED_ERROR_FRAGMENT in message:
                raise exceptions.ComposioAclOnlyForSharedError(message) from error
            raise

        return AsyncConnectionRequest(
            id=response.connected_account_id,
            redirect_url=response.redirect_url,
            status="INITIATED",
            client=self._client,
        )

    async def toolkits(
        self,
        *,
        toolkits: t.Optional[t.List[str]] = None,
        next_cursor: t.Optional[str] = None,
        limit: t.Optional[int] = None,
        is_connected: t.Optional[bool] = None,
        search: t.Optional[str] = None,
    ) -> ToolkitConnectionsDetails:
        """See :meth:`ToolRouterSession.toolkits`."""
        return _toolkit_connections_details(
            await self._client.tool_router.session.toolkits(
                session_id=self.session_id,
                **_toolkits_query(
                    toolkits=toolkits,
                    next_cursor=next_cursor,
                    limit=limit,
                    is_connected=is_connected,
                    search=search,
                ),
            )
        )

    async def search(
        self,
        *,
        query: str,
        model: t.Optional[str] = None,
    ) -> ToolRouterSessionSearchResponse:
        """See :meth:`ToolRouterSession.search`."""
        return await self._client.tool_router.session.search(
            session_id=self.session_id,
            queries=[{"use_case": query}],
            model=model if model else omit,
            experimental=self._inline_custom_tools_payload
            if self._inline_custom_tools_payload is not None
            else omit,
        )

    async def execute(
        self,
        tool_slug: str,
        *,
        arguments: t.Optional[t.Dict[str, t.Any]] = None,
        account: t.Optional[str] = None,
        input_responses: t.Optional[t.Dict[str, ToolRouterInputResponse]] = None,
        request_state: t.Optional[str] = None,
    ) -> ToolRouterSessionExecuteResponse:
        """See :meth:`ToolRouterSession.execute`."""
        entry = find_custom_tool(self._custom_tools_map, tool_slug)
        if entry and self._session_context:
            return _local_execute_response(
                await self._run_custom_tool(entry, arguments or {})
            )

        assert_unambiguous_custom_tool_slug(self._custom_tools_map, tool_slug)

        # Disable retries: a session execution is a non-idempotent write, and a
        # silent retry after a read timeout can duplicate the side effect.
        return require_execute_result(
            await self._client.without_retries.tool_router.session.execute(
                session_id=self.session_id,
                tool_slug=tool_slug,
                arguments=arguments if arguments is not None else omit,
                account=account if account is not None else omit,
                input_responses=(
                    input_responses if input_responses is not None else omit
                ),
                request_state=request_state if request_state is not None else omit,
                experimental=self._inline_custom_tools_payload
                if self._inline_custom_tools_payload is not None
                else omit,
            ),
            tool_slug,
        )

    async def execute_meta(
        self,
        slug: ToolRouterSessionMetaToolSlug,
        *,
        arguments: t.Optional[t.Dict[str, t.Any]] = None,
        input_responses: t.Optional[t.Dict[str, ToolRouterInputResponse]] = None,
        request_state: t.Optional[str] = None,
    ) -> ToolRouterSessionExecuteResponse:
        """See :meth:`ToolRouterSession.execute_meta`."""
        # Disable retries: a session execution is a non-idempotent write, and a
        # silent retry after a read timeout can duplicate the side effect.
        return require_execute_result(
            await self._client.without_retries.tool_router.session.execute_meta(
                session_id=self.session_id,
                slug=slug,
                arguments=arguments if arguments is not None else omit,
                input_responses=(
                    input_responses if input_responses is not None else omit
                ),
                request_state=request_state if request_state is not None else omit,
                experimental=self._inline_custom_tools_payload
                if self._inline_custom_tools_payload is not None
                else omit,
            ),
            slug,
        )

    async def proxy_execute(
        self,
        *,
        toolkit: str,
        endpoint: str,
        method: t.Literal["GET", "POST", "PUT", "DELETE", "PATCH"],
        body: t.Any = None,
        parameters: t.Optional[t.List[t.Dict[str, t.Any]]] = None,
    ) -> ToolRouterSessionProxyExecuteResponse:
        """See :meth:`ToolRouterSession.proxy_execute`."""
        return await async_proxy_execute_impl(
            self._client,
            self.session_id,
            toolkit=toolkit,
            endpoint=endpoint,
            method=method,
            body=body,
            parameters=parameters,
        )

    async def update(
        self,
        *,
        toolkits: t.Union[t.Optional[session_patch_params.Toolkits], "Omit"] = omit,
        instant: t.Union[t.Literal[False], ToolRouterInstantConfig, "Omit"] = omit,
        tools: t.Union[
            t.Optional[t.Dict[str, session_patch_params.Tools]], "Omit"
        ] = omit,
        tags: t.Union[t.Optional[session_patch_params.Tags], "Omit"] = omit,
        auth_configs: t.Union[t.Optional[t.Dict[str, str]], "Omit"] = omit,
        connected_accounts: t.Union[
            t.Optional[t.Dict[str, SequenceNotStr[str]]], "Omit"
        ] = omit,
        manage_connections: t.Union[
            t.Optional[session_patch_params.ManageConnections],
            t.Optional[ToolRouterUpdateManageConnectionsConfig],
            "Omit",
        ] = omit,
        sandbox: t.Union[t.Optional[session_patch_params.Workbench], "Omit"] = omit,
        workbench: t.Union[t.Optional[session_patch_params.Workbench], "Omit"] = omit,
        multi_account: t.Union[
            t.Optional[session_patch_params.MultiAccount],
            t.Optional[ToolRouterUpdateMultiAccountConfig],
            "Omit",
        ] = omit,
        proxy_execute: t.Union[
            t.Optional[t.Union[bool, ToolRouterProxyExecuteConfig]], "Omit"
        ] = omit,
        hooks: t.Union[t.Optional[t.List[ToolRouterHook]], "Omit"] = omit,
        preload: t.Union[t.Optional[session_patch_params.Preload], "Omit"] = omit,
        search: t.Union[t.Optional[session_patch_params.Search], "Omit"] = omit,
        execute: t.Union[t.Optional[session_patch_params.Execute], "Omit"] = omit,
        experimental: t.Union[
            t.Optional[session_patch_params.Experimental],
            t.Optional[ToolRouterUpdateExperimentalConfig],
            "Omit",
        ] = omit,
        expected_config_version: t.Union[int, None, t.Literal[False]] = None,
    ) -> ToolRouterSessionConfig:
        """See :meth:`ToolRouterSession.update`."""
        patch, precondition = self._patch_request(
            toolkits=toolkits,
            instant=instant,
            tools=tools,
            tags=tags,
            auth_configs=auth_configs,
            connected_accounts=connected_accounts,
            manage_connections=manage_connections,
            sandbox=sandbox,
            workbench=workbench,
            multi_account=multi_account,
            proxy_execute=proxy_execute,
            hooks=hooks,
            preload=preload,
            search=search,
            execute=execute,
            experimental=experimental,
            expected_config_version=expected_config_version,
        )
        try:
            response = await self._client.tool_router.session.patch(**patch)
        except ConflictError as exc:
            raise self._patch_conflict(precondition) from exc
        return self._apply_patch(response)

    async def list_config_history(
        self,
        **query: te.Unpack[session_config_history_params.SessionConfigHistoryParams],
    ) -> session_config_history_response.SessionConfigHistoryResponse:
        """See :meth:`ToolRouterSession.list_config_history`."""
        return await self._client.tool_router.session.config_history(
            session_id=self.session_id,
            **query,
        )

    async def delete(self) -> ToolRouterSessionDeleteResponse:
        """See :meth:`ToolRouterSession.delete`."""
        return delete_response(
            await self._client.tool_router.session.delete(self.session_id)
        )


class AsyncToolRouterSessionWithMcp(AsyncToolRouterSession[TTool, TToolCollection]):
    """An :class:`AsyncToolRouterSession` whose hosted MCP endpoint is typed.

    Returned by ``create(..., mcp=True)`` / ``use(..., mcp=True)``. See
    https://docs.composio.dev/docs/sessions-via-mcp
    """

    mcp: "ToolRouterMCPServerConfig"
