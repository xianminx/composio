"""Async tool router: creates and attaches to sessions on an async client."""

from __future__ import annotations

import typing as t
from dataclasses import dataclass

from composio_client import omit
from composio_client.types.tool_router.session_attach_response import (
    SessionAttachResponse,
)
from composio_client.types.tool_router.session_retrieve_response import (
    SessionRetrieveResponse,
)

from composio.client import AsyncHttpClient
from composio.core.models.base import AsyncResource
from composio.core.models.custom_tool import ExperimentalToolkit, LocalToolkit
from composio.core.models.custom_tool_types import CustomTool, LocalTool
from composio.core.models.tool_router import (
    SessionPreset,
    ToolRouterConfigTags,
    ToolRouterExperimentalConfig,
    ToolRouterManageConnectionsConfig,
    ToolRouterMultiAccountConfig,
    ToolRouterPreloadConfig,
    ToolRouterSandboxConfig,
    ToolRouterToolkitsDisableConfig,
    ToolRouterToolkitsEnableConfig,
    ToolRouterToolkitsRequireApprovalConfig,
    ToolRouterToolsConfig,
    ToolRouterWorkbenchConfig,
    _ToolRouterBase,
)
from composio.core.models.tool_router_session import (
    ToolRouterHook,
    ToolRouterInstantConfig,
    ToolRouterProxyExecuteConfig,
)
from composio.core.models.tool_router_session_delete import (
    ToolRouterSessionDeleteResponse,
    delete_response,
)
from composio.core.provider import TTool, TToolCollection
from composio.core.provider.base import BaseProvider

from .tool_router_session import AsyncToolRouterSession, AsyncToolRouterSessionWithMcp
from .tool_router_session_files import AsyncToolRouterSessionFilesMount


@dataclass
class AsyncToolRouterSessionExperimental:
    """Async counterpart of :class:`~composio.core.models.tool_router.ToolRouterSessionExperimental`."""

    files: AsyncToolRouterSessionFilesMount
    assistive_prompt: t.Optional[str] = None


class AsyncToolRouter(AsyncResource, _ToolRouterBase[TTool, TToolCollection]):
    """Async counterpart of :class:`~composio.core.models.tool_router.ToolRouter`.

    Surfaced as ``composio.sessions`` on :class:`~composio.AsyncComposio`::

        session = await composio.sessions.create(user_id="user_123")
        tools = await session.tools()
    """

    def __init__(
        self,
        client: AsyncHttpClient,
        provider: t.Optional["BaseProvider[TTool, TToolCollection]"] = None,
        dangerously_allow_auto_upload_download_files: bool = False,
        sensitive_file_upload_protection: bool = True,
        file_upload_path_deny_segments: t.Optional[t.Sequence[str]] = None,
        file_upload_dirs: t.Union[t.Sequence[str], t.Literal[False], None] = None,
    ):
        """See :meth:`ToolRouter.__init__`."""
        super().__init__(client)
        self._init_router(
            provider=provider,
            dangerously_allow_auto_upload_download_files=dangerously_allow_auto_upload_download_files,
            sensitive_file_upload_protection=sensitive_file_upload_protection,
            file_upload_path_deny_segments=file_upload_path_deny_segments,
            file_upload_dirs=file_upload_dirs,
        )

    @t.overload
    async def create(
        self,
        *,
        user_id: str,
        toolkits: t.Optional[
            t.Union[
                t.List[str],
                ToolRouterToolkitsEnableConfig,
                ToolRouterToolkitsDisableConfig,
                ToolRouterToolkitsRequireApprovalConfig,
            ]
        ] = None,
        tools: t.Optional[t.Dict[str, ToolRouterToolsConfig]] = None,
        tags: t.Optional[ToolRouterConfigTags] = None,
        manage_connections: t.Optional[
            t.Union[bool, ToolRouterManageConnectionsConfig]
        ] = None,
        auth_configs: t.Optional[t.Dict[str, str]] = None,
        connected_accounts: t.Optional[t.Dict[str, t.Union[str, t.List[str]]]] = None,
        sandbox: t.Optional[ToolRouterSandboxConfig] = None,
        workbench: t.Optional[ToolRouterWorkbenchConfig] = None,
        multi_account: t.Optional[ToolRouterMultiAccountConfig] = None,
        proxy_execute: t.Optional[t.Union[bool, ToolRouterProxyExecuteConfig]] = None,
        hooks: t.Optional[t.List[ToolRouterHook]] = None,
        preload: t.Optional[ToolRouterPreloadConfig] = None,
        session_preset: t.Optional[SessionPreset] = None,
        experimental: t.Optional[ToolRouterExperimentalConfig] = None,
        instant: t.Optional[t.Union[t.Literal[False], ToolRouterInstantConfig]] = None,
        mcp: t.Literal[True],
    ) -> AsyncToolRouterSessionWithMcp[TTool, TToolCollection]: ...

    @t.overload
    async def create(
        self,
        *,
        user_id: str,
        toolkits: t.Optional[
            t.Union[
                t.List[str],
                ToolRouterToolkitsEnableConfig,
                ToolRouterToolkitsDisableConfig,
                ToolRouterToolkitsRequireApprovalConfig,
            ]
        ] = None,
        tools: t.Optional[t.Dict[str, ToolRouterToolsConfig]] = None,
        tags: t.Optional[ToolRouterConfigTags] = None,
        manage_connections: t.Optional[
            t.Union[bool, ToolRouterManageConnectionsConfig]
        ] = None,
        auth_configs: t.Optional[t.Dict[str, str]] = None,
        connected_accounts: t.Optional[t.Dict[str, t.Union[str, t.List[str]]]] = None,
        sandbox: t.Optional[ToolRouterSandboxConfig] = None,
        workbench: t.Optional[ToolRouterWorkbenchConfig] = None,
        multi_account: t.Optional[ToolRouterMultiAccountConfig] = None,
        proxy_execute: t.Optional[t.Union[bool, ToolRouterProxyExecuteConfig]] = None,
        hooks: t.Optional[t.List[ToolRouterHook]] = None,
        preload: t.Optional[ToolRouterPreloadConfig] = None,
        session_preset: t.Optional[SessionPreset] = None,
        experimental: t.Optional[ToolRouterExperimentalConfig] = None,
        instant: t.Optional[t.Union[t.Literal[False], ToolRouterInstantConfig]] = None,
        mcp: t.Literal[False] = False,
    ) -> AsyncToolRouterSession[TTool, TToolCollection]: ...

    async def create(
        self,
        *,
        user_id: str,
        toolkits: t.Optional[
            t.Union[
                t.List[str],
                ToolRouterToolkitsEnableConfig,
                ToolRouterToolkitsDisableConfig,
                ToolRouterToolkitsRequireApprovalConfig,
            ]
        ] = None,
        tools: t.Optional[t.Dict[str, ToolRouterToolsConfig]] = None,
        tags: t.Optional[ToolRouterConfigTags] = None,
        manage_connections: t.Optional[
            t.Union[bool, ToolRouterManageConnectionsConfig]
        ] = None,
        auth_configs: t.Optional[t.Dict[str, str]] = None,
        connected_accounts: t.Optional[t.Dict[str, t.Union[str, t.List[str]]]] = None,
        sandbox: t.Optional[ToolRouterSandboxConfig] = None,
        workbench: t.Optional[ToolRouterWorkbenchConfig] = None,
        multi_account: t.Optional[ToolRouterMultiAccountConfig] = None,
        proxy_execute: t.Optional[t.Union[bool, ToolRouterProxyExecuteConfig]] = None,
        hooks: t.Optional[t.List[ToolRouterHook]] = None,
        preload: t.Optional[ToolRouterPreloadConfig] = None,
        session_preset: t.Optional[SessionPreset] = None,
        experimental: t.Optional[ToolRouterExperimentalConfig] = None,
        instant: t.Optional[t.Union[t.Literal[False], ToolRouterInstantConfig]] = None,
        mcp: bool = False,
    ) -> AsyncToolRouterSession[TTool, TToolCollection]:
        """See :meth:`ToolRouter.create`."""
        plan = self._plan_create(
            user_id=user_id,
            toolkits=toolkits,
            tools=tools,
            tags=tags,
            manage_connections=manage_connections,
            auth_configs=auth_configs,
            connected_accounts=connected_accounts,
            sandbox=sandbox,
            workbench=workbench,
            multi_account=multi_account,
            proxy_execute=proxy_execute,
            hooks=hooks,
            preload=preload,
            session_preset=session_preset,
            experimental=experimental,
            instant=instant,
        )
        session = await self._client.tool_router.session.create(**plan.params)

        session_cls: t.Type[AsyncToolRouterSession[TTool, TToolCollection]] = (
            AsyncToolRouterSessionWithMcp if mcp else AsyncToolRouterSession
        )
        return session_cls(
            client=self._client,
            experimental=AsyncToolRouterSessionExperimental(
                files=AsyncToolRouterSessionFilesMount(
                    self._client, session.session_id
                ),
                assistive_prompt=(
                    session.experimental.assistive_prompt
                    if session.experimental
                    else None
                ),
            ),
            **self._created_session_kwargs(session, plan, user_id=user_id, mcp=mcp),
        )

    @t.overload
    async def use(
        self,
        session_id: str,
        *,
        local_tools: t.Optional[t.List[LocalTool]] = None,
        local_toolkits: t.Optional[t.List[LocalToolkit]] = None,
        custom_tools: t.Optional[t.List[CustomTool]] = None,
        custom_toolkits: t.Optional[t.List[ExperimentalToolkit]] = None,
        mcp: t.Literal[True],
    ) -> AsyncToolRouterSessionWithMcp[TTool, TToolCollection]: ...

    @t.overload
    async def use(
        self,
        session_id: str,
        *,
        local_tools: t.Optional[t.List[LocalTool]] = None,
        local_toolkits: t.Optional[t.List[LocalToolkit]] = None,
        custom_tools: t.Optional[t.List[CustomTool]] = None,
        custom_toolkits: t.Optional[t.List[ExperimentalToolkit]] = None,
        mcp: t.Literal[False] = False,
    ) -> AsyncToolRouterSession[TTool, TToolCollection]: ...

    async def use(
        self,
        session_id: str,
        *,
        local_tools: t.Optional[t.List[LocalTool]] = None,
        local_toolkits: t.Optional[t.List[LocalToolkit]] = None,
        custom_tools: t.Optional[t.List[CustomTool]] = None,
        custom_toolkits: t.Optional[t.List[ExperimentalToolkit]] = None,
        mcp: bool = False,
    ) -> AsyncToolRouterSession[TTool, TToolCollection]:
        """See :meth:`ToolRouter.use`."""
        use_plan = self._plan_use(
            local_tools=local_tools,
            local_toolkits=local_toolkits,
            custom_tools=custom_tools,
            custom_toolkits=custom_toolkits,
        )
        session: t.Union[SessionAttachResponse, SessionRetrieveResponse]
        if use_plan.has_customs:
            session = await self._client.tool_router.session.attach(
                session_id,
                experimental=use_plan.attach_inline_custom_tools_payload
                if use_plan.attach_inline_custom_tools_payload is not None
                else omit,
            )
        else:
            session = await self._client.tool_router.session.retrieve(session_id)

        session_cls: t.Type[AsyncToolRouterSession[TTool, TToolCollection]] = (
            AsyncToolRouterSessionWithMcp if mcp else AsyncToolRouterSession
        )
        return session_cls(
            client=self._client,
            experimental=AsyncToolRouterSessionExperimental(
                files=AsyncToolRouterSessionFilesMount(
                    self._client, session.session_id
                ),
                assistive_prompt=None,
            ),
            **self._used_session_kwargs(session, use_plan, mcp=mcp),
        )

    async def delete(self, session_id: str) -> ToolRouterSessionDeleteResponse:
        """See :meth:`ToolRouter.delete`."""
        return delete_response(
            await self._client.tool_router.session.delete(session_id)
        )


__all__ = [
    "AsyncToolRouter",
    "AsyncToolRouterSession",
    "AsyncToolRouterSessionExperimental",
    "AsyncToolRouterSessionWithMcp",
]
