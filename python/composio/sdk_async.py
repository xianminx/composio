from __future__ import annotations

import os
import typing as t

import httpx
import typing_extensions as te

from composio.client import DEFAULT_MAX_RETRIES, AsyncHttpClient
from composio.core.models._async import (
    AsyncAuthConfigs,
    AsyncConnectedAccounts,
    AsyncExperimentalAPI,
    AsyncKeyring,
    AsyncLogs,
    AsyncMCP,
    AsyncToolkits,
    AsyncToolRouter,
    AsyncTools,
    AsyncTriggers,
    AsyncWebhooks,
)
from composio.core.models.base import allow_tracking
from composio.core.provider import TTool, TToolCollection
from composio.core.provider._openai import (
    OpenAIProvider,
    OpenAITool,
    OpenAIToolCollection,
)
from composio.core.provider.base import BaseProvider
from composio.sdk import SDKConfig, _custom_headers, _resolve_auth
from composio.utils.logging import WithLogger
from composio.utils.toolkit_version import get_toolkit_versions


class AsyncSDKConfig(SDKConfig):
    """:class:`SDKConfig` for :class:`AsyncComposio`: ``http_client`` is an
    ``httpx.AsyncClient``."""

    http_client: te.NotRequired[httpx.AsyncClient]  # type: ignore[misc]


class AsyncComposio(t.Generic[TTool, TToolCollection], WithLogger):
    """
    Composio SDK for Python, for ``asyncio``.

    The same surface as :class:`Composio` with every method that reaches the
    Composio API a coroutine, sent over one ``httpx.AsyncClient``::

        async with AsyncComposio() as composio:
            session = await composio.create(user_id="user@example.com")
            tools = await session.tools()
            result = await composio.tools.execute(
                "GITHUB_GET_THE_AUTHENTICATED_USER", {}, user_id="user@example.com"
            )

    It takes the keyword arguments :class:`Composio` takes, and reads the same
    environment variables; ``http_client`` is an ``httpx.AsyncClient``.

    Three things are not coroutines end to end, and never block the event loop:

    - File bytes moved to or from a presigned or third-party URL go through
      the SDK's SSRF-guarded transfer code on a worker thread.
    - The realtime trigger connection runs on its websocket thread and
      delivers events to handlers on the event loop.
    - Tools wrapped for an agentic provider, and custom tools, are synchronous
      callbacks. They hand the execution to the event loop and wait for it, so
      they have to be invoked off the loop thread. Providers that run tools on
      a worker thread (OpenAI Agents, Claude Agent SDK, AutoGen) already do; a
      framework that calls a synchronous tool inline on the loop gets
      :class:`~composio.utils.async_bridge.SyncCallOnEventLoopError`.
    """

    tools: "AsyncTools[TTool, TToolCollection]"

    @t.overload
    def __init__(
        self: "AsyncComposio[OpenAITool, OpenAIToolCollection]",
        provider: None = None,
        **kwargs: te.Unpack[AsyncSDKConfig],
    ) -> None: ...

    @t.overload
    def __init__(
        self,
        provider: BaseProvider[TTool, TToolCollection],
        **kwargs: te.Unpack[AsyncSDKConfig],
    ) -> None: ...

    def __init__(
        self,
        provider: t.Optional[BaseProvider[t.Any, t.Any]] = None,
        **kwargs: te.Unpack[AsyncSDKConfig],
    ) -> None:
        """See :meth:`Composio.__init__`."""
        logger = kwargs.get("logger")
        logging_level = kwargs.get("logging_level")
        WithLogger.__init__(self, logger=logger, logging_level=logging_level)
        auth, scope = _resolve_auth(kwargs)

        # Own provider per instance, as in `Composio` (issue #4369).
        actual_provider: BaseProvider[TTool, TToolCollection] = t.cast(
            BaseProvider[TTool, TToolCollection],
            provider if provider is not None else OpenAIProvider(),
        )
        toolkit_versions = get_toolkit_versions(kwargs.get("toolkit_versions"))

        allow_tracking.set(kwargs.get("allow_tracking", True))
        self._client = AsyncHttpClient(
            environment=kwargs.get("environment", "production"),
            provider=actual_provider.name,
            auth=auth,
            scope=scope,
            base_url=kwargs.get("base_url") or os.environ.get("COMPOSIO_BASE_URL"),
            timeout=kwargs.get("timeout"),
            max_retries=kwargs.get("max_retries", DEFAULT_MAX_RETRIES),
            default_headers=_custom_headers(),
            http_client=kwargs.get("http_client"),
            logger=logger,
            logging_level=logging_level,
        )
        self._client.allow_tracking = kwargs.get("allow_tracking", True)
        self.provider = actual_provider
        file_options: t.Dict[str, t.Any] = {
            "dangerously_allow_auto_upload_download_files": kwargs.get(
                "dangerously_allow_auto_upload_download_files", False
            ),
            "sensitive_file_upload_protection": kwargs.get(
                "sensitive_file_upload_protection", True
            ),
            "file_upload_path_deny_segments": kwargs.get(
                "file_upload_path_deny_segments"
            ),
            "file_upload_dirs": kwargs.get("file_upload_dirs"),
        }
        self.tools = AsyncTools(
            client=self._client,
            provider=actual_provider,
            file_download_dir=kwargs.get("file_download_dir"),
            toolkit_versions=toolkit_versions,
            **file_options,
        )
        self.toolkits = AsyncToolkits(client=self._client)
        self.triggers = AsyncTriggers(
            client=self._client, toolkit_versions=toolkit_versions
        )
        self.auth_configs = AsyncAuthConfigs(client=self._client)
        self.connected_accounts = AsyncConnectedAccounts(client=self._client)
        self.mcp = AsyncMCP(client=self._client)
        self.webhooks = AsyncWebhooks(client=self._client)
        self.logs = AsyncLogs(client=self._client)
        self.keyring = AsyncKeyring(client=self._client)
        self.experimental = AsyncExperimentalAPI(client=self._client)
        self.custom_toolkits = self.experimental.custom_toolkits

        self._sessions: AsyncToolRouter[TTool, TToolCollection] = AsyncToolRouter(
            client=self._client,
            provider=actual_provider,
            **file_options,
        )
        self.create = self._sessions.create
        self.use = self._sessions.use

    @property
    def sessions(self) -> AsyncToolRouter[TTool, TToolCollection]:
        """See :attr:`Composio.sessions`."""
        return self._sessions

    @property
    def client(self) -> AsyncHttpClient:
        return self._client

    async def aclose(self) -> None:
        """Close the underlying HTTP client and its connections."""
        await self._client.close()

    async def __aenter__(self) -> te.Self:
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.aclose()
