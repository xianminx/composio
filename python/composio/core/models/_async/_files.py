"""Async file helper.

Requests to the Composio API (presigned upload URLs) are awaited on the async
client. The bytes themselves still move through the hardened ``requests`` path
in :mod:`composio.utils.url_safety` (address pinning, per-hop redirect
validation, size limits), on a worker thread: the schema walk, local file
reads, hashing and third-party transfers all run there, and only the presign
call hops back to the event loop.
"""

from __future__ import annotations

import asyncio
import typing as t
from pathlib import Path

from composio.client import AsyncHttpClient, HttpClient
from composio.client.types import Tool
from composio.core.models._files import BeforeFileUpload, FileHelper
from composio.utils.async_bridge import run_io, run_on_loop
from composio.utils.logging import WithLogger

if t.TYPE_CHECKING:
    from composio.core.models.tools import ToolExecutionResponse


class _LoopBoundFiles:
    def __init__(
        self, client: AsyncHttpClient, loop: asyncio.AbstractEventLoop
    ) -> None:
        self._client = client
        self._loop = loop

    def create_presigned_url(self, **params: t.Any) -> t.Any:
        return run_on_loop(
            self._loop, self._client.files.create_presigned_url(**params)
        )


class _LoopBoundClient:
    """The slice of ``HttpClient`` the sync upload path uses, backed by the
    async client on its loop. Only valid on a thread other than that loop's."""

    def __init__(
        self, client: AsyncHttpClient, loop: asyncio.AbstractEventLoop
    ) -> None:
        self.files = _LoopBoundFiles(client, loop)


class AsyncFileHelper(WithLogger):
    """Async counterpart of :class:`~composio.core.models._files.FileHelper`.

    A ``before_file_upload`` hook is called on the worker thread that stages
    the upload, not on the event loop.
    """

    def __init__(
        self,
        client: AsyncHttpClient,
        outdir: t.Optional[str] = None,
        *,
        sensitive_file_upload_protection: bool = True,
        file_upload_path_deny_segments: t.Optional[t.Sequence[str]] = None,
        file_upload_allowlist: t.Optional[t.Sequence[Path]] = None,
    ) -> None:
        super().__init__()
        self._client = client
        self._outdir = outdir
        self._options: t.Dict[str, t.Any] = {
            "sensitive_file_upload_protection": sensitive_file_upload_protection,
            "file_upload_path_deny_segments": file_upload_path_deny_segments,
            "file_upload_allowlist": file_upload_allowlist,
        }
        # Never reaches the API: serves the schema-only and download methods.
        self._offline = self._helper(t.cast(HttpClient, None))

    def _helper(self, client: HttpClient) -> FileHelper:
        return FileHelper(client=client, outdir=self._outdir, **self._options)

    def enhance_schema_descriptions(self, schema: t.Dict) -> t.Dict:
        return self._offline.enhance_schema_descriptions(schema=schema)

    def process_file_uploadable_schema(self, schema: t.Dict) -> t.Dict:
        return self._offline.process_file_uploadable_schema(schema=schema)

    def process_schema_recursively(self, schema: t.Dict) -> t.Dict:
        return self._offline.process_schema_recursively(schema=schema)

    def drop_empty_file_uploads(self, tool: Tool, request: t.Dict) -> t.Dict:
        return self._offline.drop_empty_file_uploads(tool=tool, request=request)

    async def substitute_file_uploads(
        self,
        tool: Tool,
        request: t.Dict,
        *,
        before_file_upload: t.Optional[BeforeFileUpload] = None,
    ) -> t.Dict:
        """See :meth:`FileHelper.substitute_file_uploads`."""
        helper = self._helper(
            t.cast(
                HttpClient,
                _LoopBoundClient(self._client, asyncio.get_running_loop()),
            )
        )
        return await run_io(
            helper.substitute_file_uploads,
            tool=tool,
            request=request,
            before_file_upload=before_file_upload,
        )

    async def substitute_file_downloads(
        self,
        tool: Tool,
        response: ToolExecutionResponse,
    ) -> ToolExecutionResponse:
        """See :meth:`FileHelper.substitute_file_downloads`."""
        return await run_io(
            self._offline.substitute_file_downloads, tool=tool, response=response
        )
