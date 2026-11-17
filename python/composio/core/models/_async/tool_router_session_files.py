"""Async file mount of a tool router session.

The Composio API calls are awaited natively. File bytes travel to and from
presigned and user-supplied URLs through the SDK's hardened ``requests`` path
(see :mod:`composio.utils.url_safety`), on a worker thread.
"""

from __future__ import annotations

import typing as t
from pathlib import Path

from composio_client import omit
from composio_client.types.tool_router.session.file_create_download_url_response import (
    FileCreateDownloadURLResponse,
)
from composio_client.types.tool_router.session.file_delete_response import (
    FileDeleteResponse,
)
from composio_client.types.tool_router.session.file_list_response import (
    FileListResponse,
)

from composio.client import AsyncHttpClient
from composio.core.models.tool_router_session_files import (
    DEFAULT_TOOL_ROUTER_SESSION_FILES_MOUNT_ID,
    RemoteFile,
    _mount_prefix_arg,
    _normalize_upload_input,
    _put_upload,
)
from composio.utils.async_bridge import run_io


class AsyncRemoteFile:
    """Async counterpart of :class:`~composio.core.models.tool_router_session_files.RemoteFile`."""

    def __init__(
        self,
        *,
        expires_at: str,
        mount_relative_path: str,
        sandbox_mount_prefix: str,
        download_url: str,
    ) -> None:
        self._file = RemoteFile(
            expires_at=expires_at,
            mount_relative_path=mount_relative_path,
            sandbox_mount_prefix=sandbox_mount_prefix,
            download_url=download_url,
        )

    @property
    def expires_at(self) -> str:
        return self._file.expires_at

    @property
    def mount_relative_path(self) -> str:
        return self._file.mount_relative_path

    @property
    def sandbox_mount_prefix(self) -> str:
        return self._file.sandbox_mount_prefix

    @property
    def download_url(self) -> str:
        return self._file.download_url

    @property
    def filename(self) -> str:
        """See :attr:`RemoteFile.filename`."""
        return self._file.filename

    async def buffer(self) -> bytes:
        """See :meth:`RemoteFile.buffer`."""
        return await run_io(self._file.buffer)

    async def text(self) -> str:
        """See :meth:`RemoteFile.text`."""
        return await run_io(self._file.text)

    async def save(self, path: t.Optional[t.Union[str, Path]] = None) -> str:
        """See :meth:`RemoteFile.save`."""
        return await run_io(self._file.save, path)

    @classmethod
    def from_api_response(
        cls, data: FileCreateDownloadURLResponse
    ) -> "AsyncRemoteFile":
        return cls(
            expires_at=data.expires_at,
            mount_relative_path=data.mount_relative_path,
            sandbox_mount_prefix=data.sandbox_mount_prefix,
            download_url=data.download_url,
        )


class AsyncToolRouterSessionFilesMount:
    """Async counterpart of :class:`~composio.core.models.tool_router_session_files.ToolRouterSessionFilesMount`."""

    def __init__(self, client: AsyncHttpClient, session_id: str) -> None:
        self._client = client
        self._session_id = session_id

    async def list(
        self,
        *,
        path: t.Optional[str] = None,
        mount_id: str = DEFAULT_TOOL_ROUTER_SESSION_FILES_MOUNT_ID,
        cursor: t.Optional[str] = None,
        limit: t.Optional[int] = None,
    ) -> FileListResponse:
        """See :meth:`ToolRouterSessionFilesMount.list`."""
        return await self._client.tool_router.session.files.list(
            mount_id,
            session_id=self._session_id,
            mount_relative_prefix=_mount_prefix_arg(path),
            cursor=cursor if cursor else omit,
            limit=int(limit) if limit is not None else omit,
        )

    async def upload(
        self,
        input: t.Union[str, Path, bytes, bytearray],
        *,
        remote_path: t.Optional[str] = None,
        mimetype: t.Optional[str] = None,
        mount_id: str = DEFAULT_TOOL_ROUTER_SESSION_FILES_MOUNT_ID,
    ) -> AsyncRemoteFile:
        """See :meth:`ToolRouterSessionFilesMount.upload`."""
        # Reads a local file or fetches a URL, so it leaves the loop.
        content, rpath, mime = await run_io(
            _normalize_upload_input, input, remote_path=remote_path, mimetype=mimetype
        )

        create_resp = await self._client.tool_router.session.files.create_upload_url(
            mount_id,
            session_id=self._session_id,
            mount_relative_path=rpath,
            mimetype=mime,
        )
        await run_io(_put_upload, create_resp.upload_url, content, mime)

        download_resp = (
            await self._client.tool_router.session.files.create_download_url(
                mount_id,
                session_id=self._session_id,
                mount_relative_path=create_resp.mount_relative_path,
            )
        )
        return AsyncRemoteFile.from_api_response(download_resp)

    async def download(
        self,
        file_path: str,
        *,
        mount_id: str = DEFAULT_TOOL_ROUTER_SESSION_FILES_MOUNT_ID,
    ) -> AsyncRemoteFile:
        """See :meth:`ToolRouterSessionFilesMount.download`."""
        resp = await self._client.tool_router.session.files.create_download_url(
            mount_id,
            session_id=self._session_id,
            mount_relative_path=file_path,
        )
        return AsyncRemoteFile.from_api_response(resp)

    async def delete(
        self,
        remote_path: str,
        *,
        mount_id: str = DEFAULT_TOOL_ROUTER_SESSION_FILES_MOUNT_ID,
    ) -> FileDeleteResponse:
        """See :meth:`ToolRouterSessionFilesMount.delete`."""
        return await self._client.tool_router.session.files.delete(
            mount_id,
            session_id=self._session_id,
            mount_relative_path=remote_path,
        )
