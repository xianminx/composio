"""AsyncToolRouterSessionFilesMount and AsyncRemoteFile."""

import asyncio
import json
import threading
import typing as t
from unittest.mock import MagicMock, patch

import httpx
import pytest
import requests
from composio_client import ProjectAuth

from composio.client import AsyncHttpClient
from composio.core.models._async.tool_router_session_files import (
    AsyncRemoteFile,
    AsyncToolRouterSessionFilesMount,
)
from composio.exceptions import (
    BlockedInternalUrlError,
    RemoteFileDownloadError,
    ValidationError,
)

MODULE = "composio.core.models.tool_router_session_files"
SAFE_REQUEST = f"{MODULE}.safe_request"
SAFE_GET = f"{MODULE}.safe_get"
MOUNT = "/api/v3.1/tool_router/session/session_123/mounts/files"


def stream_response(content: bytes = b"file content", status_code: int = 200):
    response = MagicMock()
    response.status_code = status_code
    response.ok = 200 <= status_code < 300
    response.reason = "OK" if response.ok else "Not Found"
    response.headers = {"content-type": "text/plain"}
    response.iter_content = lambda chunk_size: [content]
    return response


class Backend:
    def __init__(self) -> None:
        self.requests: t.List[httpx.Request] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        path = request.url.path
        if path.endswith("/upload_url"):
            return httpx.Response(
                200,
                json={
                    "upload_url": "https://s3.example.com/upload",
                    "mount_relative_path": "test.txt",
                    "expires_at": "2026-01-01T00:00:00Z",
                    "sandbox_mount_prefix": "/mnt/files",
                },
            )
        if path.endswith("/download_url"):
            return httpx.Response(
                200,
                json={
                    "download_url": "https://s3.example.com/download",
                    "mount_relative_path": "out/test.txt",
                    "expires_at": "2026-01-01T00:00:00Z",
                    "sandbox_mount_prefix": "/mnt/files",
                },
            )
        if path.endswith("/delete"):
            return httpx.Response(
                200, json={"mount_relative_path": "test.txt", "deleted": True}
            )
        return httpx.Response(200, json={"items": [], "next_cursor": None})

    def body(self, suffix: str) -> t.Dict[str, t.Any]:
        (request,) = [r for r in self.requests if r.url.path.endswith(suffix)]
        return json.loads(request.content)


@pytest.fixture
def backend() -> Backend:
    return Backend()


@pytest.fixture
def mount(backend: Backend) -> AsyncToolRouterSessionFilesMount:
    client = AsyncHttpClient(
        provider="openai",
        auth=ProjectAuth("ak_test"),
        base_url="https://backend.composio.dev",
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(backend)),
    )
    return AsyncToolRouterSessionFilesMount(client, "session_123")


def _remote_file(mount_relative_path: str = "out/report.txt") -> AsyncRemoteFile:
    return AsyncRemoteFile(
        expires_at="2026-01-01T00:00:00Z",
        mount_relative_path=mount_relative_path,
        sandbox_mount_prefix="/mnt/files",
        download_url="https://s3.example.com/download",
    )


class TestMount:
    def test_list_root_omits_the_prefix(self, mount, backend):
        asyncio.run(mount.list())
        (request,) = backend.requests
        assert request.url.path == f"{MOUNT}/items"
        assert "mount_relative_prefix" not in request.url.params

    def test_list_with_path_and_pagination(self, mount, backend):
        asyncio.run(mount.list(path="/reports", cursor="c1", limit=10))
        params = backend.requests[0].url.params
        assert params["mount_relative_prefix"] == "reports"
        assert params["cursor"] == "c1"
        assert params["limit"] == "10"

    def test_upload_bytes_puts_through_the_guard_off_the_loop(self, mount, backend):
        put_threads: t.List[threading.Thread] = []

        def fake_put(method, url, **kwargs):
            put_threads.append(threading.current_thread())
            assert (method, url) == ("PUT", "https://s3.example.com/upload")
            assert kwargs["data"] == b"hello"
            assert kwargs["headers"] == {"Content-Type": "text/plain"}
            return stream_response()

        async def run():
            with patch(SAFE_REQUEST, side_effect=fake_put):
                file = await mount.upload(
                    b"hello", remote_path="test.txt", mimetype="text/plain"
                )
            return file, threading.current_thread()

        file, loop_thread = asyncio.run(run())
        assert isinstance(file, AsyncRemoteFile)
        assert file.download_url == "https://s3.example.com/download"
        assert file.mount_relative_path == "out/test.txt"
        assert file.filename == "test.txt"
        assert put_threads and put_threads[0] is not loop_thread
        assert backend.body("/upload_url") == {
            "mount_relative_path": "test.txt",
            "mimetype": "text/plain",
        }
        assert backend.body("/download_url") == {"mount_relative_path": "test.txt"}

    def test_upload_local_file(self, mount, backend, tmp_path):
        source = tmp_path / "notes.txt"
        source.write_text("notes")
        with patch(SAFE_REQUEST, return_value=stream_response()) as put:
            asyncio.run(mount.upload(source))
        assert put.call_args.kwargs["data"] == b"notes"
        assert backend.body("/upload_url")["mount_relative_path"] == "notes.txt"

    def test_upload_bytes_requires_mimetype_or_remote_path(self, mount, backend):
        with pytest.raises(ValidationError, match="mimetype or remote_path"):
            asyncio.run(mount.upload(b"data"))
        assert backend.requests == []

    def test_upload_failure_is_a_validation_error(self, mount):
        with patch(SAFE_REQUEST, side_effect=requests.exceptions.Timeout("slow")):
            with pytest.raises(ValidationError, match="Failed to upload file: Timeout"):
                asyncio.run(mount.upload(b"x", remote_path="a.txt"))

    def test_upload_rejected_by_storage_is_a_validation_error(self, mount, backend):
        with patch(SAFE_REQUEST, return_value=stream_response(status_code=404)):
            with pytest.raises(ValidationError, match="404 Not Found"):
                asyncio.run(mount.upload(b"x", remote_path="a.txt"))
        assert not any(r.url.path.endswith("/download_url") for r in backend.requests)

    def test_upload_from_blocked_url_sends_nothing(self, mount, backend):
        with patch(SAFE_REQUEST) as put:
            with pytest.raises(BlockedInternalUrlError):
                asyncio.run(mount.upload("http://169.254.169.254/latest/meta-data"))
        put.assert_not_called()
        assert backend.requests == []

    def test_download_and_delete(self, mount, backend):
        file = asyncio.run(mount.download("out/test.txt"))
        assert isinstance(file, AsyncRemoteFile)
        assert backend.body("/download_url") == {"mount_relative_path": "out/test.txt"}
        asyncio.run(mount.delete("test.txt"))
        assert backend.body("/delete") == {"mount_relative_path": "test.txt"}


class TestAsyncRemoteFile:
    def test_buffer_and_text_fetch_through_the_guard(self):
        with patch(SAFE_GET, return_value=stream_response(b"hello")) as get:
            assert asyncio.run(_remote_file().buffer()) == b"hello"
            assert asyncio.run(_remote_file().text()) == "hello"
        assert get.call_args.args == ("https://s3.example.com/download",)

    def test_buffer_failure_raises_remote_file_download_error(self):
        with patch(SAFE_GET, return_value=stream_response(status_code=404)):
            with pytest.raises(RemoteFileDownloadError) as raised:
                asyncio.run(_remote_file().buffer())
        assert raised.value.status_code == 404
        assert raised.value.filename == "report.txt"

    def test_buffer_blocked_url_never_reaches_the_network(self):
        file = AsyncRemoteFile(
            expires_at="",
            mount_relative_path="a.txt",
            sandbox_mount_prefix="",
            download_url="http://127.0.0.1/secret",
        )
        with patch("composio.utils.url_safety.requests.Session.request") as request:
            with pytest.raises(BlockedInternalUrlError):
                asyncio.run(file.buffer())
        request.assert_not_called()

    def test_save_to_explicit_path(self, tmp_path):
        target = tmp_path / "nested" / "saved.txt"
        with patch(SAFE_GET, return_value=stream_response(b"saved")):
            saved = asyncio.run(_remote_file().save(target))
        assert saved == str(target.resolve())
        assert target.read_bytes() == b"saved"

    def test_default_save_stays_inside_the_composio_files_dir(self, tmp_path):
        with (
            patch(SAFE_GET, return_value=stream_response(b"x")),
            patch(f"{MODULE}.Path.home", return_value=tmp_path),
        ):
            saved = asyncio.run(_remote_file("../../evil/report.txt").save())
        assert saved == str((tmp_path / ".composio" / "files" / "report.txt").resolve())

    def test_default_save_rejects_an_unusable_basename(self, tmp_path):
        with (
            patch(SAFE_GET, return_value=stream_response(b"x")),
            patch(f"{MODULE}.Path.home", return_value=tmp_path),
        ):
            with pytest.raises(ValidationError):
                asyncio.run(_remote_file("..").save())
        assert not (tmp_path / ".composio").exists()
