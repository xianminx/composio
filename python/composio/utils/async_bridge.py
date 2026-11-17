"""Calling the async SDK from synchronous callbacks.

Agent frameworks and custom tools invoke tool callbacks synchronously. Under
:class:`~composio.AsyncComposio` the execution behind such a callback is a
coroutine bound to the SDK's event loop, so the callback hands it to that loop
and blocks its own (worker) thread on the result.
"""

from __future__ import annotations

import asyncio
import contextvars
import functools
import threading
import typing as t
from concurrent.futures import ThreadPoolExecutor

from composio.exceptions import ComposioError

T = t.TypeVar("T")


class SyncCallOnEventLoopError(ComposioError):
    """A synchronous tool callback was invoked on the SDK's event loop thread."""

    def __init__(self) -> None:
        super().__init__(
            "A synchronous Composio tool callback was called on the event loop "
            "that AsyncComposio runs on, which would block the loop on its own "
            "result. Invoke the tool through the framework's async entrypoint, "
            "run it in a worker thread (`asyncio.to_thread`), or `await` the "
            "SDK method (`composio.tools.execute` / `session.execute`) directly."
        )


class EventLoopNotRunningError(ComposioError):
    """A synchronous tool callback outlived the event loop it is bound to."""

    def __init__(self) -> None:
        super().__init__(
            "This Composio tool callback is bound to the event loop it was "
            "created on, and that loop is no longer running. Fetch the tools "
            "and invoke them inside the same running loop (for example within "
            "one `asyncio.run(main())`)."
        )


_MAX_WORKERS = 32
_executors: t.Dict[str, ThreadPoolExecutor] = {}
_executors_lock = threading.Lock()


def _executor(name: str) -> ThreadPoolExecutor:
    with _executors_lock:
        executor = _executors.get(name)
        if executor is None:
            executor = _executors[name] = ThreadPoolExecutor(
                max_workers=_MAX_WORKERS, thread_name_prefix=f"composio-{name}"
            )
        return executor


async def _run(name: str, fn: t.Callable[..., T], *args: t.Any, **kwargs: t.Any) -> T:
    call = functools.partial(contextvars.copy_context().run, fn, *args, **kwargs)
    return await asyncio.get_running_loop().run_in_executor(_executor(name), call)


# The SDK's own worker threads, kept off the loop's default executor: a
# framework that offloads tool callbacks there can fill it with threads blocked
# in `run_on_loop`, and work the loop then queued behind them would never run.
# Two pools for the same reason one level down. A callback thread (a custom
# tool) can wait on the loop for work that needs an IO thread; an IO thread
# only ever waits on the loop for an API request, so IO always drains.


async def run_io(fn: t.Callable[..., T], *args: t.Any, **kwargs: t.Any) -> T:
    """Run blocking transfer or file work on the SDK's IO threads."""
    return await _run("io", fn, *args, **kwargs)


async def run_callback(fn: t.Callable[..., T], *args: t.Any, **kwargs: t.Any) -> T:
    """Run a user callback that may call back into the SDK (a custom tool)."""
    return await _run("callback", fn, *args, **kwargs)


def run_on_loop(
    loop: asyncio.AbstractEventLoop, coroutine: t.Coroutine[t.Any, t.Any, T]
) -> T:
    """Run ``coroutine`` on ``loop`` from another thread and return its result."""
    try:
        running = asyncio.get_running_loop()
    except RuntimeError:
        running = None
    if running is loop:
        coroutine.close()
        raise SyncCallOnEventLoopError()
    if loop.is_closed() or not loop.is_running():
        coroutine.close()
        raise EventLoopNotRunningError()
    return asyncio.run_coroutine_threadsafe(coroutine, loop).result()


def sync_bridge(
    fn: t.Callable[..., t.Coroutine[t.Any, t.Any, T]],
    loop: t.Optional[asyncio.AbstractEventLoop] = None,
) -> t.Callable[..., T]:
    """A blocking callable for the coroutine function ``fn``.

    ``loop`` defaults to the running loop, so call this from the coroutine that
    hands the callback out.
    """
    target = loop if loop is not None else asyncio.get_running_loop()

    @functools.wraps(fn)
    def call(*args: t.Any, **kwargs: t.Any) -> T:
        return run_on_loop(target, fn(*args, **kwargs))

    return call
