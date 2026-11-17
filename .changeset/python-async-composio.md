---
'@composio/python-sdk': minor
---

Add `AsyncComposio`, a native asyncio client for the Python SDK. It has the same surface as `Composio` (tools, toolkits, sessions, auth configs, connected accounts, triggers, MCP, webhooks, logs, keyring, custom toolkits), with every method that reaches the Composio API a coroutine sent over `httpx.AsyncClient`. `Composio` is unchanged.

```python
from composio import AsyncComposio

async with AsyncComposio() as composio:
    session = await composio.create(user_id="user_123")
    tools = await session.tools()
```

Trigger subscriptions made through `AsyncComposio` accept coroutine handlers. Providers gain `aexecute_tool`, and the OpenAI providers gain async helpers such as `ahandle_tool_calls`.
