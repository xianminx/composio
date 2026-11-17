from composio.core.models.base import AsyncResource
from composio.core.models.internal import (
    INTERNAL_SDK_REALTIME_CREDENTIALS_ENDPOINT,
    SDKRealtimeCredentialsResponse,
)


class AsyncInternal(AsyncResource):
    """Async counterpart of :class:`~composio.core.models.internal.Internal`."""

    async def get_sdk_realtime_credentials(self) -> SDKRealtimeCredentialsResponse:
        return await self._client.get(
            path=INTERNAL_SDK_REALTIME_CREDENTIALS_ENDPOINT,
            cast_to=SDKRealtimeCredentialsResponse,
        )
