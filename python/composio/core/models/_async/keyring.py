from __future__ import annotations

from composio.client.types import keyring_list_transfer_keys_response
from composio.core.models.base import AsyncResource


class AsyncKeyring(AsyncResource):
    """Async counterpart of :class:`~composio.core.models.keyring.Keyring`."""

    async def list_transfer_keys(
        self,
    ) -> keyring_list_transfer_keys_response.KeyringListTransferKeysResponse:
        """See :meth:`Keyring.list_transfer_keys`."""
        return await self._client.keyring.list_transfer_keys()
