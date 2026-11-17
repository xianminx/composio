"""Tool router session deletion helper."""

from __future__ import annotations

import typing as t

import typing_extensions as te

from composio.client import HttpClient
from composio.exceptions import ValidationError


class ToolRouterSessionDeleteResponse(te.TypedDict):
    """Response returned when a tool router session is deleted."""

    session_id: str
    deleted: t.Literal[True]


def delete_tool_router_session(
    client: HttpClient,
    session_id: str,
) -> ToolRouterSessionDeleteResponse:
    """Delete a tool router session by ID."""
    return delete_response(client.tool_router.session.delete(session_id))


def delete_response(response: object) -> ToolRouterSessionDeleteResponse:
    """Validate a ``session.delete`` response."""
    response_session_id = getattr(response, "session_id", None)
    deleted = getattr(response, "deleted", None)
    if not isinstance(response_session_id, str) or deleted is not True:
        raise ValidationError("Invalid tool router session delete response")

    return {
        "session_id": response_session_id,
        "deleted": True,
    }
