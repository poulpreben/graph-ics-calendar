"""Wrappers around MSAL and the Microsoft Graph calendar delta API."""

from __future__ import annotations

from typing import Any

import httpx
import msal

from .config import GRAPH_BASE, SCOPES, Settings
from .db import TokenStore


class AuthenticationError(RuntimeError):
    """Raised when an access token cannot be acquired for a given account."""


class DeltaLinkExpired(RuntimeError):
    """Raised when Graph rejects a delta link (HTTP 410) and a full resync is needed."""


def build_msal_app(
    settings: Settings, cache: msal.SerializableTokenCache
) -> msal.ConfidentialClientApplication:
    return msal.ConfidentialClientApplication(
        settings.client_id,
        authority=settings.authority,
        client_credential=settings.client_secret,
        token_cache=cache,
    )


def acquire_access_token(settings: Settings, store: TokenStore, home_account_id: str) -> str:
    """Return a fresh access token for ``home_account_id`` using the cached
    refresh token. Persists any cache updates back to SQLite."""

    cache = store.load_cache()
    client = build_msal_app(settings, cache)
    accounts = [a for a in client.get_accounts() if a["home_account_id"] == home_account_id]
    if not accounts:
        raise AuthenticationError(
            f"no cached account for {home_account_id}; user must sign in again"
        )
    result = client.acquire_token_silent(SCOPES, account=accounts[0])
    store.persist_cache(cache)
    if not result or "access_token" not in result:
        raise AuthenticationError("silent token refresh failed; user must sign in again")
    return str(result["access_token"])


async def fetch_delta_page(
    http: httpx.AsyncClient,
    access_token: str,
    url: str,
) -> tuple[list[dict[str, Any]], str | None, str | None]:
    """Fetch one page of a Graph ``calendarView/delta`` query.

    Returns ``(events, next_link, delta_link)``. Intermediate pages carry
    ``next_link``; the final page carries ``delta_link``.
    """

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Prefer": 'outlook.timezone="UTC", odata.maxpagesize=100',
    }
    response = await http.get(url, headers=headers)
    if response.status_code == 410:
        raise DeltaLinkExpired("delta link expired (410); full resync required")
    if response.status_code == 401:
        raise AuthenticationError("graph returned 401 for access token")
    response.raise_for_status()
    data = response.json()
    events: list[dict[str, Any]] = data.get("value") or []
    next_link: str | None = data.get("@odata.nextLink")
    delta_link: str | None = data.get("@odata.deltaLink")
    return events, next_link, delta_link


def build_initial_delta_url(window_start: str, window_end: str, calendar_id: str) -> str:
    """Build the first-page URL for a calendarView delta query on one calendar."""

    return (
        f"{GRAPH_BASE}/me/calendars/{calendar_id}/calendarView/delta"
        f"?startDateTime={window_start}&endDateTime={window_end}"
    )


async def list_calendars(http: httpx.AsyncClient, access_token: str) -> list[dict[str, Any]]:
    """List the signed-in user's calendars.

    Returns the subset of fields we use: ``id``, ``name``, ``isDefaultCalendar``,
    ``owner`` (dict with ``name``/``address``), ``canEdit``.
    """

    headers = {"Authorization": f"Bearer {access_token}"}
    url = f"{GRAPH_BASE}/me/calendars?$select=id,name,isDefaultCalendar,owner,canEdit"
    calendars: list[dict[str, Any]] = []
    while url:
        response = await http.get(url, headers=headers)
        if response.status_code == 401:
            raise AuthenticationError("graph returned 401 listing calendars")
        response.raise_for_status()
        data = response.json()
        calendars.extend(data.get("value") or [])
        url = data.get("@odata.nextLink") or ""
    return calendars


def remove_msal_account(settings: Settings, store: TokenStore, home_account_id: str) -> bool:
    """Remove an account (and its refresh token) from the MSAL cache.

    Returns True if an account was found and removed. Persists the cache back
    to SQLite. The access token at the IdP is NOT explicitly revoked — the
    user should also revoke the app in their Microsoft account if they want
    the refresh token invalidated server-side.
    """

    cache = store.load_cache()
    client = build_msal_app(settings, cache)
    matches = [a for a in client.get_accounts() if a["home_account_id"] == home_account_id]
    if not matches:
        return False
    for account in matches:
        client.remove_account(account)
    store.persist_cache(cache)
    return True
