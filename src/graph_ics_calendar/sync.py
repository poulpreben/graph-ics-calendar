"""Background service that keeps each user's cached calendar up to date.

For every ``(account, calendar)`` pair with at least one feed subscription, we
run a sync cycle every ``SYNC_INTERVAL_SECONDS`` (default 10 minutes). The
sync uses Microsoft Graph's ``calendarView/delta`` endpoint scoped to that
calendar, so after the initial load only created/updated/deleted events since
the last sync are transferred.

Microsoft Graph bakes the ``startDateTime``/``endDateTime`` window into the
delta link, so a fresh (full) resync is performed periodically — default once
a day — to shift the window forward.

Before the per-pair loop each cycle, feeds that still have a NULL
``calendar_id`` (from the pre-multi-calendar schema) are resolved to the
user's default calendar via ``GET /me/calendars``.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx

from .config import Settings
from .db import TokenStore
from .graph import (
    AuthenticationError,
    DeltaLinkExpired,
    acquire_access_token,
    build_initial_delta_url,
    fetch_delta_page,
    list_calendars,
)

log = logging.getLogger(__name__)


def _compute_window(settings: Settings) -> tuple[str, str]:
    now = datetime.now(UTC)
    start = now - timedelta(days=settings.past_days)
    end = now + timedelta(days=settings.future_days)
    fmt = "%Y-%m-%dT%H:%M:%SZ"
    return start.strftime(fmt), end.strftime(fmt)


def _should_full_resync(settings: Settings, state: dict[str, str | None], now: datetime) -> bool:
    if not state.get("delta_link"):
        return True
    last_full = state.get("last_full_sync")
    if not last_full:
        return True
    try:
        last_full_dt = datetime.fromisoformat(last_full)
    except ValueError:
        return True
    return (now - last_full_dt).total_seconds() > settings.full_resync_after_seconds


def _partition_delta_page(
    page: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[str]]:
    """Split a delta page into upserts and tombstones.

    Graph represents removed events with ``@removed`` plus the event ``id``.
    """

    upserts: list[dict[str, Any]] = []
    deleted_ids: list[str] = []
    for item in page:
        if "@removed" in item and item.get("id"):
            deleted_ids.append(item["id"])
        elif item.get("id"):
            upserts.append(item)
    return upserts, deleted_ids


async def backfill_missing_calendar_ids(
    settings: Settings,
    store: TokenStore,
    http: httpx.AsyncClient,
) -> None:
    """Resolve any feeds with NULL ``calendar_id`` to the account's default
    calendar. Runs once per sync cycle and is a no-op when everything is
    already backfilled."""

    pending = await asyncio.to_thread(store.list_feeds_needing_calendar_backfill)
    if not pending:
        return

    # Group by account so we only fetch each user's calendar list once.
    by_account: dict[str, list[dict[str, Any]]] = {}
    for feed in pending:
        by_account.setdefault(feed["home_account_id"], []).append(feed)

    for home_account_id, feeds in by_account.items():
        try:
            access_token = await asyncio.to_thread(
                acquire_access_token, settings, store, home_account_id
            )
        except AuthenticationError as exc:
            log.warning("auth failure resolving default calendar for %s: %s", home_account_id, exc)
            continue
        try:
            calendars = await list_calendars(http, access_token)
        except (httpx.HTTPError, AuthenticationError) as exc:
            log.warning("failed to list calendars for %s: %s", home_account_id, exc)
            continue

        default = next((c for c in calendars if c.get("isDefaultCalendar")), None)
        if default is None:
            log.warning("no default calendar found for %s; skipping backfill", home_account_id)
            continue

        for feed in feeds:
            await asyncio.to_thread(
                store.set_feed_calendar, feed["feed_token"], default["id"], default.get("name")
            )
            log.info(
                "backfilled feed %s to default calendar %r",
                feed["feed_token"][:6] + "…",
                default.get("name"),
            )


async def sync_pair(
    settings: Settings,
    store: TokenStore,
    http: httpx.AsyncClient,
    home_account_id: str,
    calendar_id: str,
) -> None:
    """Run one sync cycle for a single (account, calendar) pair."""

    state = await asyncio.to_thread(store.get_sync_state, home_account_id, calendar_id)
    now = datetime.now(UTC)

    full = _should_full_resync(settings, state, now)
    if full:
        window_start, window_end = _compute_window(settings)
        current_url: str | None = build_initial_delta_url(window_start, window_end, calendar_id)
        await asyncio.to_thread(store.clear_events, home_account_id, calendar_id)
    else:
        window_start = state["window_start"] or ""
        window_end = state["window_end"] or ""
        current_url = state["delta_link"]

    try:
        access_token = await asyncio.to_thread(
            acquire_access_token, settings, store, home_account_id
        )
    except AuthenticationError as exc:
        log.warning("auth failure for %s: %s", home_account_id, exc)
        await asyncio.to_thread(
            store.update_sync_state,
            home_account_id,
            calendar_id,
            delta_link=state.get("delta_link"),
            window_start=window_start,
            window_end=window_end,
            last_error=str(exc),
        )
        return

    final_delta_link: str | None = None
    try:
        while current_url:
            page, next_link, delta_link = await fetch_delta_page(http, access_token, current_url)
            upserts, deleted_ids = _partition_delta_page(page)
            if upserts:
                await asyncio.to_thread(
                    store.upsert_events, home_account_id, calendar_id, upserts
                )
            if deleted_ids:
                await asyncio.to_thread(
                    store.delete_events, home_account_id, calendar_id, deleted_ids
                )
            if delta_link:
                final_delta_link = delta_link
            current_url = next_link
    except DeltaLinkExpired:
        log.info(
            "delta link expired for %s/%s; clearing and retrying next cycle",
            home_account_id,
            calendar_id,
        )
        await asyncio.to_thread(
            store.update_sync_state,
            home_account_id,
            calendar_id,
            delta_link=None,
            window_start=None,
            window_end=None,
            last_error="delta_link_expired",
        )
        return
    except AuthenticationError as exc:
        log.warning("auth failure mid-sync for %s: %s", home_account_id, exc)
        await asyncio.to_thread(
            store.update_sync_state,
            home_account_id,
            calendar_id,
            delta_link=state.get("delta_link"),
            window_start=window_start,
            window_end=window_end,
            last_error=str(exc),
        )
        return
    except httpx.HTTPError as exc:
        log.exception("http error syncing %s/%s", home_account_id, calendar_id)
        await asyncio.to_thread(
            store.update_sync_state,
            home_account_id,
            calendar_id,
            delta_link=state.get("delta_link"),
            window_start=window_start,
            window_end=window_end,
            last_error=f"http: {exc}",
        )
        return

    await asyncio.to_thread(
        store.update_sync_state,
        home_account_id,
        calendar_id,
        delta_link=final_delta_link,
        window_start=window_start,
        window_end=window_end,
        last_full_sync=now.isoformat() if full else None,
        last_error=None,
    )


class SyncService:
    """Drive ``sync_pair`` periodically for every subscribed (account, calendar)."""

    def __init__(self, settings: Settings, store: TokenStore) -> None:
        self._settings = settings
        self._store = store
        self._task: asyncio.Task[None] | None = None
        self._stop = asyncio.Event()

    async def start(self) -> None:
        if self._task is not None:
            return
        self._stop.clear()
        self._task = asyncio.create_task(self._run(), name="calendar-sync")

    async def stop(self) -> None:
        self._stop.set()
        if self._task is not None:
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None

    async def trigger_pair(self, home_account_id: str, calendar_id: str) -> None:
        """Run a one-shot sync for a specific (account, calendar) — used right
        after the user adds a new feed so the ICS URL returns events quickly."""

        async with httpx.AsyncClient(timeout=30.0) as http:
            await sync_pair(self._settings, self._store, http, home_account_id, calendar_id)

    async def _run(self) -> None:
        interval = self._settings.sync_interval_seconds
        log.info("sync service started; interval=%ss", interval)
        while not self._stop.is_set():
            try:
                await self._cycle()
            except Exception:
                log.exception("unexpected error during sync cycle")
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._stop.wait(), timeout=interval)
        log.info("sync service stopped")

    async def _cycle(self) -> None:
        async with httpx.AsyncClient(timeout=30.0) as http:
            await backfill_missing_calendar_ids(self._settings, self._store, http)
            pairs = await asyncio.to_thread(self._store.list_active_pairs)
            if not pairs:
                return
            for home_account_id, calendar_id in pairs:
                if self._stop.is_set():
                    break
                await sync_pair(self._settings, self._store, http, home_account_id, calendar_id)
