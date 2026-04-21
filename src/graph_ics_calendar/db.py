"""SQLite-backed persistence.

Stores four things:

* ``msal_cache`` — the serialized MSAL token cache (refresh tokens for every
  signed-in user), encrypted at rest with Fernet (AES-128-CBC + HMAC-SHA256)
  using the key from ``GRAPH_ICS_CACHE_KEY``. The DB file is also written with
  ``0600`` permissions as defence-in-depth.
* ``feeds`` — public feed tokens shared with calendar clients, mapped to the
  (Microsoft account, calendar) pair that the feed proxies. One feed row =
  one proxied calendar for one account.
* ``events`` — per-(account, calendar) cached calendar events, populated by the
  background sync service. The ICS endpoint serves from this table instead of
  hitting Microsoft Graph on every request.
* ``sync_state`` — the Graph delta link + window metadata, per (account,
  calendar), used for incremental refresh.
"""

from __future__ import annotations

import contextlib
import json
import logging
import os
import sqlite3
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import msal
from cryptography.fernet import Fernet, InvalidToken

log = logging.getLogger(__name__)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS msal_cache (
    id         INTEGER PRIMARY KEY CHECK (id = 1),
    data       TEXT    NOT NULL,
    updated_at TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS feeds (
    feed_token      TEXT PRIMARY KEY,
    home_account_id TEXT NOT NULL,
    username        TEXT,
    display_name    TEXT,
    calendar_id     TEXT,
    calendar_name   TEXT,
    created_at      TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_feeds_account
    ON feeds (home_account_id);

CREATE TABLE IF NOT EXISTS events (
    home_account_id TEXT NOT NULL,
    calendar_id     TEXT NOT NULL,
    event_id        TEXT NOT NULL,
    raw_json        TEXT NOT NULL,
    updated_at      TEXT NOT NULL,
    PRIMARY KEY (home_account_id, calendar_id, event_id)
);

CREATE INDEX IF NOT EXISTS idx_events_account_calendar
    ON events (home_account_id, calendar_id);

CREATE TABLE IF NOT EXISTS sync_state (
    home_account_id TEXT NOT NULL,
    calendar_id     TEXT NOT NULL,
    delta_link      TEXT,
    window_start    TEXT,
    window_end      TEXT,
    last_full_sync  TEXT,
    last_sync_at    TEXT,
    last_error      TEXT,
    PRIMARY KEY (home_account_id, calendar_id)
);
"""


def _looks_like_msal_json(s: str) -> bool:
    stripped = s.lstrip()
    if not stripped.startswith("{"):
        return False
    try:
        obj = json.loads(stripped)
    except ValueError:
        return False
    return isinstance(obj, dict)


def _migrate_pre_calendar_schema(conn: sqlite3.Connection) -> None:
    """Migrate from the single-calendar-per-account schema to per-calendar.

    Old schema: feeds lacked ``calendar_id`` / ``calendar_name``; events and
    sync_state were keyed solely on ``home_account_id``.

    New schema adds ``calendar_id`` to all three tables. The events/sync_state
    tables are caches, so we drop their contents — the next sync cycle will
    repopulate them once each feed has been backfilled with its calendar id.
    """

    feeds_cols = {row[1] for row in conn.execute("PRAGMA table_info(feeds)").fetchall()}
    if not feeds_cols or "calendar_id" in feeds_cols:
        # Fresh DB (no feeds table yet) or already migrated.
        return

    log.warning("migrating DB schema to per-calendar feeds/events/sync_state")
    conn.execute("ALTER TABLE feeds ADD COLUMN calendar_id TEXT")
    conn.execute("ALTER TABLE feeds ADD COLUMN calendar_name TEXT")
    # Drop event + sync caches; they'll be rebuilt on the next sync cycle.
    conn.execute("DROP TABLE IF EXISTS events")
    conn.execute("DROP TABLE IF EXISTS sync_state")


class TokenStore:
    """SQLite store for the MSAL cache, feeds, cached events, and sync state."""

    def __init__(self, path: Path, cache_key: bytes) -> None:
        self._path = path
        self._fernet = Fernet(cache_key)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        first_create = not self._path.exists()
        with self._connect() as conn:
            _migrate_pre_calendar_schema(conn)
            conn.executescript(_SCHEMA)
        if first_create:
            # Non-POSIX filesystems may not support chmod; acceptable.
            with contextlib.suppress(OSError):
                os.chmod(self._path, 0o600)

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self._path, isolation_level=None)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        try:
            yield conn
        finally:
            conn.close()

    # -- MSAL cache ----------------------------------------------------------

    def load_cache(self) -> msal.SerializableTokenCache:
        cache = msal.SerializableTokenCache()
        with self._connect() as conn:
            row = conn.execute("SELECT data FROM msal_cache WHERE id = 1").fetchone()
        if not row or not row[0]:
            return cache

        stored: str = row[0]
        try:
            plaintext = self._fernet.decrypt(stored.encode("ascii")).decode("utf-8")
        except InvalidToken:
            # One-shot migration path: a pre-encryption install has plaintext
            # MSAL JSON here. If it parses as JSON, accept it and let the next
            # persist_cache re-write it encrypted.
            if _looks_like_msal_json(stored):
                log.warning("migrating plaintext MSAL cache to encrypted storage")
                cache.deserialize(stored)
                self._rewrite_encrypted(stored)
                return cache
            raise RuntimeError(
                "failed to decrypt MSAL cache; GRAPH_ICS_CACHE_KEY does not match "
                "the key used to encrypt the existing database"
            ) from None
        cache.deserialize(plaintext)
        return cache

    def persist_cache(self, cache: msal.SerializableTokenCache) -> None:
        if not cache.has_state_changed:
            return
        token = self._fernet.encrypt(cache.serialize().encode("utf-8")).decode("ascii")
        now = datetime.now(UTC).isoformat()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO msal_cache (id, data, updated_at)
                VALUES (1, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    data       = excluded.data,
                    updated_at = excluded.updated_at
                """,
                (token, now),
            )

    def _rewrite_encrypted(self, plaintext: str) -> None:
        """Re-encrypt a legacy plaintext MSAL blob in place."""

        token = self._fernet.encrypt(plaintext.encode("utf-8")).decode("ascii")
        now = datetime.now(UTC).isoformat()
        with self._connect() as conn:
            conn.execute(
                "UPDATE msal_cache SET data = ?, updated_at = ? WHERE id = 1",
                (token, now),
            )

    # -- Feed subscriptions --------------------------------------------------

    def create_feed(
        self,
        feed_token: str,
        home_account_id: str,
        *,
        username: str | None,
        display_name: str | None,
        calendar_id: str,
        calendar_name: str | None,
    ) -> None:
        now = datetime.now(UTC).isoformat()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO feeds (
                    feed_token, home_account_id, username, display_name,
                    calendar_id, calendar_name, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    feed_token,
                    home_account_id,
                    username,
                    display_name,
                    calendar_id,
                    calendar_name,
                    now,
                ),
            )

    def get_feed(self, feed_token: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT feed_token, home_account_id, username, display_name,
                       calendar_id, calendar_name, created_at
                FROM feeds
                WHERE feed_token = ?
                """,
                (feed_token,),
            ).fetchone()
        if row is None:
            return None
        return {
            "feed_token": row[0],
            "home_account_id": row[1],
            "username": row[2] or "",
            "display_name": row[3] or "",
            "calendar_id": row[4],
            "calendar_name": row[5] or "",
            "created_at": row[6],
        }

    def list_feeds_for_account(self, home_account_id: str) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT feed_token, home_account_id, username, display_name,
                       calendar_id, calendar_name, created_at
                FROM feeds
                WHERE home_account_id = ?
                ORDER BY created_at
                """,
                (home_account_id,),
            ).fetchall()
        return [
            {
                "feed_token": r[0],
                "home_account_id": r[1],
                "username": r[2] or "",
                "display_name": r[3] or "",
                "calendar_id": r[4],
                "calendar_name": r[5] or "",
                "created_at": r[6],
            }
            for r in rows
        ]

    def list_feeds_needing_calendar_backfill(self) -> list[dict[str, Any]]:
        """Return feed rows where ``calendar_id`` is NULL (upgrade migration)."""

        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT feed_token, home_account_id, username, display_name
                FROM feeds
                WHERE calendar_id IS NULL
                """,
            ).fetchall()
        return [
            {
                "feed_token": r[0],
                "home_account_id": r[1],
                "username": r[2] or "",
                "display_name": r[3] or "",
            }
            for r in rows
        ]

    def set_feed_calendar(
        self, feed_token: str, calendar_id: str, calendar_name: str | None
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                "UPDATE feeds SET calendar_id = ?, calendar_name = ? WHERE feed_token = ?",
                (calendar_id, calendar_name, feed_token),
            )

    def rotate_feed_token(self, old_token: str, new_token: str) -> None:
        with self._connect() as conn:
            conn.execute(
                "UPDATE feeds SET feed_token = ? WHERE feed_token = ?",
                (new_token, old_token),
            )

    def delete_feed(self, feed_token: str) -> dict[str, Any] | None:
        """Delete one feed row. Also drops the cached events + sync_state for
        the (account, calendar) pair if no other feed references it.

        Returns the deleted feed row (for caller to act on), or None.
        """

        feed = self.get_feed(feed_token)
        if feed is None:
            return None
        with self._connect() as conn:
            conn.execute("DELETE FROM feeds WHERE feed_token = ?", (feed_token,))
            # If no sibling feed references the same (account, calendar), drop
            # its cached events + sync state.
            if feed["calendar_id"]:
                row = conn.execute(
                    """
                    SELECT count(*) FROM feeds
                    WHERE home_account_id = ? AND calendar_id = ?
                    """,
                    (feed["home_account_id"], feed["calendar_id"]),
                ).fetchone()
                if row and row[0] == 0:
                    conn.execute(
                        "DELETE FROM events WHERE home_account_id = ? AND calendar_id = ?",
                        (feed["home_account_id"], feed["calendar_id"]),
                    )
                    conn.execute(
                        "DELETE FROM sync_state WHERE home_account_id = ? AND calendar_id = ?",
                        (feed["home_account_id"], feed["calendar_id"]),
                    )
        return feed

    def delete_account(self, home_account_id: str) -> None:
        """Drop all feeds, cached events, and sync state for an account.

        Does NOT touch the MSAL cache — callers are expected to also call
        ``remove_msal_account`` so the refresh token is forgotten.
        """

        with self._connect() as conn:
            conn.execute("DELETE FROM feeds WHERE home_account_id = ?", (home_account_id,))
            conn.execute("DELETE FROM events WHERE home_account_id = ?", (home_account_id,))
            conn.execute("DELETE FROM sync_state WHERE home_account_id = ?", (home_account_id,))

    def list_active_pairs(self) -> list[tuple[str, str]]:
        """Return (home_account_id, calendar_id) pairs that have at least one
        feed subscription and a resolved calendar id. Skips rows still waiting
        on backfill."""

        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT DISTINCT home_account_id, calendar_id
                FROM feeds
                WHERE calendar_id IS NOT NULL
                """,
            ).fetchall()
        return [(r[0], r[1]) for r in rows]

    # -- Events --------------------------------------------------------------

    def upsert_events(
        self,
        home_account_id: str,
        calendar_id: str,
        events: Sequence[dict[str, Any]],
    ) -> None:
        if not events:
            return
        now = datetime.now(UTC).isoformat()
        rows = [
            (home_account_id, calendar_id, e["id"], json.dumps(e), now)
            for e in events
            if e.get("id")
        ]
        with self._connect() as conn:
            conn.executemany(
                """
                INSERT INTO events (home_account_id, calendar_id, event_id, raw_json, updated_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(home_account_id, calendar_id, event_id) DO UPDATE SET
                    raw_json   = excluded.raw_json,
                    updated_at = excluded.updated_at
                """,
                rows,
            )

    def delete_events(
        self,
        home_account_id: str,
        calendar_id: str,
        event_ids: Sequence[str],
    ) -> None:
        if not event_ids:
            return
        with self._connect() as conn:
            conn.executemany(
                """
                DELETE FROM events
                WHERE home_account_id = ? AND calendar_id = ? AND event_id = ?
                """,
                [(home_account_id, calendar_id, eid) for eid in event_ids],
            )

    def clear_events(self, home_account_id: str, calendar_id: str) -> None:
        with self._connect() as conn:
            conn.execute(
                "DELETE FROM events WHERE home_account_id = ? AND calendar_id = ?",
                (home_account_id, calendar_id),
            )

    def list_events(self, home_account_id: str, calendar_id: str) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT raw_json FROM events
                WHERE home_account_id = ? AND calendar_id = ?
                """,
                (home_account_id, calendar_id),
            ).fetchall()
        return [json.loads(r[0]) for r in rows]

    # -- Sync state ----------------------------------------------------------

    def get_sync_state(self, home_account_id: str, calendar_id: str) -> dict[str, str | None]:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT delta_link, window_start, window_end,
                       last_full_sync, last_sync_at, last_error
                FROM sync_state
                WHERE home_account_id = ? AND calendar_id = ?
                """,
                (home_account_id, calendar_id),
            ).fetchone()
        if row is None:
            return {
                "delta_link": None,
                "window_start": None,
                "window_end": None,
                "last_full_sync": None,
                "last_sync_at": None,
                "last_error": None,
            }
        return {
            "delta_link": row[0],
            "window_start": row[1],
            "window_end": row[2],
            "last_full_sync": row[3],
            "last_sync_at": row[4],
            "last_error": row[5],
        }

    def update_sync_state(
        self,
        home_account_id: str,
        calendar_id: str,
        *,
        delta_link: str | None,
        window_start: str | None,
        window_end: str | None,
        last_full_sync: str | None = None,
        last_error: str | None = None,
    ) -> None:
        now = datetime.now(UTC).isoformat()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO sync_state (
                    home_account_id, calendar_id, delta_link, window_start, window_end,
                    last_full_sync, last_sync_at, last_error
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(home_account_id, calendar_id) DO UPDATE SET
                    delta_link     = excluded.delta_link,
                    window_start   = excluded.window_start,
                    window_end     = excluded.window_end,
                    last_full_sync = COALESCE(excluded.last_full_sync, sync_state.last_full_sync),
                    last_sync_at   = excluded.last_sync_at,
                    last_error     = excluded.last_error
                """,
                (
                    home_account_id,
                    calendar_id,
                    delta_link,
                    window_start,
                    window_end,
                    last_full_sync,
                    now,
                    last_error,
                ),
            )
