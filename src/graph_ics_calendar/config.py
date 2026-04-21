"""Runtime configuration loaded from environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

from cryptography.fernet import Fernet, InvalidToken


@dataclass(frozen=True)
class Settings:
    client_id: str
    client_secret: str
    tenant_id: str
    public_base_url: str
    database_path: Path
    past_days: int
    future_days: int
    sync_interval_seconds: int
    full_resync_after_seconds: int
    cache_key: bytes
    enable_docs: bool

    @property
    def authority(self) -> str:
        return f"https://login.microsoftonline.com/{self.tenant_id}"

    @property
    def redirect_uri(self) -> str:
        return f"{self.public_base_url}/auth/callback"


def _required(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"environment variable {name} is required")
    return value


def _bool(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _load_cache_key() -> bytes:
    raw = _required("GRAPH_ICS_CACHE_KEY").encode("ascii")
    try:
        Fernet(raw)
    except (ValueError, InvalidToken) as exc:
        raise RuntimeError(
            "GRAPH_ICS_CACHE_KEY is not a valid Fernet key. Generate one with: "
            "python -c 'from cryptography.fernet import Fernet; "
            "print(Fernet.generate_key().decode())'"
        ) from exc
    return raw


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings(
        client_id=_required("MS_CLIENT_ID"),
        client_secret=_required("MS_CLIENT_SECRET"),
        tenant_id=os.environ.get("MS_TENANT_ID", "common"),
        public_base_url=os.environ.get("PUBLIC_BASE_URL", "http://localhost:8000").rstrip("/"),
        database_path=Path(os.environ.get("DATABASE_PATH", "./data/tokens.sqlite3")),
        past_days=int(os.environ.get("ICS_PAST_DAYS", "30")),
        future_days=int(os.environ.get("ICS_FUTURE_DAYS", "365")),
        sync_interval_seconds=int(os.environ.get("SYNC_INTERVAL_SECONDS", "600")),
        full_resync_after_seconds=int(os.environ.get("FULL_RESYNC_AFTER_SECONDS", "86400")),
        cache_key=_load_cache_key(),
        enable_docs=_bool("GRAPH_ICS_ENABLE_DOCS", default=False),
    )


# Delegated permission: user-scoped read access to the signed-in user's
# calendar. ``offline_access`` is added automatically by MSAL on confidential
# clients, so a refresh token is issued and cached in our SQLite database.
SCOPES: list[str] = ["Calendars.Read"]

GRAPH_BASE: str = "https://graph.microsoft.com/v1.0"
