# graph-ics-calendar

Self-hosted Microsoft 365 → ICS calendar proxy. Sign in with your Microsoft
work/school account, get back a personal subscription URL that any calendar
client can consume. Useful when your phone cannot log in directly to Exchange
Online but can subscribe to an ICS feed.

## How it works

1. A tiny FastAPI app presents a "Sign in with Microsoft" button.
2. OAuth 2.0 authorization-code flow (MSAL, **delegated permissions**, scope
   `Calendars.Read`) returns a refresh token.
3. The refresh token is persisted in a SQLite database, encrypted at rest with
   a Fernet key held outside the DB (see **Securing the token cache** below).
4. A background task refreshes each signed-in user's calendar every 10 minutes
   using the Microsoft Graph `calendarView/delta` endpoint, so only changes
   are transferred after the initial sync.
5. The ICS endpoint (`/calendar/<feed_token>.ics`) serves from the local
   cache — your phone hitting the URL never waits on Microsoft Graph.

Event window: 30 days in the past to 12 months in the future (configurable).
A full resync runs at least once every 24 hours to slide the window forward.

## Microsoft Entra app registration

1. In Entra ID → **App registrations** → **New registration**.
2. Redirect URI (Web): `https://your-domain.example/auth/callback` (or
   `http://localhost:8000/auth/callback` for local dev).
3. **Certificates & secrets** → create a client secret, copy the value.
4. **API permissions** → add Microsoft Graph **delegated** permission
   `Calendars.Read`. Grant admin consent if required by your tenant.
5. Copy the **Application (client) ID** and **Directory (tenant) ID**.

## Configuration

Copy `.env.example` to `.env` and fill in the values:

```bash
cp .env.example .env
```

## Running

This project uses [uv](https://docs.astral.sh/uv/) for dependency management.

```bash
# Install dependencies (creates .venv and uv.lock)
uv sync

# Run the server
uv run graph-ics-calendar
```

Then open <http://localhost:8000>, click **Sign in with Microsoft**, and
subscribe your phone to the URL you get back.

### Development tooling

```bash
# Lint + format check
uv run ruff check .
uv run ruff format --check .

# Type check
uv run ty check
```

## Operational notes

- **Single worker**: the background sync assumes one process. Run behind a
  reverse proxy (Caddy, nginx, Traefik) for TLS rather than scaling out.
- **Secrets**: `data/tokens.sqlite3` contains the encrypted MSAL cache. The
  encryption key lives in `GRAPH_ICS_CACHE_KEY` and must be held separately
  from the DB file and its backups.
- **Revocation**: to cut access, remove the user from the app in Entra ID or
  delete the relevant row from the `feeds` table (and optionally clear
  `msal_cache`).
- **Feed URL is a bearer token**: anyone with the `feed_token` in the URL can
  read the cached calendar. Don't share it.

### Securing the token cache

Refresh tokens are stored in the `msal_cache` table encrypted with
[Fernet](https://cryptography.io/en/latest/fernet/) (AES-128-CBC + HMAC-SHA256).
The key is read from `GRAPH_ICS_CACHE_KEY` and is never written to the DB.

Generate a key:

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

Operational notes:

- Keep the key out of the directory and backups that include `tokens.sqlite3`.
  Co-locating them defeats the point.
- Losing the key is equivalent to wiping the cache: every user must sign in
  again. There is no recovery path by design.
- Starting the service with the wrong key fails fast (the service refuses to
  decrypt rather than silently discarding tokens).
- Upgrading from a pre-encryption install: on first boot with a key set, an
  existing plaintext MSAL blob is migrated in place, no re-auth required.
- Rotation: to rotate, start once with the old key, export the cache, shut
  down, set the new key, and let MSAL re-persist on next token acquisition.
  A dedicated rotate command can be added if this becomes a routine operation.

## Project layout

```
src/graph_ics_calendar/
  __main__.py   # `graph-ics-calendar` CLI → uvicorn
  config.py     # env-backed Settings
  db.py         # SQLite: msal_cache, feeds, events, sync_state
  graph.py     # MSAL + Graph calendarView/delta helpers
  ics.py        # RFC 5545 ICS serializer
  sync.py       # background refresh loop (10 min, delta)
  web.py        # FastAPI app + OAuth flow + ICS endpoint
```
