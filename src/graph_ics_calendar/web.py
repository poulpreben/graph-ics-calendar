"""FastAPI application: login flow, admin UI, and ICS endpoint.

Session model
-------------
After OAuth completes, the server issues an opaque session id stored in an
in-memory dict keyed to the ``home_account_id``. The session is presented as
an HttpOnly SameSite=Lax cookie (``Secure`` when the public URL is HTTPS).
Sessions are process-scoped — restarting the service logs everyone out, which
is acceptable for a single-user self-hosted tool.

POST endpoints rely on ``SameSite=Lax`` to block cross-site form submissions;
no explicit CSRF tokens are issued.
"""

from __future__ import annotations

import asyncio
import html
import logging
import secrets
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urljoin, urlparse

import httpx
from fastapi import FastAPI, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse, Response

from .config import SCOPES, Settings, get_settings
from .db import TokenStore
from .graph import (
    AuthenticationError,
    acquire_access_token,
    build_msal_app,
    list_calendars,
    remove_msal_account,
)
from .ics import build_alert_ics, build_ics
from .sync import SyncService

log = logging.getLogger(__name__)

_STATE_TTL_SECONDS = 600.0
_SESSION_COOKIE = "gic_sid"
_SESSION_TTL_SECONDS = 60 * 60 * 24 * 30  # 30 days


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()
    store = TokenStore(settings.database_path, settings.cache_key)
    sync_service = SyncService(settings, store)
    oauth_states: dict[str, float] = {}
    sessions: dict[str, dict[str, str]] = {}
    secure_cookie = urlparse(settings.public_base_url).scheme == "https"

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        )
        await sync_service.start()
        try:
            yield
        finally:
            await sync_service.stop()

    app = FastAPI(
        title="graph-ics-calendar",
        lifespan=lifespan,
        docs_url="/docs" if settings.enable_docs else None,
        redoc_url="/redoc" if settings.enable_docs else None,
        openapi_url="/openapi.json" if settings.enable_docs else None,
    )
    app.state.settings = settings
    app.state.store = store
    app.state.sync_service = sync_service

    def issue_state() -> str:
        now = datetime.now(UTC).timestamp()
        for s, ts in list(oauth_states.items()):
            if now - ts > _STATE_TTL_SECONDS:
                oauth_states.pop(s, None)
        state = secrets.token_urlsafe(24)
        oauth_states[state] = now
        return state

    def consume_state(state: str) -> bool:
        return oauth_states.pop(state, None) is not None

    def issue_session(home_account_id: str, username: str, display_name: str) -> str:
        sid = secrets.token_urlsafe(32)
        sessions[sid] = {
            "home_account_id": home_account_id,
            "username": username,
            "display_name": display_name,
        }
        return sid

    def get_session(request: Request) -> dict[str, str] | None:
        sid = request.cookies.get(_SESSION_COOKIE)
        if not sid:
            return None
        return sessions.get(sid)

    def clear_session(request: Request, response: Response) -> None:
        sid = request.cookies.get(_SESSION_COOKIE)
        if sid:
            sessions.pop(sid, None)
        response.delete_cookie(_SESSION_COOKIE, path="/")

    def set_session_cookie(response: Response, sid: str) -> None:
        response.set_cookie(
            _SESSION_COOKIE,
            sid,
            max_age=_SESSION_TTL_SECONDS,
            httponly=True,
            secure=secure_cookie,
            samesite="lax",
            path="/",
        )

    @app.get("/", response_class=HTMLResponse)
    def index(request: Request) -> Response:
        if get_session(request):
            return RedirectResponse("/admin", status_code=302)
        return HTMLResponse(_render_signed_out())

    @app.get("/login")
    def login() -> RedirectResponse:
        cache = store.load_cache()
        client = build_msal_app(settings, cache)
        state = issue_state()
        auth_url = client.get_authorization_request_url(
            SCOPES,
            state=state,
            redirect_uri=settings.redirect_uri,
            prompt="select_account",
        )
        return RedirectResponse(auth_url, status_code=302)

    @app.get("/auth/callback")
    async def auth_callback(
        code: str | None = None,
        state: str | None = None,
        error: str | None = None,
        error_description: str | None = None,
    ) -> Response:
        if error:
            raise HTTPException(400, f"{error}: {error_description or ''}")
        if not code or not state:
            raise HTTPException(400, "missing code or state")
        if not consume_state(state):
            raise HTTPException(400, "invalid or expired state")

        cache = store.load_cache()
        client = build_msal_app(settings, cache)
        result = client.acquire_token_by_authorization_code(
            code, scopes=SCOPES, redirect_uri=settings.redirect_uri
        )
        if "access_token" not in result:
            store.persist_cache(cache)
            raise HTTPException(400, result.get("error_description") or "token exchange failed")

        claims: dict[str, Any] = result.get("id_token_claims") or {}
        username = claims.get("preferred_username") or ""
        display_name = claims.get("name") or ""

        accounts = client.get_accounts(username=username or None)
        store.persist_cache(cache)
        if not accounts:
            raise HTTPException(500, "msal did not register an account after code exchange")
        home_account_id = accounts[0]["home_account_id"]

        sid = issue_session(home_account_id, username, display_name)
        response = RedirectResponse("/admin", status_code=302)
        set_session_cookie(response, sid)
        return response

    @app.get("/admin", response_class=HTMLResponse)
    async def admin(request: Request) -> Response:
        session = get_session(request)
        if session is None:
            return RedirectResponse("/", status_code=302)
        home_account_id = session["home_account_id"]

        # Fetch live calendar list from Graph. If this fails (revoked account,
        # network), render with an error so the user still has access to the
        # existing feeds / revoke buttons.
        calendars: list[dict[str, Any]] | None = None
        calendar_error: str | None = None
        try:
            access_token = await asyncio.to_thread(
                acquire_access_token, settings, store, home_account_id
            )
            async with httpx.AsyncClient(timeout=15.0) as http:
                calendars = await list_calendars(http, access_token)
        except AuthenticationError as exc:
            calendar_error = f"Microsoft sign-in expired: {exc}"
        except httpx.HTTPError as exc:
            calendar_error = f"failed to contact Microsoft Graph: {exc}"

        feeds = store.list_feeds_for_account(home_account_id)
        return HTMLResponse(
            _render_admin(
                settings=settings,
                display_name=session["display_name"] or session["username"] or "you",
                username=session["username"],
                calendars=calendars,
                calendar_error=calendar_error,
                feeds=feeds,
            )
        )

    @app.post("/admin/feeds")
    async def create_feed(
        request: Request,
        calendar_id: str = Form(...),
        calendar_name: str = Form(""),
    ) -> Response:
        session = get_session(request)
        if session is None:
            raise HTTPException(401, "not signed in")
        home_account_id = session["home_account_id"]

        # Prevent duplicate feeds for the same calendar.
        for existing in store.list_feeds_for_account(home_account_id):
            if existing["calendar_id"] == calendar_id:
                return RedirectResponse("/admin", status_code=303)

        feed_token = secrets.token_urlsafe(24)
        store.create_feed(
            feed_token,
            home_account_id,
            username=session["username"] or None,
            display_name=session["display_name"] or None,
            calendar_id=calendar_id,
            calendar_name=calendar_name or None,
        )
        try:
            await sync_service.trigger_pair(home_account_id, calendar_id)
        except Exception:
            log.exception("initial sync failed for new feed %s/%s", home_account_id, calendar_id)
        return RedirectResponse("/admin", status_code=303)

    @app.post("/admin/feeds/{feed_token}/rotate")
    def rotate_feed(request: Request, feed_token: str) -> Response:
        session = get_session(request)
        if session is None:
            raise HTTPException(401, "not signed in")
        feed = store.get_feed(feed_token)
        if feed is None or feed["home_account_id"] != session["home_account_id"]:
            raise HTTPException(404, "feed not found")
        new_token = secrets.token_urlsafe(24)
        store.rotate_feed_token(feed_token, new_token)
        return RedirectResponse("/admin", status_code=303)

    @app.post("/admin/feeds/{feed_token}/delete")
    def delete_feed(request: Request, feed_token: str) -> Response:
        session = get_session(request)
        if session is None:
            raise HTTPException(401, "not signed in")
        feed = store.get_feed(feed_token)
        if feed is None or feed["home_account_id"] != session["home_account_id"]:
            raise HTTPException(404, "feed not found")
        store.delete_feed(feed_token)
        return RedirectResponse("/admin", status_code=303)

    @app.post("/admin/revoke")
    def revoke_account(request: Request) -> Response:
        session = get_session(request)
        if session is None:
            raise HTTPException(401, "not signed in")
        home_account_id = session["home_account_id"]
        store.delete_account(home_account_id)
        remove_msal_account(settings, store, home_account_id)
        response = RedirectResponse("/", status_code=303)
        clear_session(request, response)
        return response

    @app.post("/admin/logout")
    def logout(request: Request) -> Response:
        response = RedirectResponse("/", status_code=303)
        clear_session(request, response)
        return response

    @app.get("/calendar/{feed_token}.ics")
    def calendar_ics(feed_token: str) -> Response:
        feed = store.get_feed(feed_token)
        if feed is None or not feed.get("calendar_id"):
            # Return a valid ICS with a daily warning event instead of 404 so
            # the subscriber's calendar app surfaces the problem to them.
            body = build_alert_ics(admin_url=settings.public_base_url)
            return Response(
                content=body,
                media_type="text/calendar; charset=utf-8",
                headers={"Cache-Control": "private, max-age=300"},
            )
        events = store.list_events(feed["home_account_id"], feed["calendar_id"])
        calendar_name = (
            feed.get("calendar_name")
            or feed.get("display_name")
            or feed.get("username")
            or "Microsoft 365 Calendar"
        )
        body = build_ics(events, calendar_name=f"{calendar_name} (M365)")
        return Response(
            content=body,
            media_type="text/calendar; charset=utf-8",
            headers={"Cache-Control": "private, max-age=300"},
        )

    @app.get("/healthz")
    def healthz() -> dict[str, str]:
        return {"status": "ok"}

    return app


# ---------------------------------------------------------------------------
# HTML rendering
# ---------------------------------------------------------------------------

_BASE_CSS = """
body { font-family: -apple-system, system-ui, sans-serif; max-width: 48rem;
       margin: 2.5rem auto; padding: 0 1rem; color: #222; }
h1 { margin-bottom: 0.25rem; }
h2 { margin-top: 2rem; font-size: 1.15rem; }
p  { line-height: 1.5; }
small, .note { color: #666; font-size: 0.9rem; }
.btn { display: inline-flex; align-items: center; gap: 0.5rem;
       background: #2f2f2f; color: #fff; text-decoration: none;
       padding: 0.55rem 0.9rem; border-radius: 4px; font-weight: 500;
       border: none; cursor: pointer; font-size: 0.95rem; }
.btn:hover { background: #000; }
.btn.primary { background: #0078d4; }
.btn.primary:hover { background: #005ea6; }
.btn.danger  { background: #a1262c; }
.btn.danger:hover  { background: #7a1b1f; }
.btn.ghost   { background: transparent; color: #0078d4; padding: 0.35rem 0.5rem; }
.btn.ghost:hover { background: #eef5fb; }
.row { display: flex; align-items: center; gap: 0.5rem; flex-wrap: wrap; }
.card { border: 1px solid #e3e3e3; border-radius: 6px; padding: 0.9rem 1rem;
        margin: 0.6rem 0; }
.card h3 { margin: 0 0 0.3rem; font-size: 1rem; }
pre.url { background: #f3f3f3; padding: 0.5rem 0.7rem; border-radius: 4px;
          overflow-wrap: anywhere; white-space: pre-wrap;
          font-size: 0.85rem; margin: 0.4rem 0; }
.banner { padding: 0.7rem 0.9rem; border-radius: 4px; margin: 0.8rem 0;
          background: #fdecea; color: #7a1b1f; border: 1px solid #f3c4c4; }
form.inline { display: inline; margin: 0; }
"""


def _render_signed_out() -> str:
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Calendar ICS Proxy</title>
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <style>{_BASE_CSS}</style>
</head>
<body>
  <h1>Microsoft Calendar &rarr; ICS</h1>
  <p>Sign in with your Microsoft work or school account to choose which
     calendars to expose as ICS subscription URLs.</p>
  <p><a class="btn" href="/login">
    <svg width="20" height="20" viewBox="0 0 23 23" xmlns="http://www.w3.org/2000/svg">
      <rect x="1"  y="1"  width="10" height="10" fill="#f25022"/>
      <rect x="12" y="1"  width="10" height="10" fill="#7fba00"/>
      <rect x="1"  y="12" width="10" height="10" fill="#00a4ef"/>
      <rect x="12" y="12" width="10" height="10" fill="#ffb900"/>
    </svg>
    Sign in with Microsoft
  </a></p>
</body>
</html>"""


def _render_admin(
    *,
    settings: Settings,
    display_name: str,
    username: str,
    calendars: list[dict[str, Any]] | None,
    calendar_error: str | None,
    feeds: list[dict[str, Any]],
) -> str:
    exposed_ids = {f["calendar_id"] for f in feeds if f.get("calendar_id")}
    feeds_by_cal = {f["calendar_id"]: f for f in feeds if f.get("calendar_id")}

    # ---- "Your calendars" section --------------------------------------
    if calendar_error:
        calendars_section = f'<div class="banner">{html.escape(calendar_error)}</div>'
    elif calendars is None:
        calendars_section = "<p><em>loading&hellip;</em></p>"
    else:
        sorted_cals = sorted(
            calendars,
            key=lambda c: (not c.get("isDefaultCalendar"), (c.get("name") or "").lower()),
        )
        if not sorted_cals:
            calendars_section = "<p>No calendars found.</p>"
        else:
            items: list[str] = []
            for cal in sorted_cals:
                cal_id = cal.get("id") or ""
                cal_name = cal.get("name") or "(unnamed)"
                is_default = bool(cal.get("isDefaultCalendar"))
                tag = " <small>(default)</small>" if is_default else ""
                existing = feeds_by_cal.get(cal_id)
                if existing:
                    feed_url = urljoin(
                        settings.public_base_url + "/",
                        f"calendar/{existing['feed_token']}.ics",
                    )
                    action = (
                        f'<pre class="url">{html.escape(feed_url)}</pre>'
                        f'<div class="row">'
                        f'<form class="inline" method="post" '
                        f'action="/admin/feeds/{html.escape(existing["feed_token"])}/rotate">'
                        f'<button class="btn ghost" type="submit" '
                        f"onclick=\"return confirm('Rotate this feed URL? The current URL "
                        f"will stop working immediately and you\\'ll need to re-subscribe.')\">"
                        f"Rotate URL</button></form>"
                        f'<form class="inline" method="post" '
                        f'action="/admin/feeds/{html.escape(existing["feed_token"])}/delete">'
                        f'<button class="btn ghost" type="submit" '
                        f"onclick=\"return confirm('Stop proxying this calendar?')\">"
                        f"Stop proxying</button></form>"
                        f"</div>"
                    )
                else:
                    esc_id = html.escape(cal_id)
                    esc_name = html.escape(cal_name)
                    action = (
                        '<form class="inline" method="post" action="/admin/feeds">'
                        f'<input type="hidden" name="calendar_id" value="{esc_id}">'
                        f'<input type="hidden" name="calendar_name" value="{esc_name}">'
                        '<button class="btn primary" type="submit">'
                        "Expose as ICS feed</button>"
                        "</form>"
                    )
                items.append(
                    f'<div class="card"><h3>{html.escape(cal_name)}{tag}</h3>{action}</div>'
                )
            calendars_section = "\n".join(items)

    # ---- "Orphan" feeds (calendar_id present but calendar no longer listed)
    known_ids = {c.get("id") for c in (calendars or [])}
    orphans = (
        [
            f
            for f in feeds
            if f.get("calendar_id") and known_ids and f["calendar_id"] not in known_ids
        ]
        if calendars is not None
        else []
    )
    orphans_section = ""
    if orphans:
        cards: list[str] = []
        for f in orphans:
            feed_url = urljoin(
                settings.public_base_url + "/",
                f"calendar/{f['feed_token']}.ics",
            )
            cards.append(
                f'<div class="card">'
                f"<h3>{html.escape(f.get('calendar_name') or '(unknown calendar)')}</h3>"
                f'<p class="note">This calendar is no longer visible on your '
                f"Microsoft account (deleted or access removed).</p>"
                f'<pre class="url">{html.escape(feed_url)}</pre>'
                f'<form class="inline" method="post" '
                f'action="/admin/feeds/{html.escape(f["feed_token"])}/delete">'
                f'<button class="btn ghost" type="submit">Stop proxying</button></form>'
                f"</div>"
            )
        orphans_section = "<h2>Orphaned feeds</h2>" + "\n".join(cards)

    # ---- Pending-backfill feeds (calendar_id still NULL) ---------------
    pending = [f for f in feeds if not f.get("calendar_id")]
    pending_section = ""
    if pending:
        pending_section = (
            '<div class="banner">'
            f"{len(pending)} feed(s) are waiting on a background migration to "
            "resolve which calendar they proxy. Refresh in a minute."
            "</div>"
        )

    count_exposed = len(exposed_ids)
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Proxied calendars</title>
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <style>{_BASE_CSS}</style>
</head>
<body>
  <div class="row" style="justify-content: space-between;">
    <div>
      <h1>Proxied calendars</h1>
      <p class="note">Signed in as {html.escape(display_name)}
      {f"&lt;{html.escape(username)}&gt;" if username else ""}
      &middot; {count_exposed} exposed</p>
    </div>
    <div class="row">
      <form class="inline" method="post" action="/admin/logout">
        <button class="btn ghost" type="submit">Sign out</button>
      </form>
      <form class="inline" method="post" action="/admin/revoke">
        <button class="btn danger" type="submit"
          onclick="return confirm('Revoke access? This deletes all feeds and the refresh token.')">
          Revoke access
        </button>
      </form>
    </div>
  </div>
  {pending_section}
  <h2>Your calendars</h2>
  {calendars_section}
  {orphans_section}
  <p class="note" style="margin-top: 2rem;">
    Treat feed URLs like passwords &mdash; anyone with the URL can read the
    calendar until you rotate or stop it. &ldquo;Revoke access&rdquo; also
    removes the refresh token locally; if you want the token invalidated at
    Microsoft, also remove the app from
    <a href="https://myaccount.microsoft.com/" target="_blank" rel="noopener">
    myaccount.microsoft.com</a>.
  </p>
</body>
</html>"""
