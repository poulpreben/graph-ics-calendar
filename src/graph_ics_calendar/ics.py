"""Convert Microsoft Graph calendar events into an RFC 5545 ICS document."""

from __future__ import annotations

from collections.abc import Iterable
from datetime import UTC, datetime, time, timedelta
from typing import Any

_SHOW_AS_TO_TRANSP: dict[str, str] = {
    "free": "TRANSPARENT",
    "tentative": "OPAQUE",
    "busy": "OPAQUE",
    "oof": "OPAQUE",
    "workingElsewhere": "OPAQUE",
    "unknown": "OPAQUE",
}


def _escape(s: str) -> str:
    return (
        s.replace("\\", "\\\\")
        .replace(";", "\\;")
        .replace(",", "\\,")
        .replace("\r\n", "\\n")
        .replace("\n", "\\n")
    )


def _parse_graph_dt(s: str) -> datetime:
    """Parse a Graph dateTime. With ``Prefer: outlook.timezone="UTC"`` the
    string looks like ``2026-04-20T10:00:00.0000000`` (no offset)."""

    s = s.split(".", 1)[0] if "." in s else s
    return datetime.fromisoformat(s).replace(tzinfo=UTC)


def _fmt_utc(dt: datetime) -> str:
    return dt.astimezone(UTC).strftime("%Y%m%dT%H%M%SZ")


def _fold(line: str) -> str:
    """RFC 5545 line folding: each line <= 75 octets, continuation prefixed by SP."""

    encoded = line.encode("utf-8")
    if len(encoded) <= 75:
        return line
    parts: list[bytes] = []
    while len(encoded) > 73:
        cut = 73
        # Don't split a multi-byte UTF-8 sequence
        while cut > 0 and (encoded[cut] & 0xC0) == 0x80:
            cut -= 1
        parts.append(encoded[:cut])
        encoded = encoded[cut:]
    parts.append(encoded)
    return "\r\n ".join(p.decode("utf-8") for p in parts)


def build_ics(
    events: Iterable[dict[str, Any]],
    *,
    calendar_name: str = "Microsoft 365 Calendar",
) -> str:
    # Graph's calendarView/delta returns recurring-series occurrences as skinny
    # payloads that only carry id/start/end/seriesMasterId. The canonical
    # subject/body/location/organizer live on the separate seriesMaster event.
    # Build a lookup so occurrences can inherit from their master, and skip
    # emitting the seriesMaster itself (its start/end reflect the series origin
    # and would duplicate the first instance).
    event_list = list(events)
    masters_by_id: dict[str, dict[str, Any]] = {
        e["id"]: e for e in event_list if e.get("type") == "seriesMaster" and e.get("id")
    }

    now_utc = datetime.now(UTC)
    lines: list[str] = [
        "BEGIN:VCALENDAR",
        "VERSION:2.0",
        "PRODID:-//graph-ics-calendar//EN",
        "CALSCALE:GREGORIAN",
        "METHOD:PUBLISH",
        f"X-WR-CALNAME:{_escape(calendar_name)}",
    ]
    for e in event_list:
        if e.get("type") == "seriesMaster":
            continue

        master: dict[str, Any] | None = None
        if e.get("type") == "occurrence":
            master = masters_by_id.get(e.get("seriesMasterId") or "")

        uid = e.get("iCalUId") or e.get("id")
        start_obj = e.get("start") or {}
        end_obj = e.get("end") or {}
        start_s = start_obj.get("dateTime")
        end_s = end_obj.get("dateTime")
        if not uid or not start_s or not end_s:
            continue

        subject = e.get("subject") or (master.get("subject") if master else None) or "(no subject)"
        body_preview = e.get("bodyPreview") or (master.get("bodyPreview") if master else "") or ""
        location_obj = e.get("location") or (master.get("location") if master else {}) or {}
        location = location_obj.get("displayName") or ""
        organizer_src = e.get("organizer") or (master.get("organizer") if master else {}) or {}
        organizer_addr = (organizer_src.get("emailAddress") or {}).get("address") or ""
        show_as = e.get("showAs") or (master.get("showAs") if master else None) or "busy"
        transp = _SHOW_AS_TO_TRANSP.get(show_as, "OPAQUE")
        is_all_day = bool(e.get("isAllDay"))
        is_cancelled = bool(e.get("isCancelled"))

        lines.append("BEGIN:VEVENT")
        lines.append(f"UID:{uid}")
        lines.append(f"DTSTAMP:{_fmt_utc(now_utc)}")
        if is_all_day:
            lines.append(f"DTSTART;VALUE=DATE:{start_s[:10].replace('-', '')}")
            lines.append(f"DTEND;VALUE=DATE:{end_s[:10].replace('-', '')}")
        else:
            lines.append(f"DTSTART:{_fmt_utc(_parse_graph_dt(start_s))}")
            lines.append(f"DTEND:{_fmt_utc(_parse_graph_dt(end_s))}")
        lines.append(f"SUMMARY:{_escape(subject)}")
        if body_preview:
            lines.append(f"DESCRIPTION:{_escape(body_preview)}")
        if location:
            lines.append(f"LOCATION:{_escape(location)}")
        if organizer_addr:
            lines.append(f"ORGANIZER:mailto:{organizer_addr}")
        lines.append(f"TRANSP:{transp}")
        if is_cancelled:
            lines.append("STATUS:CANCELLED")
        lines.append("END:VEVENT")
    lines.append("END:VCALENDAR")
    return "\r\n".join(_fold(line) for line in lines) + "\r\n"


def build_alert_ics(
    *,
    calendar_name: str = "Calendar sync paused",
    admin_url: str | None = None,
) -> str:
    """ICS returned when a feed URL is unknown or its backing calendar has
    gone away. Emits a 08:00-09:00 event every day of the current week so the
    subscriber's calendar app surfaces a visible prompt to re-authenticate."""

    now_local = datetime.now().astimezone()
    today = now_local.date()
    monday = today - timedelta(days=today.weekday())
    tz = now_local.tzinfo

    summary = "⚠️ Calendar sync paused — sign in again"
    description_lines = [
        "The ICS proxy can no longer update this calendar.",
        "This usually means your Microsoft 365 sign-in has expired or access was revoked.",
        "",
        "To resume updates:",
        "  1. Open the admin page" + (f" ({admin_url})" if admin_url else ""),
        "  2. Sign in with your Microsoft account",
        "  3. Re-subscribe to your calendar",
        "",
        "Until then, this calendar will not reflect any changes made in Outlook.",
    ]
    description = "\n".join(description_lines)

    now_utc = datetime.now(UTC)
    lines: list[str] = [
        "BEGIN:VCALENDAR",
        "VERSION:2.0",
        "PRODID:-//graph-ics-calendar//alert//EN",
        "CALSCALE:GREGORIAN",
        "METHOD:PUBLISH",
        f"X-WR-CALNAME:{_escape(calendar_name)}",
    ]
    for offset in range(7):
        day = monday + timedelta(days=offset)
        start_local = datetime.combine(day, time(8, 0), tzinfo=tz)
        end_local = datetime.combine(day, time(9, 0), tzinfo=tz)
        uid = f"graph-ics-calendar-alert-{day.isoformat()}@local"
        lines.extend(
            [
                "BEGIN:VEVENT",
                f"UID:{uid}",
                f"DTSTAMP:{_fmt_utc(now_utc)}",
                f"DTSTART:{_fmt_utc(start_local)}",
                f"DTEND:{_fmt_utc(end_local)}",
                f"SUMMARY:{_escape(summary)}",
                f"DESCRIPTION:{_escape(description)}",
                "TRANSP:OPAQUE",
                "END:VEVENT",
            ]
        )
    lines.append("END:VCALENDAR")
    return "\r\n".join(_fold(line) for line in lines) + "\r\n"
