"""CLI entry point: ``uv run graph-ics-calendar`` starts the web server."""

from __future__ import annotations

import os

import uvicorn


def main() -> None:
    host = os.environ.get("HOST", "127.0.0.1")
    port = int(os.environ.get("PORT", "8000"))
    # Single worker by design: the background sync loop assumes one process.
    uvicorn.run(
        "graph_ics_calendar.web:create_app",
        factory=True,
        host=host,
        port=port,
        workers=1,
    )


if __name__ == "__main__":
    main()
