"""Serve the built single-page app: its files as they are, index.html otherwise."""

from __future__ import annotations

from fastapi.staticfiles import StaticFiles
from starlette.exceptions import HTTPException


class SinglePageApp(StaticFiles):
    """Static files that fall back to index.html for client-side routes.

    Paths under /api never fall back: an unknown API path stays a 404 rather
    than turning into the app's HTML.
    """

    def __init__(self, directory):
        super().__init__(directory=directory, html=True, check_dir=False)

    async def get_response(self, path: str, scope):
        try:
            return await super().get_response(path, scope)
        except HTTPException as exc:
            if exc.status_code != 404 or path.startswith("api/") or path == "api":
                raise
            return await super().get_response("index.html", scope)
