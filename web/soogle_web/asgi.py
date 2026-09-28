"""
ASGI config for soogle_web project.

Serves the Django app, plus the MCP server at /mcp (streamable HTTP).
Requires an ASGI server (uvicorn/gunicorn) and the `mcp` uv group; without
the group the /mcp route is simply absent.

The MCP app's Starlette lifespan (which starts its session manager) is run
from Django's own lifespan handler, since uvicorn only runs the lifespan of
the top-level application.
"""

import os
import sys
from contextlib import asynccontextmanager
from pathlib import Path

from django.core.asgi import get_asgi_application

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'soogle_web.settings')

django_app = get_asgi_application()

try:
    # mcp_server lives at the repo root, one level above web/.
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
    from mcp_server import server as mcp_server

    mcp_app = mcp_server.streamable_http_app()
except ImportError:
    mcp_app = None


@asynccontextmanager
async def _mcp_lifespan():
    if mcp_app is None:
        yield
        return
    async with mcp_app.router.lifespan_context(mcp_app):
        yield


async def application(scope, receive, send):
    if scope["type"] == "lifespan":
        async with _mcp_lifespan():
            while True:
                message = await receive()
                if message["type"] == "lifespan.startup":
                    await send({"type": "lifespan.startup.complete"})
                elif message["type"] == "lifespan.shutdown":
                    await send({"type": "lifespan.shutdown.complete"})
                    return
    elif mcp_app is not None and scope["type"] == "http" and scope["path"].startswith("/mcp"):
        await mcp_app(scope, receive, send)
    else:
        await django_app(scope, receive, send)