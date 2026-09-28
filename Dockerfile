FROM python:3.12-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/

WORKDIR /app

COPY pyproject.toml uv.lock ./
RUN uv sync --group mcp --no-dev --no-install-project

COPY scrape/ scrape/
COPY db/ db/
COPY mcp_server/ mcp_server/
COPY web/ web/

EXPOSE 8000

CMD [".venv/bin/python", "-m", "uvicorn", "soogle_web.asgi:application", "--app-dir", "web", "--host", "0.0.0.0", "--port", "8000"]