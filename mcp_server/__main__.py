import argparse
import asyncio

from mcp_server import server


def main():
    parser = argparse.ArgumentParser(description="Soogle MCP server")
    parser.add_argument(
        "--transport",
        choices=["stdio", "http"],
        default="stdio",
        help="stdio (default, for local MCP clients) or http (for containers)",
    )
    parser.add_argument("--host", default="0.0.0.0", help="http transport bind host")
    parser.add_argument("--port", type=int, default=8000, help="http transport bind port")
    args = parser.parse_args()

    if args.transport == "http":
        asyncio.run(server.run_streamable_http_async(host=args.host, port=args.port))
    else:
        asyncio.run(server.run_stdio_async())


if __name__ == "__main__":
    main()