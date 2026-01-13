#!/usr/bin/env python3

from __future__ import annotations

import argparse
import sys
from pathlib import Path

if __package__ is None:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from agents.nl2pipeline.shared.manager_client import ManagerClient  # noqa: E402
from agents.nl2pipeline.shared.mcp import (  # noqa: E402
    EmbeddedMcpRuntime,
    McpRegistry,
    StdioMcpServer,
    register_veloflux_mcp,
)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="VeloFlux MCP server (stdio, minimal)")
    p.add_argument(
        "--stdio",
        action="store_true",
        help="Serve MCP over stdio (LSP framing).",
    )
    p.add_argument(
        "--manager-url",
        default="http://127.0.0.1:8080",
        help="VeloFlux Manager base URL (default http://127.0.0.1:8080).",
    )
    p.add_argument(
        "--timeout-secs",
        type=float,
        default=10.0,
        help="Manager HTTP timeout in seconds (default 10).",
    )
    return p


def main(argv: list[str]) -> int:
    args = build_parser().parse_args(argv)
    if not args.stdio:
        print("missing mode: use --stdio", file=sys.stderr)
        return 2

    registry = McpRegistry()
    manager = ManagerClient.new(args.manager_url, args.timeout_secs)
    register_veloflux_mcp(registry, manager)
    runtime = EmbeddedMcpRuntime.new(registry)
    server = StdioMcpServer(runtime=runtime)
    return server.serve_forever()


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
