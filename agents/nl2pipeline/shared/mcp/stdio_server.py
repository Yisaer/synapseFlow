from __future__ import annotations

import io
import json
import sys
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

from .runtime import EmbeddedMcpRuntime
from .types import McpError


JsonRpcId = Any


def _read_lsp_message(stream: io.BufferedReader) -> Optional[Dict[str, Any]]:
    """
    Read an LSP-style framed JSON-RPC message from `stream`.

    Frame format:
      Content-Length: <bytes>\r\n
      \r\n
      <json bytes>
    """

    headers: Dict[str, str] = {}
    while True:
        line = stream.readline()
        if not line:
            return None
        if line in (b"\r\n", b"\n"):
            break
        try:
            text = line.decode("utf-8", errors="replace").strip()
        except Exception:
            continue
        if ":" not in text:
            continue
        k, v = text.split(":", 1)
        headers[k.strip().lower()] = v.strip()

    length_raw = headers.get("content-length")
    if not length_raw:
        return None
    try:
        length = int(length_raw)
    except Exception:
        return None

    payload = stream.read(length)
    if not payload:
        return None
    try:
        return json.loads(payload.decode("utf-8", errors="replace"))
    except Exception:
        return None


def _write_lsp_message(stream: io.BufferedWriter, obj: Dict[str, Any]) -> None:
    data = json.dumps(obj, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    header = f"Content-Length: {len(data)}\r\n\r\n".encode("ascii")
    stream.write(header)
    stream.write(data)
    stream.flush()


def _jsonrpc_ok(req_id: JsonRpcId, result: Any) -> Dict[str, Any]:
    return {"jsonrpc": "2.0", "id": req_id, "result": result}


def _jsonrpc_err(req_id: JsonRpcId, code: int, message: str, data: Optional[Any] = None) -> Dict[str, Any]:
    err: Dict[str, Any] = {"code": int(code), "message": message}
    if data is not None:
        err["data"] = data
    return {"jsonrpc": "2.0", "id": req_id, "error": err}


@dataclass
class StdioMcpServer:
    """
    Minimal MCP-compatible JSON-RPC server over stdio (LSP framing).

    This server exposes the embedded runtime's tools/resources.
    It is intentionally minimal and only implements a subset of MCP methods needed by this repo.
    """

    runtime: EmbeddedMcpRuntime

    def serve_forever(
        self,
        *,
        stdin: io.BufferedReader = sys.stdin.buffer,
        stdout: io.BufferedWriter = sys.stdout.buffer,
    ) -> int:
        while True:
            req = _read_lsp_message(stdin)
            if req is None:
                return 0
            resp = self.handle_request(req)
            if resp is not None:
                _write_lsp_message(stdout, resp)

    def handle_request(self, req: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        req_id = req.get("id")
        method = str(req.get("method") or "").strip()
        params = req.get("params") or {}

        # Notifications have no id; ignore them.
        if "id" not in req:
            return None

        try:
            if method == "initialize":
                return _jsonrpc_ok(
                    req_id,
                    {
                        "protocolVersion": "0.1",
                        "serverInfo": {"name": "synapseflow-embedded-mcp", "version": "v1"},
                        "capabilities": {"tools": {}, "resources": {}},
                    },
                )

            if method == "ping":
                return _jsonrpc_ok(req_id, {"ok": True})

            if method == "tools/list":
                tools = self.runtime.list_tools()
                # MCP-style shape (minimal).
                return _jsonrpc_ok(req_id, {"tools": tools})

            if method == "tools/call":
                if not isinstance(params, dict):
                    raise McpError(code="invalid_params", message="params must be an object")
                name = str(params.get("name") or "").strip()
                arguments = params.get("arguments") or {}
                if not isinstance(arguments, dict):
                    raise McpError(code="invalid_params", message="arguments must be an object")
                out = self.runtime.call_tool(name, arguments)
                # Return JSON as a text content item (common MCP pattern).
                return _jsonrpc_ok(
                    req_id,
                    {
                        "content": [
                            {"type": "text", "text": json.dumps(out, ensure_ascii=False)}
                        ]
                    },
                )

            if method == "resources/list":
                res = self.runtime.list_resources()
                return _jsonrpc_ok(req_id, {"resources": res})

            if method == "resources/read":
                if not isinstance(params, dict):
                    raise McpError(code="invalid_params", message="params must be an object")
                uri = str(params.get("uri") or "").strip()
                if not uri:
                    raise McpError(code="invalid_params", message="missing uri")
                out = self.runtime.read_resource(uri)
                return _jsonrpc_ok(
                    req_id,
                    {
                        "contents": [
                            {
                                "uri": uri,
                                "mimeType": "application/json",
                                "text": json.dumps(out, ensure_ascii=False),
                            }
                        ]
                    },
                )

            return _jsonrpc_err(req_id, -32601, f"method not found: {method}")
        except McpError as e:
            # JSON-RPC application error (use -32000 range).
            return _jsonrpc_err(req_id, -32000, str(e), data=e.details)
        except Exception as e:
            return _jsonrpc_err(req_id, -32000, str(e))


__all__ = ["StdioMcpServer"]

