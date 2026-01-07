from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Dict, Literal, Optional


TraceKind = Literal["tool_call", "resource_read"]


@dataclass(frozen=True)
class McpError(RuntimeError):
    """
    A normalized error type produced by the embedded MCP layer.

    This is intentionally stable so agent code does not need to parse provider-specific
    exceptions (HTTPError, timeout strings, etc.).
    """

    code: str
    message: str
    details: Optional[Dict[str, Any]] = None

    def __str__(self) -> str:
        return f"{self.code}: {self.message}"


TraceEntry = Dict[str, Any]


def new_trace_entry(
    *,
    kind: TraceKind,
    name: str,
    args: Dict[str, Any],
    ok: bool,
    latency_ms: int,
    error_code: Optional[str] = None,
    error_message: Optional[str] = None,
) -> TraceEntry:
    entry: TraceEntry = {
        "ts_ms": int(time.time() * 1000),
        "kind": kind,
        "name": name,
        "ok": bool(ok),
        "latency_ms": int(latency_ms),
        "args": dict(args),
    }
    if not ok:
        entry["error_code"] = error_code or "unknown_error"
        entry["error_message"] = error_message or ""
    return entry


__all__ = ["McpError", "TraceEntry", "TraceKind", "new_trace_entry"]

