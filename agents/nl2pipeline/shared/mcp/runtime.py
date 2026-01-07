from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from .registry import McpRegistry
from .types import McpError, TraceEntry, new_trace_entry


@dataclass
class EmbeddedMcpRuntime:
    """
    In-process MCP-shaped dispatcher.

    This runtime does NOT implement the MCP transport protocol (JSON-RPC, SSE, etc.).
    It provides the same logical contract (tools/resources/trace) so the agent workflow can
    be refactored to MCP calls with minimal churn, and later swapped to a real MCP client/server.
    """

    registry: McpRegistry
    trace: List[TraceEntry]

    @classmethod
    def new(cls, registry: Optional[McpRegistry] = None) -> "EmbeddedMcpRuntime":
        return cls(registry=registry or McpRegistry(), trace=[])

    def list_tools(self) -> List[Dict[str, str]]:
        return self.registry.list_tools()

    def list_resources(self) -> List[Dict[str, str]]:
        return self.registry.list_resources()

    def call_tool(self, name: str, args: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        start = time.time()
        safe_args = dict(args or {})
        try:
            tool = self.registry.get_tool(name)
            out = tool.handler(safe_args)
            latency_ms = int((time.time() - start) * 1000)
            self.trace.append(
                new_trace_entry(
                    kind="tool_call",
                    name=name,
                    args=safe_args,
                    ok=True,
                    latency_ms=latency_ms,
                )
            )
            return out or {}
        except McpError as e:
            latency_ms = int((time.time() - start) * 1000)
            self.trace.append(
                new_trace_entry(
                    kind="tool_call",
                    name=name,
                    args=safe_args,
                    ok=False,
                    latency_ms=latency_ms,
                    error_code=e.code,
                    error_message=e.message,
                )
            )
            raise
        except KeyError as e:
            latency_ms = int((time.time() - start) * 1000)
            err = McpError(code="not_found", message=str(e))
            self.trace.append(
                new_trace_entry(
                    kind="tool_call",
                    name=name,
                    args=safe_args,
                    ok=False,
                    latency_ms=latency_ms,
                    error_code=err.code,
                    error_message=err.message,
                )
            )
            raise err from None
        except Exception as e:
            latency_ms = int((time.time() - start) * 1000)
            err = McpError(code="tool_error", message=str(e))
            self.trace.append(
                new_trace_entry(
                    kind="tool_call",
                    name=name,
                    args=safe_args,
                    ok=False,
                    latency_ms=latency_ms,
                    error_code=err.code,
                    error_message=err.message,
                )
            )
            raise err from None

    def read_resource(self, uri: str) -> Dict[str, Any]:
        start = time.time()
        args = {"uri": uri}
        try:
            matched = self.registry.match_resource(uri)
            if matched is None:
                raise McpError(code="not_found", message=f"unknown resource: {uri}")
            res_def, full_uri = matched
            out = res_def.handler(full_uri)
            latency_ms = int((time.time() - start) * 1000)
            self.trace.append(
                new_trace_entry(
                    kind="resource_read",
                    name=res_def.uri_prefix,
                    args=args,
                    ok=True,
                    latency_ms=latency_ms,
                )
            )
            return out or {}
        except McpError as e:
            latency_ms = int((time.time() - start) * 1000)
            self.trace.append(
                new_trace_entry(
                    kind="resource_read",
                    name=uri,
                    args=args,
                    ok=False,
                    latency_ms=latency_ms,
                    error_code=e.code,
                    error_message=e.message,
                )
            )
            raise
        except Exception as e:
            latency_ms = int((time.time() - start) * 1000)
            err = McpError(code="resource_error", message=str(e))
            self.trace.append(
                new_trace_entry(
                    kind="resource_read",
                    name=uri,
                    args=args,
                    ok=False,
                    latency_ms=latency_ms,
                    error_code=err.code,
                    error_message=err.message,
                )
            )
            raise err from None


__all__ = ["EmbeddedMcpRuntime"]

