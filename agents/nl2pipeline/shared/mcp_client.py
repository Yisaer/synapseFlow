from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List

from .catalogs import CapabilitiesDigest, supported_schema_type_strings
from .mcp import EmbeddedMcpRuntime, McpError


@dataclass
class VeloFluxMcpClient:
    """
    Typed wrapper over the embedded MCP runtime.

    This keeps the workflow readable (method calls instead of raw tool names),
    while still enforcing the MCP contract boundary.
    """

    runtime: EmbeddedMcpRuntime

    # Tools
    def streams_list(self) -> List[Dict[str, Any]]:
        out = self.runtime.call_tool("streams.list", {})
        streams = out.get("streams") or []
        return streams if isinstance(streams, list) else []

    def streams_describe(self, name: str) -> Dict[str, Any]:
        out = self.runtime.call_tool("streams.describe", {"name": name})
        desc = out.get("describe") or {}
        return desc if isinstance(desc, dict) else {}

    def catalog_functions(self) -> List[Dict[str, Any]]:
        out = self.runtime.call_tool("catalog.functions", {})
        functions = out.get("functions") or []
        return functions if isinstance(functions, list) else []

    def catalog_syntax_capabilities(self) -> Dict[str, Any]:
        out = self.runtime.call_tool("catalog.syntax_capabilities", {})
        caps = out.get("syntax_capabilities") or {}
        return caps if isinstance(caps, dict) else {}

    def pipelines_create(self, body: Dict[str, Any]) -> Dict[str, Any]:
        pipeline_id = str(body.get("id", "")).strip()
        sql = str(body.get("sql", "")).strip()
        sinks = body.get("sinks")
        options = body.get("options")
        args: Dict[str, Any] = {"id": pipeline_id, "sql": sql}
        if sinks is not None:
            args["sinks"] = sinks
        if options is not None:
            args["options"] = options
        return self.runtime.call_tool("pipelines.create", args)

    def pipelines_explain(self, pipeline_id: str) -> str:
        out = self.runtime.call_tool("pipelines.explain", {"pipeline_id": pipeline_id})
        return str(out.get("pretty", "") or "")

    def pipelines_delete(self, pipeline_id: str) -> bool:
        out = self.runtime.call_tool("pipelines.delete", {"pipeline_id": pipeline_id})
        return bool(out.get("ok") is True)

    # Resources
    def read_functions_digest(self) -> List[Dict[str, Any]]:
        out = self.runtime.read_resource("veloflux://catalog/functions_digest")
        funcs = out.get("functions") or []
        return funcs if isinstance(funcs, list) else []

    def read_syntax_digest(self) -> Dict[str, Any]:
        out = self.runtime.read_resource("veloflux://catalog/syntax_digest")
        return out if isinstance(out, dict) else {}

    def read_streams_snapshot(self) -> List[Dict[str, Any]]:
        out = self.runtime.read_resource("veloflux://streams/snapshot")
        streams = out.get("streams") or []
        return streams if isinstance(streams, list) else []

    def read_stream_schema(self, name: str) -> Dict[str, Any]:
        out = self.runtime.read_resource(f"veloflux://streams/schema?name={name}")
        schema = out.get("schema") or {}
        return schema if isinstance(schema, dict) else {}

    def build_capabilities_digest(self) -> CapabilitiesDigest:
        """
        Build a CapabilitiesDigest from MCP resources (compact form).
        """

        functions = self.read_functions_digest()
        syntax = self.read_syntax_digest()
        return CapabilitiesDigest(
            schema_type_strings=supported_schema_type_strings(),
            functions=functions,
            syntax_capabilities={
                "dialect": syntax.get("dialect", ""),
                "ir": syntax.get("ir", ""),
                "constructs": syntax.get("constructs", []) or [],
            },
        )

    def trace(self) -> List[Dict[str, Any]]:
        return list(self.runtime.trace)


__all__ = ["McpError", "VeloFluxMcpClient"]
