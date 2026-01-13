from __future__ import annotations

import urllib.parse
from typing import Any, Dict, List

from ..catalogs import compact_function_def, filter_syntax_constructs
from ..manager_client import ApiError, ManagerClient
from .registry import McpRegistry
from .types import McpError


def _to_mcp_error(err: ApiError, *, tool_or_resource: str) -> McpError:
    msg = str(err)
    lower = msg.lower()
    if "timeout after" in lower or "timed out" in lower:
        return McpError(code="timeout", message=msg, details={"source": tool_or_resource})
    return McpError(code="manager_http_error", message=msg, details={"source": tool_or_resource})


def _summarize_streams(streams: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for s in streams:
        if not isinstance(s, dict):
            continue
        name = str(s.get("name", "")).strip()
        schema = s.get("schema") or {}
        cols = (schema.get("columns") or []) if isinstance(schema, dict) else []
        col_count = len(cols) if isinstance(cols, list) else 0
        sample_cols: List[str] = []
        if isinstance(cols, list):
            for c in cols[:8]:
                if isinstance(c, dict) and c.get("name"):
                    sample_cols.append(str(c["name"]))
        out.append(
            {
                "name": name,
                "cols": col_count,
                "sample_cols": sample_cols,
            }
        )
    return out


def register_veloflux_mcp(registry: McpRegistry, manager: ManagerClient) -> None:
    """
    Register the v1 tools/resources defined in `docs/mcp_design.md`.

    This is a thin adapter: it maps MCP-shaped calls to Manager REST calls.
    """

    # Tools
    def _streams_list(_args: Dict[str, Any]) -> Dict[str, Any]:
        try:
            streams = manager.list_streams()
            return {"streams": streams}
        except ApiError as e:
            raise _to_mcp_error(e, tool_or_resource="streams.list") from None

    def _streams_describe(args: Dict[str, Any]) -> Dict[str, Any]:
        name = str(args.get("name", "")).strip()
        if not name:
            raise McpError(code="invalid_params", message="missing required param: name")
        try:
            desc = manager.describe_stream(name)
            return {"describe": desc}
        except ApiError as e:
            raise _to_mcp_error(e, tool_or_resource="streams.describe") from None

    def _catalog_functions(_args: Dict[str, Any]) -> Dict[str, Any]:
        try:
            return {"functions": manager.list_functions()}
        except ApiError as e:
            raise _to_mcp_error(e, tool_or_resource="catalog.functions") from None

    def _catalog_syntax(_args: Dict[str, Any]) -> Dict[str, Any]:
        try:
            return {"syntax_capabilities": manager.get_syntax_capabilities()}
        except ApiError as e:
            raise _to_mcp_error(e, tool_or_resource="catalog.syntax_capabilities") from None

    def _pipelines_create(args: Dict[str, Any]) -> Dict[str, Any]:
        # Pass-through to Manager (agent controls the id lifecycle).
        pipeline_id = str(args.get("id", "")).strip()
        sql = str(args.get("sql", "")).strip()
        sinks = args.get("sinks")
        options = args.get("options")
        if not pipeline_id:
            raise McpError(code="invalid_params", message="missing required param: id")
        if not sql:
            raise McpError(code="invalid_params", message="missing required param: sql")
        body: Dict[str, Any] = {"id": pipeline_id, "sql": sql}
        if sinks is not None:
            body["sinks"] = sinks
        if options is not None:
            body["options"] = options
        try:
            resp = manager.create_pipeline(body)
            return {"pipeline_id": pipeline_id, "response": resp}
        except ApiError as e:
            # v1: keep error coarse (later we can refine planner vs HTTP errors).
            raise _to_mcp_error(e, tool_or_resource="pipelines.create") from None

    def _pipelines_explain(args: Dict[str, Any]) -> Dict[str, Any]:
        pipeline_id = str(args.get("pipeline_id", "")).strip()
        if not pipeline_id:
            raise McpError(code="invalid_params", message="missing required param: pipeline_id")
        try:
            pretty = manager.explain_pipeline(pipeline_id)
            return {"pretty": pretty}
        except ApiError as e:
            raise _to_mcp_error(e, tool_or_resource="pipelines.explain") from None

    def _pipelines_delete(args: Dict[str, Any]) -> Dict[str, Any]:
        pipeline_id = str(args.get("pipeline_id", "")).strip()
        if not pipeline_id:
            raise McpError(code="invalid_params", message="missing required param: pipeline_id")
        try:
            manager.delete_pipeline(pipeline_id)
            return {"ok": True}
        except ApiError as e:
            # Best-effort delete; still surface as error to the caller.
            raise _to_mcp_error(e, tool_or_resource="pipelines.delete") from None

    registry.register_tool("streams.list", "List available streams (GET /streams).", _streams_list)
    registry.register_tool(
        "streams.describe",
        "Describe a stream schema (GET /streams/describe/:name).",
        _streams_describe,
    )
    registry.register_tool(
        "catalog.functions",
        "List function catalog (GET /functions).",
        _catalog_functions,
    )
    registry.register_tool(
        "catalog.syntax_capabilities",
        "Get syntax capabilities (GET /capabilities/syntax).",
        _catalog_syntax,
    )
    registry.register_tool(
        "pipelines.create",
        "Create pipeline (POST /pipelines). Agent may treat it as temporary and delete after use.",
        _pipelines_create,
    )
    registry.register_tool(
        "pipelines.explain",
        "Explain pipeline (GET /pipelines/:id/explain).",
        _pipelines_explain,
    )
    registry.register_tool(
        "pipelines.delete",
        "Delete pipeline (DELETE /pipelines/:id).",
        _pipelines_delete,
    )

    # Resources
    def _functions_digest(_uri: str) -> Dict[str, Any]:
        try:
            functions = manager.list_functions()
        except ApiError as e:
            raise _to_mcp_error(e, tool_or_resource="veloflux://catalog/functions_digest") from None
        return {"functions": [compact_function_def(f) for f in functions]}

    def _syntax_digest(_uri: str) -> Dict[str, Any]:
        try:
            caps = manager.get_syntax_capabilities()
        except ApiError as e:
            raise _to_mcp_error(e, tool_or_resource="veloflux://catalog/syntax_digest") from None
        constructs = filter_syntax_constructs(caps.get("constructs", []) or [])
        return {
            "dialect": caps.get("dialect", ""),
            "ir": caps.get("ir", ""),
            "constructs": constructs,
        }

    def _streams_snapshot(_uri: str) -> Dict[str, Any]:
        try:
            streams = manager.list_streams()
        except ApiError as e:
            raise _to_mcp_error(e, tool_or_resource="veloflux://streams/snapshot") from None
        return {"streams": _summarize_streams(streams)}

    def _stream_schema(uri: str) -> Dict[str, Any]:
        parsed = urllib.parse.urlparse(uri)
        qs = urllib.parse.parse_qs(parsed.query or "")
        name = (qs.get("name") or [""])[0].strip()
        if not name:
            raise McpError(code="invalid_params", message="missing required query param: name")
        try:
            desc = manager.describe_stream(name)
        except ApiError as e:
            raise _to_mcp_error(e, tool_or_resource="veloflux://streams/schema") from None
        schema = (desc.get("spec") or {}).get("schema") or {}
        return {"name": name, "schema": schema}

    registry.register_resource(
        "veloflux://catalog/functions_digest",
        "Compact function digest derived from GET /functions.",
        _functions_digest,
    )
    registry.register_resource(
        "veloflux://catalog/syntax_digest",
        "Compact syntax digest derived from GET /capabilities/syntax.",
        _syntax_digest,
    )
    registry.register_resource(
        "veloflux://streams/snapshot",
        "Streams snapshot derived from GET /streams.",
        _streams_snapshot,
    )
    registry.register_resource(
        "veloflux://streams/schema",
        "Stream schema derived from GET /streams/describe/:name. Use ?name=...",
        _stream_schema,
    )


__all__ = ["register_veloflux_mcp"]
