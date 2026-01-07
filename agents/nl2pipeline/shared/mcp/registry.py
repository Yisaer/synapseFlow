from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Tuple


ToolHandler = Callable[[Dict[str, Any]], Dict[str, Any]]
ResourceHandler = Callable[[str], Dict[str, Any]]


@dataclass(frozen=True)
class ToolDef:
    name: str
    description: str
    handler: ToolHandler


@dataclass(frozen=True)
class ResourceDef:
    """
    Resource definition. `uri_prefix` is used for prefix matching.

    Examples:
    - Fixed:   uri_prefix="synapseflow://catalog/functions_digest"
    - Param:   uri_prefix="synapseflow://streams/schema"
    """

    uri_prefix: str
    description: str
    handler: ResourceHandler


class McpRegistry:
    def __init__(self) -> None:
        self._tools: Dict[str, ToolDef] = {}
        self._resources: List[ResourceDef] = []

    def register_tool(self, name: str, description: str, handler: ToolHandler) -> None:
        if not name.strip():
            raise ValueError("tool name must be non-empty")
        if name in self._tools:
            raise ValueError(f"tool already registered: {name}")
        self._tools[name] = ToolDef(name=name, description=description, handler=handler)

    def register_resource(self, uri_prefix: str, description: str, handler: ResourceHandler) -> None:
        if not uri_prefix.strip():
            raise ValueError("resource uri_prefix must be non-empty")
        self._resources.append(
            ResourceDef(uri_prefix=uri_prefix.rstrip("/"), description=description, handler=handler)
        )

    def list_tools(self) -> List[Dict[str, str]]:
        return [
            {"name": t.name, "description": t.description}
            for t in sorted(self._tools.values(), key=lambda x: x.name)
        ]

    def list_resources(self) -> List[Dict[str, str]]:
        return [{"uri_prefix": r.uri_prefix, "description": r.description} for r in self._resources]

    def get_tool(self, name: str) -> ToolDef:
        if name not in self._tools:
            raise KeyError(f"unknown tool: {name}")
        return self._tools[name]

    def match_resource(self, uri: str) -> Optional[Tuple[ResourceDef, str]]:
        u = uri.strip()
        if not u:
            return None
        # Longest prefix wins to avoid generic prefixes shadowing specific ones.
        best: Optional[ResourceDef] = None
        for r in self._resources:
            if u == r.uri_prefix or u.startswith(r.uri_prefix + "/") or u.startswith(r.uri_prefix + "?"):
                if best is None or len(r.uri_prefix) > len(best.uri_prefix):
                    best = r
        if best is None:
            return None
        return best, u


__all__ = ["McpRegistry", "ResourceDef", "ToolDef", "ResourceHandler", "ToolHandler"]

