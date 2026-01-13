# MCP Design (v1): Embedded MCP Server in CLI

This document defines a v1 MCP-shaped contract for the `nl2pipeline` agent.

The chosen architecture is **CLI embeds an MCP Server** that adapts veloFlux Manager REST APIs
into MCP **resources** and **tools**. The agent workflow uses these resources/tools for grounding
and verification in the `ask → verify → loop` flow.

## Goals

- Provide a stable, schema-first contract for agent grounding and verification.
- Keep `tmp` semantics **inside the agent only** (Manager remains unaware of “temporary pipeline”).
- Enable deterministic traceability for each turn (tool calls and resource reads).

## Non-goals (v1)

- No separate MCP server process / deployment (in-process only).
- No `sql_hash` / pipeline hash support in trace (v1 records raw SQL).
- No automatic “apply pipeline” behavior (agent only emits JSON for manual apply).

## Architecture (v1)

- **Agent/Workflow** (legacy CLI and/or LangGraph CLI) calls MCP tools/resources via an in-process MCP layer.
- **Embedded MCP Server** executes requests by calling **veloFlux Manager REST API**.
- **Manager** remains unchanged; MCP is an agent-side adaptation layer.

## Deployment modes

The MCP layer is designed as **reusable core + pluggable transport** so it can run in two modes:

1) **Embedded (in-process)**
- The CLI creates an `EmbeddedMcpRuntime` and calls `call_tool(...)` / `read_resource(...)` directly.
- No JSON-RPC transport is involved.

2) **Standalone server (stdio transport)**
- A small wrapper exposes the same registry/runtime over JSON-RPC on stdio (LSP framing).
- This enables launching a `veloFlux-mcp --stdio` subprocess from an MCP client.

## Resources (v1)

All resources are intended to be cacheable inside the embedded MCP server (TTL-based).

1) `veloFlux://catalog/functions_digest`
- A compact digest of `GET /functions` results.
- Intended for prompt grounding (short, stable).

2) `veloFlux://catalog/syntax_digest`
- A compact digest of `GET /capabilities/syntax` results.
- Intended for prompt grounding (short, stable).

3) `veloFlux://streams/snapshot`
- A snapshot of `GET /streams` including `fetched_at` and a summary (column count / sample columns).

4) `veloFlux://streams/schema?name=<stream_name>`
- The schema for a specific stream, derived from `GET /streams/describe/:name`.

## Tools (v1)

Tools map to existing Manager REST endpoints. There is **no** `tmp` terminology in tool names.
The agent creates a pipeline with a temporary id (agent convention) and deletes it after use.

### Streams / Catalog

1) `streams.list`
- REST: `GET /streams`

2) `streams.describe { name }`
- REST: `GET /streams/describe/:name`

3) `catalog.functions`
- REST: `GET /functions`
- Note: primarily for debugging and (re)building digests.

4) `catalog.syntax_capabilities`
- REST: `GET /capabilities/syntax`
- Note: primarily for debugging and (re)building digests.

### Pipelines (verification only)

5) `pipelines.create { id, sql, sinks, options }`
- REST: `POST /pipelines`
- Agent convention: `id` is generated as a temporary pipeline id; it must be deleted after explain.

6) `pipelines.explain { pipeline_id }`
- REST: `GET /pipelines/:id/explain`

7) `pipelines.delete { pipeline_id }`
- REST: `DELETE /pipelines/:id` (best-effort)

## Trace (v1)

The embedded MCP layer MUST produce a structured trace entry for every tool call and resource read.
v1 records raw SQL in `args` (no hash support yet).

Minimum trace entry fields:

- `ts_ms`: integer
- `kind`: `"tool_call" | "resource_read"`
- `name`: tool/resource name
- `ok`: boolean
- `latency_ms`: integer
- `error_code`: string (when `ok=false`)
- `error_message`: string (when `ok=false`)
- `args`: object
  - For tool calls: the tool input args (including raw `sql`).
  - For resource reads: `{ "uri": "..." }`.

## Error conventions

All failures should be normalized into stable error shapes for the agent:

- `error_code`: e.g. `manager_http_error`, `planner_error`, `not_found`, `timeout`
- `error_message`: human-readable, safe to show to users
- Optional: `details` (if needed) for debugging (HTTP status, raw response, etc.)

## Security considerations (v1)

- The agent should restrict tool usage to verification/introspection paths only.
- The agent must delete any pipeline it creates for verification.
- Trace contains raw SQL; treat trace as potentially sensitive and avoid logging secrets.

## Future work

- Replace raw SQL in trace with `sql_hash` and store SQL separately (optional).
- Expose the same tools/resources over a standalone MCP server or embed MCP directly in Manager.
- Add capability/version stamps for resources to avoid stale grounding.
