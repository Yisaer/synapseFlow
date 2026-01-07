# Design: `nl2pipeline` (v1)

This document describes the initial design for an agent that converts natural language (NL)
requirements into:

- a stream definition request (`POST /streams`)
- a pipeline definition request (`POST /pipelines`)

## Constraints and grounding

The agent must follow `docs/agents_readme.md` grounding rules:

- Do not invent stream names, column names, column types, or function names.
- Treat Manager REST introspection as source of truth:
  - streams/schema: `GET /streams`, `GET /streams/describe/:name`
  - functions: `GET /functions`
  - syntax/expr subset: `GET /capabilities/syntax`

## Inputs / outputs

Inputs:

- NL request text
- Manager base URL
- LLM endpoint config (base URL, key, model)
- Optional: explicit stream name, pipeline id, MQTT broker/topic overrides
- Optional: schema (when creating a new MQTT source stream)

Outputs:

- `CreateStreamRequest` JSON (only when needed / requested)
- `CreatePipelineRequest` JSON
- The generated SQL string
- (Optional) check result: success or planning error message

## Execution modes

This tool runs as an interactive REPL. It does not apply a final pipeline by default; it emits the
JSON that the user can apply via Manager.

## State machine (v1)

Represent state explicitly to make later migration to LangGraph easy:

### Session initialization

1) Fetch catalogs:
   - `GET /capabilities/syntax`
   - `GET /functions`
   - `GET /streams`
2) Resolve an active stream:
   - Use an LLM router to decide whether the user is asking for stream listing/schema or NL→SQL.
   - For NL→SQL, ensure an active stream exists; otherwise prompt the user to pick one.
3) Create an LLM session using Chat Completions:
   - Keep a local message history (system + context + last N turns).
   - Seed the history with a compact "capabilities digest" (index-like types/functions/syntax).
   - Stream schema is provided per turn (and when active stream changes) to avoid binding a stream at startup.

### Per-turn loop

For each user prompt:

1) `draft_sql` (LLM):
   - First, stream a fast *SQL preview* (plain text) for responsiveness.
   - If planning fails, run a structured JSON repair loop (bounded attempts) using the planner error.
2) `validate_pipeline` (Manager planning as validator):
   - Create a temporary pipeline via `POST /pipelines`.
   - On failure, feed the error back to the LLM to repair SQL (bounded attempts).
3) `explain`:
   - `GET /pipelines/:id/explain` and show the pretty string to the user.
4) Cleanup:
   - `DELETE /pipelines/:id` best-effort after each round.
5) Emit:
   - Print `CreatePipelineRequest` JSON for manual apply.

## Code organization

The implementation is split so the workflow is easy to maintain:

- `agents/nl2pipeline/workflow.py`: core workflow state machine (events/phases).
- `agents/nl2pipeline/repl.py`: REPL wrapper over workflow events.
- `agents/nl2pipeline/chat_client.py`: Chat Completions API client.
- `agents/nl2pipeline/manager_client.py`: Manager API client.

## Token budget notes

Chat Completions is stateless, so the seed context is resent on every request as part of message
history. To keep token usage reasonable, the capabilities digest is intentionally minimized:

- functions: only `name/kind/aliases/arg_count/allowed_contexts/requirements`
- syntax: only `id/type/title/status/placement/constraints/syntax` (plus the minimal tree shape)

## MQTT defaults

For local development, v1 assumes a working broker:

- broker URL: `tcp://127.0.0.1:1883`
- default sink topic: `/nl2pipeline/out`

Source stream creation (`POST /streams`) requires:

- `source-topic`
- schema (at least column names and types)

## Non-goals (v1)

- Full type inference and operator-specific type rules.
- A first-class `validate_sql` REST endpoint (planned later).
- Multi-stream joins and complex pipeline graph inference.
