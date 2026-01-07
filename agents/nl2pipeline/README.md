# NL → Stream + Pipeline Agent (v1)

This folder contains a developer-facing prototype agent that turns natural-language requirements
into SynapseFlow Manager REST API requests (stream + pipeline).

- `POST /streams` (MQTT source stream)
- `POST /pipelines` (MQTT sink pipeline)

It follows the grounding rules in `docs/agents_readme.md`: do not invent stream/column/function
names; always ground on Manager introspection APIs and syntax/function catalogs.

## What v1 does

- Fetches runtime catalogs:
  - `GET /streams`, `GET /streams/describe/:name`
  - `GET /functions`
  - `GET /capabilities/syntax`
- Uses an LLM to produce SynapseFlow-valid SQL constrained by the catalogs.
- Runs a `draft → validate(planning) → explain → user confirm` loop in an interactive REPL.
  - Validation is implemented as a best-effort `POST /pipelines` with a temporary id.
  - Explain is shown via `GET /pipelines/:id/explain`.
  - The temporary pipeline is deleted after each round.
- Emits `CreateStreamRequest` + `CreatePipelineRequest` JSON for the user to apply (this tool does
  not apply by default).

## CLI

Entry point: `agents/nl2pipeline/nl2pipeline.py`

This tool is configured via a TOML file. Start with `agents/nl2pipeline/config.example.toml`
and copy it to `agents/nl2pipeline/config.toml`.

## Code Layout

- `agents/nl2pipeline/workflow.py`: the state machine and the draft→validate→explain loop (primary maintenance surface).
- `agents/nl2pipeline/repl.py`: interactive shell UI and command handling.
- `agents/nl2pipeline/chat_client.py`: minimal OpenAI-compatible Chat Completions client.
- `agents/nl2pipeline/manager_client.py`: minimal SynapseFlow Manager REST client.
- `agents/nl2pipeline/config.py`: TOML parsing and config dataclasses.
- `agents/nl2pipeline/catalogs.py`: capability digest building (compaction/filtering).

### Run

```bash
cp agents/nl2pipeline/config.example.toml agents/nl2pipeline/config.toml
python3 agents/nl2pipeline/nl2pipeline.py --config agents/nl2pipeline/config.toml
```

### LLM API

The agent uses an OpenAI-compatible Chat Completions API. The config must provide:

- `llm.base_url` (e.g. `https://api.openai.com`)
- `llm.api_key_env` (recommended; reads key from env)
- `llm.model` (default model)
- Optional per-stage overrides:
  - `llm.router_model` (intent routing; fast model recommended)
  - `llm.preview_model` (streaming SQL preview; fast model recommended)
  - `llm.draft_model` (structured SQL repair; reasoning model recommended)
- Optional: `llm.json_mode` (default `true`): requests `response_format={"type":"json_object"}`.
- Optional: `llm.stream` (default `false`): streams the SQL preview via SSE.

### REPL commands

- `/help`
- `/streams` (list available streams)
- `/use <stream_name>` (select active stream)
- `/exit`

## Notes

- This tool currently validates SQL by attempting to create a temporary pipeline. It relies on the
  planner's error strings for feedback. A dedicated `validate_sql` endpoint is planned later.
- Chat Completions is stateless, so the agent keeps a local message history. The seeded capabilities
  digest is intentionally minimized to keep token usage reasonable.

## Stream Selection

- On startup, the agent fetches existing streams via `GET /streams`.
- For each user message, the agent first asks the LLM to route intent:
  - `list_streams` → calls `GET /streams`
  - `describe_stream` → calls `GET /streams/describe/:name`
  - `nl2sql` → runs the draft→validate→explain loop (requires an active stream; otherwise asks you to choose)
