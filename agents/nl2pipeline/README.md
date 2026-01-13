# NL → Stream + Pipeline Agent (v1)

This folder contains a developer-facing prototype agent that turns natural-language requirements
into veloFlux Manager REST API requests (stream + pipeline).

- `POST /streams` (MQTT source stream)
- `POST /pipelines` (MQTT sink pipeline)

It follows the grounding rules in `docs/agents_readme.md`: do not invent stream/column/function
names; always ground on Manager introspection APIs and syntax/function catalogs.

## What v1 does

- Fetches runtime catalogs:
  - `GET /streams`, `GET /streams/describe/:name`
  - `GET /functions`
  - `GET /capabilities/syntax`
- Uses an LLM to produce veloFlux-valid SQL constrained by the catalogs.
- Runs a `draft → validate(planning) → explain → user confirm` loop in an interactive REPL.
  - Validation is implemented as a best-effort `POST /pipelines` with a temporary id.
  - Explain is shown via `GET /pipelines/:id/explain`.
  - The temporary pipeline is deleted after each round.
- Emits `CreateStreamRequest` + `CreatePipelineRequest` JSON for the user to apply (this tool does
  not apply by default).

## CLI

Two CLI entrypoints are available (to compare legacy vs LangGraph during migration):

- Legacy (handwritten state machine): `agents/nl2pipeline/nl2pipeline.py`
- LangGraph (in-memory, v1): `agents/nl2pipeline/langgraph_cli.py`

This tool is configured via a TOML file. Start with `agents/nl2pipeline/config.example.toml`
and copy it to `agents/nl2pipeline/config.toml`.

## Code Layout

- `agents/nl2pipeline/shared/`: shared building blocks (config, Manager/LLM clients, catalogs, history, prompts).
- `agents/nl2pipeline/legacy/`: legacy CLI engine (handwritten state machine).
- `agents/nl2pipeline/core/`: LangGraph engine (state + workflow + driver).
- `agents/nl2pipeline/cli/`: CLI entrypoints.
- `agents/nl2pipeline/docs/`: design docs.

### Run

```bash
cp agents/nl2pipeline/config.example.toml agents/nl2pipeline/config.toml
python3 agents/nl2pipeline/nl2pipeline.py --config agents/nl2pipeline/config.toml
```

### LangGraph CLI (in-memory, v1)

If you want to run the same core workflow via LangGraph (still a CLI; no server in this repo for now):

Install dependency:

```bash
python3 -m pip install -r agents/nl2pipeline/requirements.txt
```

Run:

```bash
python3 agents/nl2pipeline/langgraph_cli.py --config agents/nl2pipeline/config.toml
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

- Legacy CLI supports: `/help`, `/streams`, `/use <stream_name>`, `/show ...`, `/trace`, `/exit`.
- LangGraph CLI currently supports: `/help`, `/exit` (stream/schema selection is handled via interrupts/prompts).

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
